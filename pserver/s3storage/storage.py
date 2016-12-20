# -*- coding: utf-8 -*-

from zope.schema import Object
from pserver.s3storage.interfaces import IS3File
from pserver.s3storage.interfaces import IS3FileField
from plone.server.interfaces import IAbsoluteURL
from zope.interface import implementer
from zope.component import getUtility
from persistent import Persistent
from zope.schema.fieldproperty import FieldProperty
from zope.component import adapter
from plone.server.interfaces import IResource
from plone.server.interfaces import IRequest
from plone.server.interfaces import IFileManager
from pserver.s3storage.interfaces import IS3BlobStore
from pserver.s3storage.events import InitialS3Upload
from pserver.s3storage.events import FinishS3Upload
from plone.server.json.interfaces import IValueToJson
from plone.server.transactions import get_current_request
from aiohttp.web import StreamResponse
from plone.server.browser import Response
from plone.server.events import notify
from datetime import datetime
from dateutil.tz import tzlocal
from datetime import timedelta
import logging
import uuid
import aiohttp
import asyncio
import json
import boto3
import base64
from io import BytesIO


log = logging.getLogger(__name__)

MAX_SIZE = 1073741824

SCOPES = ['https://www.googleapis.com/auth/devstorage.read_write']
UPLOAD_URL = 'https://www.googleapis.com/upload/storage/v1/b/{bucket}/o?uploadType=resumable'  # noqa
CHUNK_SIZE = 524288
MAX_RETRIES = 5


class S3Exception(Exception):
    pass


@adapter(IS3File)
@implementer(IValueToJson)
def json_converter(value):
    if value is None:
        return value

    return {
        'filename': value.filename,
        'contenttype': value.contentType,
        'size': value.size,
        'extension': value.extension,
        'md5': value.md5
    }


@adapter(IResource, IRequest, IS3FileField)
@implementer(IFileManager)
class S3FileManager(object):

    def __init__(self, context, request, field):
        self.context = context
        self.request = request
        self.field = field

    async def upload(self):
        """In order to support TUS and IO upload.

        we need to provide an upload that concats the incoming
        """
        file = self.field.get(self.context)
        if file is None:
            file = S3File(contentType=self.request.content_type)
            self.field.set(self.context, file)
        if 'X-UPLOAD-MD5HASH' in self.request.headers:
            file._md5hash = self.request.headers['X-UPLOAD-MD5HASH']
        else:
            file._md5hash = None

        if 'X-UPLOAD-SIZE' in self.request.headers:
            file._size = int(self.request.headers['X-UPLOAD-SIZE'])
        else:
            raise AttributeError('x-upload-size header needed')

        if 'X-UPLOAD-FILENAME' in self.request.headers:
            file.filename = self.request.headers['X-UPLOAD-FILENAME']
        else:
            file.filename = uuid.uuid4().hex

        await file.initUpload(self.context)
        try:
            data = await self.request.content.readexactly(CHUNK_SIZE)
        except asyncio.IncompleteReadError as e:
            data = e.partial

        count = 0
        while data:
            old_current_upload = file._current_upload
            resp = await file.appendData(data)
            readed_bytes = file._current_upload - old_current_upload

            data = data[readed_bytes:]

            bytes_to_read = readed_bytes

            if resp.status in [200, 201]:
                break
            if resp.status == 308:
                count = 0
                try:
                    data += await self.request.content.readexactly(bytes_to_read)  # noqa
                except asyncio.IncompleteReadError as e:
                    data += e.partial

            else:
                count += 1
                if count > MAX_RETRIES:
                    raise AttributeError('MAX retries error')
        # Test resp and checksum to finish upload
        await file.finishUpload(self.context)

    async def tus_create(self):
        file = self.field.get(self.context)
        if file is None:
            file = S3File(contentType=self.request.content_type)
            self.field.set(self.context, file)
        if 'CONTENT-LENGTH' in self.request.headers:
            file._current_upload = int(self.request.headers['CONTENT-LENGTH'])
        else:
            file._current_upload = 0

        if 'UPLOAD-LENGTH' in self.request.headers:
            file._size = int(self.request.headers['UPLOAD-LENGTH'])
        else:
            raise AttributeError('We need upload-length header')

        if 'UPLOAD-MD5' in self.request.headers:
            file._md5hash = self.request.headers['UPLOAD-MD5']

        if 'TUS-RESUMABLE' not in self.request.headers:
            raise AttributeError('Its a TUS needs a TUS version')

        if 'UPLOAD-METADATA' not in self.request.headers:
            file.filename = uuid.uuid4().hex
        else:
            filename = self.request.headers['UPLOAD-METADATA']
            file.filename = str(base64.b64decode(filename.split()[1]))

        await file.initUpload(self.context)
        # Location will need to be adapted on aiohttp 1.1.x
        resp = Response(headers=aiohttp.MultiDict({
            'Location': IAbsoluteURL(self.context, self.request)() + '/@tusupload/' + self.field.__name__,  # noqa
            'Tus-Resumable': '1.0.0'
        }), status=201)
        return resp

    async def tus_patch(self):
        file = self.field.get(self.context)
        if 'CONTENT-LENGTH' in self.request.headers:
            to_upload = int(self.request.headers['CONTENT-LENGTH'])
        else:
            raise AttributeError('No content-length header')

        if 'UPLOAD-OFFSET' in self.request.headers:
            file._current_upload = int(self.request.headers['UPLOAD-OFFSET'])
        else:
            raise AttributeError('No upload-offset header')

        try:
            data = await self.request.content.readexactly(to_upload)
        except asyncio.IncompleteReadError as e:
            data = e.partial
        count = 0
        while data:
            old_current_upload = file._current_upload
            resp = await file.appendData(data)

            # The amount of bytes that are readed
            readed_bytes = file._current_upload - old_current_upload + 1

            # Cut the data so there is only the needed data
            data = data[readed_bytes:]

            bytes_to_read = readed_bytes

            if bytes_to_read == 0:
                break
            if len(data) < 262144:
                break
            if resp.status in [200, 201]:
                file.finishUpload(self.context)
            if resp.status in [400]:
                break
            if resp.status == 308:
                count = 0
                try:
                    data += await self.request.content.readexactly(bytes_to_read)  # noqa
                except asyncio.IncompleteReadError as e:
                    data += e.partial

            else:
                count += 1
                if count > MAX_RETRIES:
                    raise AttributeError('MAX retries error')
        expiration = file._resumable_uri_date + timedelta(days=7)

        resp = Response(headers=aiohttp.MultiDict({
            'Upload-Offset': str(file.actualSize() + 1),
            'Tus-Resumable': '1.0.0',
            'Upload-Expires': expiration.isoformat()
        }))
        return resp

    async def tus_head(self):
        file = self.field.get(self.context)
        if file is None:
            raise KeyError('No file on this context')
        resp = Response(headers=aiohttp.MultiDict({
            'Upload-Offset': str(file.actualSize()),
            'Tus-Resumable': '1.0.0'
        }))
        return resp

    async def tus_options(self):
        resp = Response(headers=aiohttp.MultiDict({
            'Tus-Resumable': '1.0.0',
            'Tus-Version': '1.0.0',
            'Tus-Max-Size': '1073741824',
            'Tus-Extension': 'creation,expiration'
        }))
        return resp

    async def download(self):
        file = self.field.get(self.context)
        if file is None:
            raise AttributeError('No field value')

        resp = StreamResponse(headers=aiohttp.MultiDict({
            'CONTENT-DISPOSITION': 'attachment; filename="%s"' % file.filename
        }))
        resp.content_type = file.contentType
        resp.content_length = file._size
        buf = BytesIO()
        downloader = await file.download(buf)
        await resp.prepare(self.request)
        # response.start(request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print("Download {}%.".format(int(status.progress() * 100)))
            buf.seek(0)
            data = buf.read()
            resp.write(data)
            await resp.drain()
            buf.seek(0)
            buf.truncate()

        return resp


@implementer(IS3File)
class S3File(Persistent):
    """File stored in a GCloud, with a filename."""

    filename = FieldProperty(IS3File['filename'])

    def __init__(  # noqa
            self,
            contentType='application/octet-stream',
            filename=None):
        self.contentType = contentType
        self._current_upload = 0
        if filename is not None:
            self.filename = filename
            extension_discovery = filename.split('.')
            if len(extension_discovery) > 1:
                self._extension = extension_discovery[-1]
        elif self.filename is not None:
            self.filename = uuid.uuid4().hex

    async def initUpload(self, context):
        """Init an upload.

        self._uload_file_id : temporal url to image beeing uploaded
        self._resumable_uri : uri to resumable upload
        self._uri : finished uploaded image
        """
        util = getUtility(IS3BlobStore)
        request = get_current_request()
        if hasattr(self, '_upload_file_id') and self._upload_file_id is not None:  # noqa
            req = util._service.objects().delete(
                bucket=util.bucket, object=self._upload_file_id)
            try:
                req.execute()
            except errors.HttpError:
                pass

        self._upload_file_id = request._site_id + '/' + uuid.uuid4().hex
        self._mpu = util.bucket.initiate_multipart_upload(
            self._upload_file_id)
        self._current_upload = 0
        self._block = 0
        self._resumable_uri_date = datetime.now(tz=tzlocal())
        await notify(InitialS3Upload(context))

    async def appendData(self, data):

        self._mpu.upload_part_from_file(data, self._block, cb=progress)
        self._block += 1
        return call

    def actualSize(self):
        return self._current_upload

    async def finishUpload(self, context):
        util = getUtility(IS3BlobStore)
        # It would be great to do on AfterCommit
        # Delete the old file and update the new uri
        if hasattr(self, '_uri') and self._uri is not None:
            req = util._service.objects().delete(
                bucket=util.bucket, object=self._uri)
            try:
                resp = req.execute()  # noqa
            except errors.HttpError:
                pass
        self._uri = self._upload_file_id
        self._upload_file_id = None
        await notify(FinishS3Upload(context))

    async def deleteUpload(self):
        if hasattr(self, '_uri') and self._uri is not None:
            req = util._service.objects().delete(
                bucket=util.bucket, object=self._uri)
            resp = req.execute()
            return resp
        else:
            raise AttributeError('No valid uri')

    async def download(self, buf):
        util = getUtility(IS3BlobStore)
        if not hasattr(self, '_uri'):
            url = self._upload_file_id
        else:
            url = self._uri
        req = util._service.objects().get_media(
            bucket=util.bucket, object=url)
        downloader = http.MediaIoBaseDownload(buf, req, chunksize=CHUNK_SIZE)
        return downloader

    def _set_data(self, data):
        raise NotImplemented('Only specific upload permitted')

    def _get_data(self):
        raise NotImplemented('Only specific download permitted')

    data = property(_get_data, _set_data)

    @property
    def size(self):
        if hasattr(self, '_size'):
            return self._size
        else:
            return None

    @property
    def md5(self):
        if hasattr(self, '_md5hash'):
            return self._md5hash
        else:
            return None

    @property
    def extension(self):
        if hasattr(self, '_extension'):
            return self._extension
        else:
            return None

    def getSize(self):  # noqa
        return self.size


@implementer(IS3FileField)
class S3FileField(Object):
    """A NamedBlobFile field."""

    _type = S3File
    schema = IS3File

    def __init__(self, **kw):
        if 'schema' in kw:
            self.schema = kw.pop('schema')
        super(S3FileField, self).__init__(schema=self.schema, **kw)


# Configuration Utility

class S3BlobStore(object):

    def __init__(self, settings):
        # self._aws_access_key = settings['AWS_ACCESS_KEY_ID']
        # self._aws_secret_key = settings['AWS_SECRET_ACCESS_KEY']
        # self._s3 = boto3.client(
        #     's3',
        #     aws_access_key_id=self._aws_access_key,
        #     aws_secret_access_key=self._aws_secret_key
        # )
        self._bucket_name = settings['bucket']
        self._s3 = boto3.resource('s3')

    @property
    def bucket(self):
        request = get_current_request()
        if '.' in self._bucket:
            char_delimiter = '.'
        else:
            char_delimiter = '_'
        bucket_name = request._site_id.lower() + char_delimiter + self._bucket
        bucket = self._s3.lookup(bucket_name)
        return bucket

    async def initialize(self, app=None):
        # No asyncio loop to run
        self.app = app
