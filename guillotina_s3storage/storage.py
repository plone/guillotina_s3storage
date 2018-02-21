# -*- coding: utf-8 -*-
from aiohttp.web import StreamResponse
from aiohttp.web_exceptions import HTTPNotFound
from datetime import datetime
from datetime import timedelta
from dateutil.tz import tzlocal
from guillotina import app_settings
from guillotina import configure
from guillotina.browser import Response
from guillotina.component import getUtility
from guillotina.event import notify
from guillotina.files import BaseCloudFile
from guillotina.files import read_request_data
from guillotina.interfaces import IAbsoluteURL
from guillotina.interfaces import IApplication
from guillotina.interfaces import IFileCleanup
from guillotina.interfaces import IFileManager
from guillotina.interfaces import IRequest
from guillotina.interfaces import IResource
from guillotina.schema import Object
from guillotina.utils import get_current_request
from guillotina_s3storage.events import FinishS3Upload
from guillotina_s3storage.events import InitialS3Upload
from guillotina_s3storage.interfaces import IS3BlobStore
from guillotina_s3storage.interfaces import IS3File
from guillotina_s3storage.interfaces import IS3FileField
from guillotina_s3storage.utils import aretriable
from io import BytesIO
from zope.interface import implementer

import aiobotocore
import asyncio
import base64
import boto3
import botocore
import logging
import uuid


log = logging.getLogger('guillotina_s3storage')

MAX_SIZE = 1073741824

MIN_UPLOAD_SIZE = 5 * 1024 * 1024
CHUNK_SIZE = MIN_UPLOAD_SIZE
MAX_RETRIES = 5


class S3Exception(Exception):
    pass


@configure.adapter(
    for_=(IResource, IRequest, IS3FileField),
    provides=IFileManager)
class S3FileManager(object):

    def __init__(self, context, request, field):
        self.context = context
        self.request = request
        self.field = field

    async def upload(self):
        """In order to support TUS and IO upload.

        we need to provide an upload that concats the incoming
        """
        self.context._p_register()  # writing to object

        file = self.field.get(self.field.context or self.context)
        if not isinstance(file, S3File):
            file = S3File(content_type=self.request.content_type)
            self.field.set(self.field.context or self.context, file)

        if 'X-UPLOAD-MD5HASH' in self.request.headers:
            file._md5 = self.request.headers['X-UPLOAD-MD5HASH']
        else:
            file._md5 = None

        if 'X-UPLOAD-EXTENSION' in self.request.headers:
            file._extension = self.request.headers['X-UPLOAD-EXTENSION']
        else:
            file._extension = None

        if 'X-UPLOAD-SIZE' in self.request.headers:
            file._size = int(self.request.headers['X-UPLOAD-SIZE'])
        else:
            raise AttributeError('x-upload-size header needed')

        if 'X-UPLOAD-FILENAME' in self.request.headers:
            file.filename = self.request.headers['X-UPLOAD-FILENAME']
        elif 'X-UPLOAD-FILENAME-B64' in self.request.headers:
            file.filename = base64.b64decode(
                self.request.headers['X-UPLOAD-FILENAME-B64']).decode("utf-8")
        else:
            file.filename = uuid.uuid4().hex

        if file.size < MIN_UPLOAD_SIZE:
            file._one_tus_shoot = True
        else:
            file._one_tus_shoot = False

        await file.init_upload(self.context)
        self.request._last_read_pos = 0
        data = await read_request_data(self.request, CHUNK_SIZE)

        if file.size < MIN_UPLOAD_SIZE:
            await file.one_shot_upload(self.context, data)
        else:
            count = 0

            # If we have data or is an empty file
            while data or (len(data) == 0 and count == 0):
                old_current_upload = file._current_upload  # noqa
                await file.append_data(data)
                count += 1
                data = await read_request_data(self.request, CHUNK_SIZE)

        # Test resp and checksum to finish upload
        await file.finish_upload(self.context, clean=self.should_clean(file))

    async def tus_create(self):
        self.context._p_register()  # writing to object

        # This only happens in tus-java-client, redirect this POST to a PATCH
        if self.request.headers.get('X-HTTP-Method-Override') == 'PATCH':
            return await self.tus_patch()

        file = self.field.get(self.field.context or self.context)
        if not isinstance(file, S3File):
            file = S3File(content_type=self.request.content_type)
            self.field.set(self.field.context or self.context, file)
        if 'CONTENT-LENGTH' in self.request.headers:
            file._current_upload = int(self.request.headers['CONTENT-LENGTH'])
        else:
            file._current_upload = 0

        if 'UPLOAD-LENGTH' in self.request.headers:
            file._size = int(self.request.headers['UPLOAD-LENGTH'])
        else:
            raise AttributeError('We need upload-length header')

        if 'UPLOAD-MD5' in self.request.headers:
            file._md5 = self.request.headers['UPLOAD-MD5']

        if 'UPLOAD-EXTENSION' in self.request.headers:
            file._extension = self.request.headers['UPLOAD-EXTENSION']

        if 'TUS-RESUMABLE' not in self.request.headers:
            raise AttributeError('Its a TUS needs a TUS version')

        if 'UPLOAD-FILENAME' in self.request.headers:
            file.filename = self.request.headers['UPLOAD-FILENAME']
        elif 'UPLOAD-METADATA' not in self.request.headers:
            file.filename = uuid.uuid4().hex
        else:
            filename = self.request.headers['UPLOAD-METADATA']
            file.filename = base64.b64decode(filename.split()[1]).decode('utf-8')

        if file.size < MIN_UPLOAD_SIZE:
            file._one_tus_shoot = True
        else:
            file._one_tus_shoot = False

        await file.init_upload(self.context)

        # Location will need to be adapted on aiohttp 1.1.x
        resp = Response(headers={
            'Location': IAbsoluteURL(
                self.context, self.request)() + '/@tusupload/' + self.field.__name__,
            'Tus-Resumable': '1.0.0',
            'Access-Control-Expose-Headers': 'Location,Tus-Resumable'
        }, status=201)
        return resp

    async def tus_patch(self):
        try:
            self.field.context.data._p_register()  # register change...
        except AttributeError:
            self.context._p_register()

        file = self.field.get(self.field.context or self.context)
        if 'CONTENT-LENGTH' in self.request.headers:
            to_upload = int(self.request.headers['CONTENT-LENGTH'])
        else:
            raise AttributeError('No content-length header')

        if 'UPLOAD-OFFSET' in self.request.headers:
            file._current_upload = int(self.request.headers['UPLOAD-OFFSET'])
        else:
            raise AttributeError('No upload-offset header')

        self.request._last_read_pos = 0
        data = await read_request_data(self.request, to_upload)

        if file.one_tus_shoot:
            # One time shoot
            if file._block > 1:
                raise AttributeError('You should push 5Mb blocks AWS')
            await file.one_shot_upload(self.context, data)
            expiration = datetime.now() + timedelta(days=365)
        else:
            count = 0
            while data:
                resp = await file.append_data(data)
                count += 1

                data = await read_request_data(self.request, CHUNK_SIZE)

            expiration = file._resumable_uri_date + timedelta(days=7)
        if file._size <= file._current_upload:
            await file.finish_upload(self.context, clean=self.should_clean(file))
        resp = Response(headers={
            'Upload-Offset': str(file.get_actual_size()),
            'Tus-Resumable': '1.0.0',
            'Upload-Expires': expiration.isoformat(),
            'Access-Control-Expose-Headers': 'Upload-Offset,Upload-Expires,Tus-Resumable'
        })
        return resp

    async def tus_head(self):
        file = self.field.get(self.field.context or self.context)
        if not isinstance(file, S3File):
            raise KeyError('No file on this context')
        head_response = {
            'Upload-Offset': str(file.get_actual_size()),
            'Tus-Resumable': '1.0.0',
            'Access-Control-Expose-Headers': 'Upload-Offset,Upload-Length,Tus-Resumable'
        }
        if file.size:
            head_response['Upload-Length'] = str(file._size)
        resp = Response(headers=head_response)
        return resp

    async def tus_options(self):
        resp = Response(headers={
            'Tus-Resumable': '1.0.0',
            'Tus-Version': '1.0.0',
            'Tus-Max-Size': '1073741824',
            'Tus-Extension': 'creation,expiration'
        })
        return resp

    async def download(self, disposition=None):
        if disposition is None:
            disposition = self.request.GET.get('disposition', 'attachment')
        file = self.field.get(self.field.context or self.context)
        if not isinstance(file, S3File) or file.uri is None:
            return HTTPNotFound(text='No file found')

        cors_renderer = app_settings['cors_renderer'](self.request)
        headers = await cors_renderer.get_headers()
        headers.update({
            'CONTENT-DISPOSITION': f'{disposition}; filename="%s"' % file.filename
        })

        resp = StreamResponse(headers=headers)
        resp.content_type = file.guess_content_type()
        resp.content_length = file._size

        try:
            downloader = await file.download(None)
        except botocore.exceptions.ClientError:
            log.error(f'Referenced key {file.uri} could not be found', exc_info=True)
            return HTTPNotFound(text=f'Could not find {file.uri} in s3 storage')
        await resp.prepare(self.request)

        async with downloader['Body'] as stream:
            data = await stream.read(CHUNK_SIZE)

            while data:
                resp.write(data)
                data = await stream.read(CHUNK_SIZE)

        return resp

    async def iter_data(self):
        file = self.field.get(self.field.context or self.context)
        if not isinstance(file, S3File) or file.uri is None:
            raise AttributeError('No field value')

        downloader = await file.download(None)

        async with downloader['Body'] as stream:
            data = await stream.read(CHUNK_SIZE)
            while True:
                if not data:
                    break
                yield data
                data = await stream.read(CHUNK_SIZE)

    async def save_file(self, generator, content_type=None, size=None,
                        filename=None):
        self.context._p_register()  # writing to object

        file = self.field.get(self.field.context or self.context)
        if not isinstance(file, S3File):
            file = S3File(content_type=content_type)
            self.field.set(self.field.context or self.context, file)

        file._size = size
        if filename is None:
            filename = uuid.uuid4().hex
        file.filename = filename

        if file.size < MIN_UPLOAD_SIZE:
            file._one_tus_shoot = True  # need to set this...
        else:
            file._one_tus_shoot = False

        await file.init_upload(self.context)

        if file.size < MIN_UPLOAD_SIZE:
            data = b''
            async for chunk in generator():
                data += chunk
            await file.one_shot_upload(self.context, data)
        else:
            async for data in generator():
                await file.append_data(data)

        await file.finish_upload(self.context, clean=self.should_clean(file))
        return file

    def should_clean(self, file):
        cleanup = IFileCleanup(self.context, None)
        return cleanup is None or cleanup.should_clean(file=file, field=self.field)


@implementer(IS3File)
class S3File(BaseCloudFile):
    """File stored in a GCloud, with a filename."""

    async def copy_cloud_file(self, new_uri):
        if self.uri is None:
            Exception('To rename a uri must be set on the object')
        util = getUtility(IS3BlobStore)

        await util._s3aioclient.copy_object(
            CopySource={'Bucket': self._bucket_name, 'Key': self.uri},
            Bucket=self._bucket_name, Key=new_uri)
        old_uri = self.uri
        self._uri = new_uri
        return old_uri

    async def rename_cloud_file(self, new_uri):
        old_uri = await self.copy_cloud_file(new_uri)
        util = getUtility(IS3BlobStore)
        await util._s3aioclient.delete_object(
            Bucket=self._bucket_name, Key=old_uri)

    async def init_upload(self, context):
        """Init an upload.

        self._uload_file_id : temporal url to image beeing uploaded
        self._resumable_uri : uri to resumable upload
        self._uri : finished uploaded image
        """
        util = getUtility(IS3BlobStore)
        request = get_current_request()
        if hasattr(self, '_upload_file_id') and self._upload_file_id is not None:  # noqa
            if getattr(self, '_mpu', None) is not None:
                await self._abort_multipart()
            self._mpu = None
            self._upload_file_id = None

        bucket_name = await util.get_bucket_name()
        self._bucket_name = bucket_name
        self._upload_file_id = self.generate_key(request, context)
        self._multipart = {'Parts': []}
        if not self.one_tus_shoot:
            await self._create_multipart()
        self._current_upload = 0
        self._block = 1
        self._resumable_uri_date = datetime.now(tz=tzlocal())
        await notify(InitialS3Upload(context))

    @aretriable(3)
    async def _create_multipart(self):
        util = getUtility(IS3BlobStore)
        self._mpu = await util._s3aioclient.create_multipart_upload(
            Bucket=self._bucket_name, Key=self._upload_file_id)

    @aretriable(3)
    async def _abort_multipart(self):
        util = getUtility(IS3BlobStore)
        try:
            await util._s3aioclient.abort_multipart_upload(
                Bucket=self._bucket_name,
                Key=self._upload_file_id,
                UploadId=self._mpu['UploadId'])
        except Exception:
            log.warn('Could not abort multipart upload', exc_info=True)

    async def append_data(self, data):
        part = await self._upload_part(data)
        self._multipart['Parts'].append({
            'PartNumber': self._block,
            'ETag': part['ETag']
        })
        self._current_upload += len(data)
        self._block += 1
        return part

    @aretriable(3)
    async def _upload_part(self, data):
        util = getUtility(IS3BlobStore)
        return await util._s3aioclient.upload_part(
            Bucket=self._bucket_name,
            Key=self._upload_file_id,
            PartNumber=self._block,
            UploadId=self._mpu['UploadId'],
            Body=data)

    def get_actual_size(self):
        return self._current_upload

    async def finish_upload(self, context, clean=True):
        util = getUtility(IS3BlobStore)
        # It would be great to do on AfterCommit
        if self.uri is not None:
            self._old_uri = self.uri
            if clean:
                try:
                    await util._s3aioclient.delete_object(
                        Bucket=self._bucket_name, Key=self.uri)
                except botocore.exceptions.ClientError as e:
                    log.error(
                        f'Referenced key {self.uri} could not be found',
                        exc_info=True)
        self._uri = self._upload_file_id
        if self._mpu is not None:
            await self._complete_multipart_upload()
        self._multipart = None
        self._mpu = None
        self._block = None
        self._upload_file_id = None
        await notify(FinishS3Upload(context))

    @aretriable(3)
    async def _complete_multipart_upload(self):
        util = getUtility(IS3BlobStore)
        await util._s3aioclient.complete_multipart_upload(
            Bucket=self._bucket_name,
            Key=self._upload_file_id,
            UploadId=self._mpu['UploadId'],
            MultipartUpload=self._multipart)

    async def one_shot_upload(self, context, data):
        util = getUtility(IS3BlobStore)

        if hasattr(self, '_upload_file_id') and self._upload_file_id is not None:  # noqa
            if getattr(self, '_mpu', None) is not None:
                await self._abort_multipart()
            self._mpu = None
            self._upload_file_id = None

        bucket_name = await util.get_bucket_name()
        self._bucket_name = bucket_name
        request = get_current_request()
        self._upload_file_id = self.generate_key(request, context)

        response = await self._upload_fileobj(data)

        self._block += 1
        self._current_upload += len(data)
        return response

    @aretriable(3)
    async def _upload_fileobj(self, data):
        file_data = BytesIO(data)
        util = getUtility(IS3BlobStore)
        # XXX no support for upload_fileobj in aiobotocore so run in executor
        root = getUtility(IApplication, name='root')
        response = await util._loop.run_in_executor(
            root.executor, util._s3client.upload_fileobj,
            file_data,
            self._bucket_name,
            self._upload_file_id)
        return response

    async def delete_upload(self, uri=None):
        util = getUtility(IS3BlobStore)
        if uri is None:
            uri = self.uri
        if uri is not None:
            try:
                await util._s3aioclient.delete_object(
                    Bucket=self._bucket_name, Key=uri)
            except botocore.exceptions.ClientError as e:
                log.warn('Error deleting object', exc_info=True)
        else:
            raise AttributeError('No valid uri')

    async def download(self, buf):
        if not hasattr(self, '_uri'):
            url = self._upload_file_id
        else:
            url = self._uri

        return await self._download(url)

    @aretriable(3)
    async def _download(self, url):
        util = getUtility(IS3BlobStore)
        return await util._s3aioclient.get_object(Bucket=self._bucket_name, Key=url)

    @property
    def one_tus_shoot(self):
        if hasattr(self, '_one_tus_shoot'):
            return self._one_tus_shoot
        else:
            return False


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

    def __init__(self, settings, loop=None):
        self._aws_access_key = settings['aws_client_id']
        self._aws_secret_key = settings['aws_client_secret']

        opts = dict(
            aws_secret_access_key=self._aws_secret_key,
            aws_access_key_id=self._aws_access_key,
            endpoint_url=settings.get('endpoint_url'),
            verify=settings.get('verify_ssl'),
            use_ssl=settings.get('ssl', True),
            region_name=settings.get('region_name')
        )

        if loop is None:
            loop = asyncio.get_event_loop()
        self._loop = loop

        self._s3aiosession = aiobotocore.get_session(loop=loop)

        # This client is for downloads only
        self._s3aioclient = self._s3aiosession.create_client('s3', **opts)
        self._cached_buckets = []

        self._bucket_name = settings['bucket']

        # right now, only used for upload_fileobj in executor
        self._s3client = boto3.client('s3', **opts)

    async def get_bucket_name(self):
        request = get_current_request()
        bucket_name = request._container_id.lower() + '.' + self._bucket_name

        if bucket_name in self._cached_buckets:
            return bucket_name

        missing = False
        try:
            res = await self._s3aioclient.head_bucket(Bucket=bucket_name)
            if res['ResponseMetadata']['HTTPStatusCode'] == 404:
                missing = True
        except botocore.exceptions.ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                missing = True

        if missing:
            await self._s3aioclient.create_bucket(Bucket=bucket_name)
        return bucket_name

    async def initialize(self, app=None):
        # No asyncio loop to run
        self.app = app

    async def finalize(self, app=None):
        self._s3aiosession.close()

    async def iterate_bucket(self):
        req = get_current_request()
        bucket_name = await self.get_bucket_name()
        result = await self._s3aioclient.list_objects(
            Bucket=bucket_name, Prefix=req._container_id + '/')
        paginator = self._s3aioclient.get_paginator('list_objects')
        async for result in paginator.paginate(
                Bucket=bucket_name, Prefix=req._container_id + '/'):
            for item in result.get('Contents', []):
                yield item
