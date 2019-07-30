# -*- coding: utf-8 -*-
from guillotina import configure, task_vars
from guillotina.component import get_utility
from guillotina.exceptions import FileNotFoundException
from guillotina.files import BaseCloudFile
from guillotina.files.utils import generate_key
from guillotina.interfaces import IFileCleanup
from guillotina.interfaces import IExternalFileStorageManager
from guillotina.interfaces import IRequest
from guillotina.interfaces import IResource
from guillotina.response import HTTPNotFound
from guillotina.schema import Object
from guillotina_s3storage.interfaces import IS3BlobStore
from guillotina_s3storage.interfaces import IS3File
from guillotina_s3storage.interfaces import IS3FileField
from zope.interface import implementer

import aiobotocore
import aiohttp
import asyncio
import backoff
import botocore
import logging


log = logging.getLogger('guillotina_s3storage')

MAX_SIZE = 1073741824

MIN_UPLOAD_SIZE = 5 * 1024 * 1024
CHUNK_SIZE = MIN_UPLOAD_SIZE
MAX_RETRIES = 5

RETRIABLE_EXCEPTIONS = (
    botocore.exceptions.ClientError,
    aiohttp.client_exceptions.ClientPayloadError,
    botocore.exceptions.BotoCoreError
)


class IS3FileStorageManager(IExternalFileStorageManager):
    pass


class S3Exception(Exception):
    pass


@implementer(IS3File)
class S3File(BaseCloudFile):
    """File stored in a GCloud, with a filename."""


def _is_uploaded_file(file):
    return (file is not None and
            isinstance(file, S3File) and
            file.uri is not None)


@implementer(IS3FileField)
class S3FileField(Object):
    """A NamedBlobFile field."""

    _type = S3File
    schema = IS3File

    def __init__(self, **kw):
        if 'schema' in kw:
            self.schema = kw.pop('schema')
        super(S3FileField, self).__init__(schema=self.schema, **kw)


@configure.adapter(
    for_=(IResource, IRequest, IS3FileField),
    provides=IS3FileStorageManager)
class S3FileStorageManager:

    file_class = S3File

    def __init__(self, context, request, field):
        self.context = context
        self.request = request
        self.field = field

    def should_clean(self, file):
        cleanup = IFileCleanup(self.context, None)
        return cleanup is None or cleanup.should_clean(file=file, field=self.field)

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, max_tries=3)
    async def _download(self, uri, bucket):
        util = get_utility(IS3BlobStore)
        if bucket is None:
            bucket = await util.get_bucket_name()
        return await util._s3aioclient.get_object(Bucket=bucket, Key=uri)

    async def iter_data(self, uri=None):
        bucket = None
        if uri is None:
            file = self.field.query(self.field.context or self.context, None)
            if not _is_uploaded_file(file):
                raise FileNotFoundException('File not found')
            else:
                uri = file.uri
                bucket = file._bucket_name
        downloader = await self._download(uri, bucket)

        # we do not want to timeout ever from this...
        # downloader['Body'].set_socket_timeout(999999)
        async with downloader['Body'] as stream:
            data = await stream.read(CHUNK_SIZE)
            while True:
                if not data:
                    break
                yield data
                data = await stream.read(CHUNK_SIZE)

    async def delete_upload(self, uri, bucket=None):
        util = get_utility(IS3BlobStore)
        if bucket is None:
            bucket = await util.get_bucket_name()
        if uri is not None:
            try:
                await util._s3aioclient.delete_object(Bucket=bucket, Key=uri)
            except botocore.exceptions.ClientError:
                log.warn('Error deleting object', exc_info=True)
        else:
            raise AttributeError('No valid uri')

    async def _abort_multipart(self, dm):
        util = get_utility(IS3BlobStore)
        try:
            mpu = dm.get('_mpu')
            upload_file_id = dm.get('_upload_file_id')
            bucket_name = dm.get('_bucket_name')
            await util._s3aioclient.abort_multipart_upload(
                Bucket=bucket_name,
                Key=upload_file_id,
                UploadId=mpu['UploadId'])
        except Exception:
            log.warn('Could not abort multipart upload', exc_info=True)

    async def start(self, dm):
        util = get_utility(IS3BlobStore)
        upload_file_id = dm.get('_upload_file_id')
        if upload_file_id is not None:
            if dm.get('_mpu') is not None:
                await self._abort_multipart(dm)

        bucket_name = await util.get_bucket_name()
        upload_id = generate_key(self.context)
        await dm.update(
            _bucket_name=bucket_name,
            _upload_file_id=upload_id,
            _multipart={'Parts': []},
            _block=1,
            _mpu=await self._create_multipart(bucket_name, upload_id)
        )

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, max_tries=3)
    async def _create_multipart(self, bucket_name, upload_id):
        util = get_utility(IS3BlobStore)
        return await util._s3aioclient.create_multipart_upload(
            Bucket=bucket_name,
            Key=upload_id
        )

    async def append(self, dm, iterable, offset) -> int:
        size = 0
        async for chunk in iterable:
            size += len(chunk)
            part = await self._upload_part(dm, chunk)
            multipart = dm.get('_multipart')
            multipart['Parts'].append({
                'PartNumber': dm.get('_block'),
                'ETag': part['ETag']
            })
            await dm.update(
                _multipart=multipart,
                _block=dm.get('_block') + 1
            )
        return size

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, max_tries=3)
    async def _upload_part(self, dm, data):
        util = get_utility(IS3BlobStore)
        return await util._s3aioclient.upload_part(
            Bucket=dm.get('_bucket_name'),
            Key=dm.get('_upload_file_id'),
            PartNumber=dm.get('_block'),
            UploadId=dm.get('_mpu')['UploadId'],
            Body=data)

    async def finish(self, dm):
        file = self.field.query(self.field.context or self.context, None)
        if _is_uploaded_file(file):
            # delete existing file
            if self.should_clean(file):
                try:
                    await self.delete_upload(file.uri, file._bucket_name)
                except botocore.exceptions.ClientError:
                    log.error(f'Referenced key {file.uri} could not be found', exc_info=True)
                    log.warn('Error deleting object', exc_info=True)

        if dm.get('_mpu') is not None:
            await self._complete_multipart_upload(dm)
        await dm.update(
            uri=dm.get('_upload_file_id'),
            _multipart=None,
            _mpu=None,
            _block=None,
            _upload_file_id=None
        )

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, max_tries=3)
    async def _complete_multipart_upload(self, dm):
        util = get_utility(IS3BlobStore)
        # if blocks is 0, it means the file is of zero length so we need to
        # trick it to finish a multiple part with no data.
        if dm.get('_block') == 1:
            part = await self._upload_part(dm, b'')
            multipart = dm.get('_multipart')
            multipart['Parts'].append({
                'PartNumber': dm.get('_block'),
                'ETag': part['ETag']
            })
            await dm.update(
                _multipart=multipart,
                _block=dm.get('_block') + 1
            )
        await util._s3aioclient.complete_multipart_upload(
            Bucket=dm.get('_bucket_name'),
            Key=dm.get('_upload_file_id'),
            UploadId=dm.get('_mpu')['UploadId'],
            MultipartUpload=dm.get('_multipart'))

    async def exists(self):
        bucket = None
        file = self.field.query(self.field.context or self.context, None)
        if not _is_uploaded_file(file):
            return False
        else:
            uri = file.uri
            bucket = file._bucket_name
        util = get_utility(IS3BlobStore)
        try:
            return await util._s3aioclient.get_object(
                Bucket=bucket, Key=uri) is not None
        except botocore.exceptions.ClientError as ex:
            if ex.response['Error']['Code'] == 'NoSuchKey':
                return False
            raise

    async def copy(self, to_storage_manager, to_dm):
        file = self.field.query(self.field.context or self.context, None)
        if not _is_uploaded_file(file):
            raise HTTPNotFound(content={
                "reason": 'To copy a uri must be set on the object'
            })

        util = get_utility(IS3BlobStore)

        new_uri = generate_key(self.context)
        await util._s3aioclient.copy_object(
            CopySource={
                'Bucket': file._bucket_name,
                'Key': file.uri
            },
            Bucket=file._bucket_name, Key=new_uri)
        await to_dm.finish(
            values={
                'content_type': file.content_type,
                'size': file.size,
                'uri': new_uri,
                'filename': file.filename or 'unknown'
            }
        )


class S3BlobStore:

    def __init__(self, settings, loop=None):
        self._aws_access_key = settings['aws_client_id']
        self._aws_secret_key = settings['aws_client_secret']

        opts = dict(
            aws_secret_access_key=self._aws_secret_key,
            aws_access_key_id=self._aws_access_key,
            endpoint_url=settings.get('endpoint_url'),
            verify=settings.get('verify_ssl'),
            use_ssl=settings.get('ssl', True),
            region_name=settings.get('region_name'),
            config=aiobotocore.config.AioConfig(
                None,
                max_pool_connections=settings.get(
                    'max_pool_connections', 30)
            )
        )

        if loop is None:
            loop = asyncio.get_event_loop()
        self._loop = loop

        self._s3aiosession = aiobotocore.get_session(loop=loop)

        # This client is for downloads only
        self._s3aioclient = self._s3aiosession.create_client('s3', **opts)
        self._cached_buckets = []

        self._bucket_name = settings['bucket']

    async def get_bucket_name(self):
        container = task_vars.container.get()
        bucket_name = container.id.lower() + '.' + self._bucket_name

        bucket_name = bucket_name.replace('_', '-')

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
        await self._s3aioclient.close()

    async def iterate_bucket(self):
        container = task_vars.container.get()
        bucket_name = await self.get_bucket_name()
        result = await self._s3aioclient.list_objects(
            Bucket=bucket_name, Prefix=container.id + '/')
        paginator = self._s3aioclient.get_paginator('list_objects')
        async for result in paginator.paginate(
                Bucket=bucket_name, Prefix=container.id + '/'):
            for item in result.get('Contents', []):
                yield item
