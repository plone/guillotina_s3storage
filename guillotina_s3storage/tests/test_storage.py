from guillotina.component import get_utility
from guillotina.exceptions import UnRetryableRequestError
from guillotina.files import FileManager
from guillotina.files import MAX_REQUEST_CACHE_SIZE
from guillotina.files.adapter import DBDataManager
from guillotina.files.utils import generate_key
from guillotina.tests.utils import create_content
from guillotina.tests.utils import login
from guillotina_s3storage.interfaces import IS3BlobStore
from guillotina_s3storage.storage import CHUNK_SIZE
from guillotina_s3storage.storage import S3FileField
from guillotina_s3storage.storage import S3FileStorageManager
from hashlib import md5
from zope.interface import Interface

import base64
import pytest


_test_gif = base64.b64decode('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')  # noqa


class FakeContentReader:

    def __init__(self, file_data=_test_gif):
        self._file_data = file_data
        self._pointer = 0

    async def readexactly(self, size):
        data = self._file_data[self._pointer:self._pointer + size]
        self._pointer += len(data)
        return data

    def seek(self, pos):
        self._pointer = pos


class IContent(Interface):
    file = S3FileField()


async def _cleanup():
    util = get_utility(IS3BlobStore)
    async for item in util.iterate_bucket():
        await util._s3aioclient.delete_object(
            Bucket=await util.get_bucket_name(), Key=item['Key'])


async def get_all_objects():
    util = get_utility(IS3BlobStore)
    items = []
    async for item in util.iterate_bucket():
        items.append(item)
    return items


async def test_get_storage_object(dummy_request):
    request = dummy_request  # noqa
    request._container_id = 'test-container'
    util = get_utility(IS3BlobStore)
    assert await util.get_bucket_name() is not None


async def test_store_file_in_cloud(dummy_request):
    request = dummy_request  # noqa
    login(request)
    request._container_id = 'test-container'
    await _cleanup()

    request.headers.update({
        'Content-Type': 'image/gif',
        'X-UPLOAD-MD5HASH': md5(_test_gif).hexdigest(),
        'X-UPLOAD-EXTENSION': 'gif',
        'X-UPLOAD-SIZE': len(_test_gif),
        'X-UPLOAD-FILENAME': 'test.gif'
    })
    request._payload = FakeContentReader()

    ob = create_content()
    ob.file = None
    mng = FileManager(ob, request, IContent['file'].bind(ob))
    await mng.upload()
    assert ob.file._upload_file_id is None
    assert ob.file.uri is not None

    assert ob.file.content_type == 'image/gif'
    assert ob.file.filename == 'test.gif'
    assert ob.file._size == len(_test_gif)
    assert ob.file.md5 is not None
    assert ob._p_oid in ob.file.uri

    assert len(await get_all_objects()) == 1
    gmng = S3FileStorageManager(ob, request, IContent['file'].bind(ob))
    await gmng.delete_upload(ob.file.uri)
    assert len(await get_all_objects()) == 0


async def test_store_file_uses_cached_request_data_on_retry(dummy_request):
    request = dummy_request  # noqa
    login(request)
    request._container_id = 'test-container'
    await _cleanup()

    request.headers.update({
        'Content-Type': 'image/gif',
        'X-UPLOAD-MD5HASH': md5(_test_gif).hexdigest(),
        'X-UPLOAD-EXTENSION': 'gif',
        'X-UPLOAD-SIZE': len(_test_gif),
        'X-UPLOAD-FILENAME': 'test.gif'
    })
    request._payload = FakeContentReader()

    ob = create_content()
    ob.file = None
    mng = FileManager(ob, request, IContent['file'].bind(ob))
    await mng.upload()
    assert ob.file._upload_file_id is None
    assert ob.file.uri is not None

    assert ob.file.content_type == 'image/gif'
    assert ob.file.filename == 'test.gif'
    assert ob.file._size == len(_test_gif)
    assert ob.file.md5 is not None
    assert ob._p_oid in ob.file.uri

    # test retry...
    request._retry_attempt = 1
    await mng.upload()

    assert ob.file.content_type == 'image/gif'
    assert ob.file.filename == 'test.gif'
    assert ob.file._size == len(_test_gif)

    # should delete existing and reupload
    assert len(await get_all_objects()) == 1
    gmng = S3FileStorageManager(ob, request, IContent['file'].bind(ob))
    await gmng.delete_upload(ob.file.uri)
    assert len(await get_all_objects()) == 0


async def test_store_file_in_cloud_using_tus(dummy_request):
    request = dummy_request  # noqa
    login(request)
    request._container_id = 'test-container'
    await _cleanup()

    request.headers.update({
        'Content-Type': 'image/gif',
        'UPLOAD-MD5HASH': md5(_test_gif).hexdigest(),
        'UPLOAD-EXTENSION': 'gif',
        'UPLOAD-FILENAME': 'test.gif',
        'UPLOAD-LENGTH': len(_test_gif),
        'TUS-RESUMABLE': '1.0.0',
        'Content-Length': len(_test_gif),
        'upload-offset': 0
    })
    request._payload = FakeContentReader()

    ob = create_content()
    ob.file = None
    mng = FileManager(ob, request, IContent['file'].bind(ob))
    await mng.tus_create()
    await mng.tus_patch()
    assert ob.file._upload_file_id is None
    assert ob.file.uri is not None

    assert ob.file.content_type == 'image/gif'
    assert ob.file.filename == 'test.gif'
    assert ob.file._size == len(_test_gif)

    assert len(await get_all_objects()) == 1
    gmng = S3FileStorageManager(ob, request, IContent['file'].bind(ob))
    await gmng.delete_upload(ob.file.uri)
    assert len(await get_all_objects()) == 0


async def test_multipart_upload_with_tus(dummy_request):
    request = dummy_request  # noqa
    login(request)
    request._container_id = 'test-container'
    await _cleanup()

    file_data = _test_gif
    while len(file_data) < (11 * 1024 * 1024):
        file_data += _test_gif

    request.headers.update({
        'Content-Type': 'image/gif',
        'UPLOAD-MD5HASH': md5(file_data).hexdigest(),
        'UPLOAD-EXTENSION': 'gif',
        'UPLOAD-FILENAME': 'test.gif',
        'TUS-RESUMABLE': '1.0.0',
        'UPLOAD-LENGTH': len(file_data)
    })
    request._payload = FakeContentReader()

    ob = create_content()
    ob.file = None
    mng = FileManager(ob, request, IContent['file'].bind(ob))
    await mng.tus_create()

    chunk = file_data[:5 * 1024 * 1024]
    request.headers.update({
        'Content-Length': len(chunk),
        'upload-offset': 0
    })
    request._payload = FakeContentReader(chunk)
    request._cache_data = b''
    request._last_read_pos = 0
    await mng.tus_patch()

    chunk = file_data[5 * 1024 * 1024:]
    request.headers.update({
        'Content-Length': len(chunk),
        'upload-offset': 5 * 1024 * 1024
    })
    request._payload = FakeContentReader(chunk)
    request._cache_data = b''
    request._last_read_pos = 0
    await mng.tus_patch()

    assert ob.file._upload_file_id is None
    assert ob.file.uri is not None

    assert ob.file.content_type == 'image/gif'
    assert ob.file.filename == 'test.gif'
    assert ob.file._size == len(file_data)

    assert len(await get_all_objects()) == 1
    gmng = S3FileStorageManager(ob, request, IContent['file'].bind(ob))
    await gmng.delete_upload(ob.file.uri)
    assert len(await get_all_objects()) == 0


async def test_large_file_with_upload(dummy_request):
    request = dummy_request  # noqa
    login(request)
    request._container_id = 'test-container'
    await _cleanup()

    file_data = _test_gif
    while len(file_data) < (11 * 1024 * 1024):
        file_data += _test_gif

    request.headers.update({
        'Content-Type': 'image/gif',
        'UPLOAD-MD5HASH': md5(file_data).hexdigest(),
        'UPLOAD-EXTENSION': 'gif',
        'X-UPLOAD-FILENAME': 'test.gif',
        'TUS-RESUMABLE': '1.0.0',
        'X-UPLOAD-SIZE': len(file_data)
    })
    request._payload = FakeContentReader()

    ob = create_content()
    ob.file = None
    mng = FileManager(ob, request, IContent['file'].bind(ob))
    request._payload = FakeContentReader(file_data)
    request._cache_data = b''
    request._last_read_pos = 0
    await mng.upload()

    assert ob.file._upload_file_id is None
    assert ob.file.uri is not None

    assert ob.file.content_type == 'image/gif'
    assert ob.file.filename == 'test.gif'
    assert ob.file._size == len(file_data)

    assert len(await get_all_objects()) == 1
    gmng = S3FileStorageManager(ob, request, IContent['file'].bind(ob))
    await gmng.delete_upload(ob.file.uri)
    assert len(await get_all_objects()) == 0


async def test_multipart_upload_with_tus_and_tid_conflict(dummy_request):
    request = dummy_request  # noqa
    login(request)
    request._container_id = 'test-container'
    await _cleanup()

    file_data = _test_gif
    while len(file_data) < (11 * 1024 * 1024):
        file_data += _test_gif

    request.headers.update({
        'Content-Type': 'image/gif',
        'UPLOAD-MD5HASH': md5(file_data).hexdigest(),
        'UPLOAD-EXTENSION': 'gif',
        'UPLOAD-FILENAME': 'test.gif',
        'TUS-RESUMABLE': '1.0.0',
        'UPLOAD-LENGTH': len(file_data)
    })
    request._payload = FakeContentReader()

    ob = create_content()
    ob.file = None
    mng = FileManager(ob, request, IContent['file'].bind(ob))
    await mng.tus_create()

    chunk = file_data[:5 * 1024 * 1024]
    request.headers.update({
        'Content-Length': len(chunk),
        'upload-offset': 0
    })
    request._payload = FakeContentReader(chunk)
    request._cache_data = b''
    request._last_read_pos = 0
    await mng.tus_patch()

    # do this chunk over again...
    ob.__uploads__['file']['offset'] -= len(chunk)
    ob.__uploads__['file']['_block'] -= 1
    ob.__uploads__['file']['_multipart']['Parts'] = ob.__uploads__['file']['_multipart']['Parts'][:-1]  # noqa
    request._payload = FakeContentReader(chunk)
    request._cache_data = b''
    request._last_read_pos = 0
    await mng.tus_patch()

    chunk = file_data[5 * 1024 * 1024:]
    request.headers.update({
        'Content-Length': len(chunk),
        'upload-offset': 5 * 1024 * 1024
    })
    request._payload = FakeContentReader(chunk)
    request._cache_data = b''
    request._last_read_pos = 0
    await mng.tus_patch()

    assert ob.file._upload_file_id is None
    assert ob.file.uri is not None

    assert ob.file.content_type == 'image/gif'
    assert ob.file.filename == 'test.gif'
    assert ob.file._size == len(file_data)

    assert len(await get_all_objects()) == 1
    gmng = S3FileStorageManager(ob, request, IContent['file'].bind(ob))
    await gmng.delete_upload(ob.file.uri)
    assert len(await get_all_objects()) == 0


def test_gen_key(dummy_request):
    request = dummy_request  # noqa
    request._container_id = 'test-container'
    ob = create_content()
    key = generate_key(request, ob)
    assert key.startswith('test-container/')
    last = key.split('/')[-1]
    assert '::' in last
    assert last.split('::')[0] == ob._p_oid


async def test_copy(dummy_request):
    request = dummy_request  # noqa
    login(request)
    request._container_id = 'test-container'
    await _cleanup()

    request.headers.update({
        'Content-Type': 'image/gif',
        'X-UPLOAD-MD5HASH': md5(_test_gif).hexdigest(),
        'X-UPLOAD-EXTENSION': 'gif',
        'X-UPLOAD-SIZE': len(_test_gif),
        'X-UPLOAD-FILENAME': 'test.gif'
    })
    request._payload = FakeContentReader()

    ob = create_content()
    ob.file = None
    mng = FileManager(ob, request, IContent['file'].bind(ob))
    await mng.upload()

    items = await get_all_objects()
    assert len(items) == 1

    new_ob = create_content()
    new_ob.file = None
    gmng = S3FileStorageManager(ob, request, IContent['file'].bind(ob))
    dm = DBDataManager(gmng)
    await dm.load()
    new_gmng = S3FileStorageManager(new_ob, request, IContent['file'].bind(new_ob))
    new_dm = DBDataManager(new_gmng)
    await new_dm.load()
    await gmng.copy(new_gmng, new_dm)

    new_ob.file.content_type == ob.file.content_type
    new_ob.file.size == ob.file.size
    new_ob.file.uri != ob.file.uri

    items = await get_all_objects()
    assert len(items) == 2


async def test_iterate_storage(dummy_request):
    request = dummy_request  # noqa
    login(request)
    request._container_id = 'test-container'
    await _cleanup()

    request.headers.update({
        'Content-Type': 'image/gif',
        'X-UPLOAD-MD5HASH': md5(_test_gif).hexdigest(),
        'X-UPLOAD-EXTENSION': 'gif',
        'X-UPLOAD-SIZE': len(_test_gif),
        'X-UPLOAD-FILENAME': 'test.gif'
    })

    for _ in range(20):
        request._payload = FakeContentReader()
        request._cache_data = b''
        request._last_read_pos = 0
        ob = create_content()
        ob.file = None
        mng = FileManager(ob, request, IContent['file'].bind(ob))
        await mng.upload()

    util = get_utility(IS3BlobStore)
    items = []
    async for item in util.iterate_bucket():
        items.append(item)
    assert len(items) == 20

    await _cleanup()


async def test_download(dummy_request):
    request = dummy_request  # noqa
    login(request)
    request._container_id = 'test-container'
    await _cleanup()

    file_data = b''
    # we want to test multiple chunks here...
    while len(file_data) < CHUNK_SIZE:
        file_data += _test_gif

    request.headers.update({
        'Content-Type': 'image/gif',
        'X-UPLOAD-MD5HASH': md5(file_data).hexdigest(),
        'X-UPLOAD-EXTENSION': 'gif',
        'X-UPLOAD-SIZE': len(file_data),
        'X-UPLOAD-FILENAME': 'test.gif'
    })
    request._payload = FakeContentReader(file_data)

    ob = create_content()
    ob.file = None
    mng = FileManager(ob, request, IContent['file'].bind(ob))
    await mng.upload()
    assert ob.file._upload_file_id is None
    assert ob.file.uri is not None

    resp = await mng.download()
    assert resp.content_length == len(file_data)


async def test_raises_not_retryable(dummy_request):
    request = dummy_request  # noqa
    login(request)
    request._container_id = 'test-container'
    await _cleanup()

    file_data = b''
    # we want to test multiple chunks here...
    while len(file_data) < MAX_REQUEST_CACHE_SIZE:
        file_data += _test_gif

    request.headers.update({
        'Content-Type': 'image/gif',
        'X-UPLOAD-MD5HASH': md5(file_data).hexdigest(),
        'X-UPLOAD-EXTENSION': 'gif',
        'X-UPLOAD-SIZE': len(file_data),
        'X-UPLOAD-FILENAME': 'test.gif'
    })
    request._payload = FakeContentReader(file_data)

    ob = create_content()
    ob.file = None
    mng = FileManager(ob, request, IContent['file'].bind(ob))
    await mng.upload()

    request._retry_attempt = 1
    with pytest.raises(UnRetryableRequestError):
        await mng.upload()


async def test_save_file(dummy_request):
    request = dummy_request  # noqa
    login(request)
    request._container_id = 'test-container'
    await _cleanup()

    ob = create_content()
    ob.file = None
    mng = FileManager(ob, request, IContent['file'].bind(ob))

    async def generator():
        yield 5000 * b'x'
    await mng.save_file(generator, content_type='application/data')
    assert ob.file.size == 5000
    items = await get_all_objects()
    assert len(items) == 1


async def test_save_file_multipart(dummy_request):
    request = dummy_request  # noqa
    login(request)
    request._container_id = 'test-container'
    await _cleanup()

    ob = create_content()
    ob.file = None
    mng = FileManager(ob, request, IContent['file'].bind(ob))

    async def generator():
        yield CHUNK_SIZE * b'x'
        yield CHUNK_SIZE * b'x'
    await mng.save_file(generator, content_type='application/data')
    assert ob.file.size == CHUNK_SIZE * 2
    items = await get_all_objects()
    assert len(items) == 1


async def test_save_same_chunk_multiple_times(dummy_request):
    request = dummy_request  # noqa
    request._container_id = 'test-container'
    util = get_utility(IS3BlobStore)
    upload_file_id = 'foobar124'
    bucket_name = await util.get_bucket_name()
    multipart = {'Parts': []}
    block = 1
    mpu = await util._s3aioclient.create_multipart_upload(
        Bucket=bucket_name,
        Key=upload_file_id
    )

    part = await util._s3aioclient.upload_part(
        Bucket=bucket_name,
        Key=upload_file_id,
        PartNumber=block,
        UploadId=mpu['UploadId'],
        Body=b'A' * 1024 * 1024 * 5)
    multipart['Parts'].append({
        'PartNumber': block,
        'ETag': part['ETag']
    })
    block += 1

    part = await util._s3aioclient.upload_part(
        Bucket=bucket_name,
        Key=upload_file_id,
        PartNumber=block,
        UploadId=mpu['UploadId'],
        Body=b'B' * 1024 * 1024 * 5)
    multipart['Parts'].append({
        'PartNumber': block,
        'ETag': part['ETag']
    })
    block += 1

    # a couple more but do not save multipart
    await util._s3aioclient.upload_part(
        Bucket=bucket_name,
        Key=upload_file_id,
        PartNumber=block,
        UploadId=mpu['UploadId'],
        Body=b'C' * 1024 * 1024 * 5)
    await util._s3aioclient.upload_part(
        Bucket=bucket_name,
        Key=upload_file_id,
        PartNumber=block,
        UploadId=mpu['UploadId'],
        Body=b'D' * 1024 * 1024 * 5)

    part = await util._s3aioclient.upload_part(
        Bucket=bucket_name,
        Key=upload_file_id,
        PartNumber=block,
        UploadId=mpu['UploadId'],
        Body=b'E' * 1024 * 1024 * 5)
    multipart['Parts'].append({
        'PartNumber': block,
        'ETag': part['ETag']
    })
    block += 1

    await util._s3aioclient.complete_multipart_upload(
        Bucket=bucket_name,
        Key=upload_file_id,
        UploadId=mpu['UploadId'],
        MultipartUpload=multipart)

    ob = await util._s3aioclient.get_object(
        Bucket=bucket_name,
        Key=upload_file_id,
    )
    data = b''
    async with ob['Body'] as stream:
        chunk = await stream.read(CHUNK_SIZE)
        while True:
            if not chunk:
                break
            data += chunk
            chunk = await stream.read(CHUNK_SIZE)

    assert data[0:1024 * 1024 * 5] == b'A' * 1024 * 1024 * 5
    assert data[1024 * 1024 * 5:1024 * 1024 * 10] == b'B' * 1024 * 1024 * 5
    assert data[1024 * 1024 * 10:1024 * 1024 * 15] == b'E' * 1024 * 1024 * 5


async def test_upload_empty_file(dummy_request):
    request = dummy_request  # noqa
    login(request)
    request._container_id = 'test-container'
    await _cleanup()

    ob = create_content()
    ob.file = None
    mng = FileManager(ob, request, IContent['file'].bind(ob))

    async def generator():
        if False:
            yield ''
    await mng.save_file(generator, content_type='application/data')
    assert ob.file.size == 0
    items = await get_all_objects()
    assert len(items) == 1
