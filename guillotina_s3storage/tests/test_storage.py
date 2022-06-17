import asyncio
import base64
import random
from hashlib import md5

import backoff
import botocore.exceptions
import pytest
from guillotina import task_vars
from guillotina.component import get_utility
from guillotina.content import Container
from guillotina.exceptions import UnRetryableRequestError
from guillotina.files import MAX_REQUEST_CACHE_SIZE
from guillotina.files import FileManager
from guillotina.files.adapter import DBDataManager
from guillotina.files.utils import generate_key
from guillotina.tests.utils import create_content
from guillotina.tests.utils import login
from zope.interface import Interface

from guillotina_s3storage.interfaces import IS3BlobStore
from guillotina_s3storage.storage import CHUNK_SIZE
from guillotina_s3storage.storage import DEFAULT_MAX_POOL_CONNECTIONS
from guillotina_s3storage.storage import RETRIABLE_EXCEPTIONS
from guillotina_s3storage.storage import S3FileField
from guillotina_s3storage.storage import S3FileStorageManager
from guillotina_s3storage.tests.mocks import AsyncMock


_test_gif = base64.b64decode(
    "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"  # noqa
)


class FakeContentReader:
    def __init__(self, file_data=_test_gif):
        self.set(file_data)

    def set(self, data):
        self._file_data = data
        self._pointer = 0
        self.chunks_sent = 0

    @property
    def eof(self):
        return self._pointer >= len(self._file_data)

    async def readexactly(self, size):
        data = self._file_data[self._pointer : self._pointer + size]
        self._pointer += len(data)
        if data:
            self.chunks_sent += 1
        if data and len(data) < size:
            raise asyncio.IncompleteReadError(data, size)
        return data

    async def readany(self):
        return self._file_data

    def seek(self, pos):
        self._pointer = pos


@pytest.fixture()
def reader():
    yield FakeContentReader()


@pytest.fixture()
def setup_container(dummy_request):
    login()
    container = create_content(Container, id="test-container")
    task_vars.container.set(container)
    yield container


@pytest.fixture()
async def util(setup_container):
    util = get_utility(IS3BlobStore)
    # cleanup
    async for item in util.iterate_bucket():
        await util._s3aioclient.delete_object(
            Bucket=await util.get_bucket_name(), Key=item["Key"]
        )

    yield util

    task_vars.container.set(None)


@pytest.fixture()
async def upload_request(dummy_request, reader, util, mock_txn):
    dummy_request._stream_reader = dummy_request._payload = reader
    yield dummy_request


class IContent(Interface):
    file = S3FileField()


async def get_all_objects():
    util = get_utility(IS3BlobStore)
    items = []
    async for item in util.iterate_bucket():
        items.append(item)
    return items


async def test_get_storage_object(util):
    assert await util.get_bucket_name() is not None


async def test_store_file_in_cloud(upload_request):
    upload_request.headers.update(
        {
            "Content-Type": "image/gif",
            "X-UPLOAD-MD5HASH": md5(_test_gif).hexdigest(),
            "X-UPLOAD-EXTENSION": "gif",
            "X-UPLOAD-SIZE": len(_test_gif),
            "X-UPLOAD-FILENAME": "test.gif",
        }
    )

    ob = create_content()
    ob.file = None
    mng = FileManager(ob, upload_request, IContent["file"].bind(ob))
    await mng.upload()
    assert ob.file._upload_file_id is None
    assert ob.file.uri is not None

    assert ob.file.content_type == "image/gif"
    assert ob.file.filename == "test.gif"
    assert ob.file._size == len(_test_gif)
    assert ob.file.md5 is not None
    assert ob.__uuid__ in ob.file.uri

    assert len(await get_all_objects()) == 1
    gmng = S3FileStorageManager(ob, upload_request, IContent["file"].bind(ob))
    await gmng.delete_upload(ob.file.uri)
    assert len(await get_all_objects()) == 0


async def test_store_file_uses_cached_request_data_on_retry(upload_request):

    upload_request.headers.update(
        {
            "Content-Type": "image/gif",
            "X-UPLOAD-MD5HASH": md5(_test_gif).hexdigest(),
            "X-UPLOAD-EXTENSION": "gif",
            "X-UPLOAD-SIZE": len(_test_gif),
            "X-UPLOAD-FILENAME": "test.gif",
        }
    )

    ob = create_content()
    ob.file = None
    mng = FileManager(ob, upload_request, IContent["file"].bind(ob))
    await mng.upload()
    assert ob.file._upload_file_id is None
    assert ob.file.uri is not None

    assert ob.file.content_type == "image/gif"
    assert ob.file.filename == "test.gif"
    assert ob.file._size == len(_test_gif)
    assert ob.file.md5 is not None
    assert ob.__uuid__ in ob.file.uri

    # test retry...
    upload_request._retry_attempt = 1
    await mng.upload()

    assert ob.file.content_type == "image/gif"
    assert ob.file.filename == "test.gif"
    assert ob.file._size == len(_test_gif)

    # should delete existing and reupload
    assert len(await get_all_objects()) == 1
    gmng = S3FileStorageManager(ob, upload_request, IContent["file"].bind(ob))
    await gmng.delete_upload(ob.file.uri)
    assert len(await get_all_objects()) == 0


async def test_store_file_in_cloud_using_tus(upload_request):
    upload_request.headers.update(
        {
            "Content-Type": "image/gif",
            "UPLOAD-MD5HASH": md5(_test_gif).hexdigest(),
            "UPLOAD-EXTENSION": "gif",
            "UPLOAD-FILENAME": "test.gif",
            "UPLOAD-LENGTH": len(_test_gif),
            "TUS-RESUMABLE": "1.0.0",
            "Content-Length": len(_test_gif),
            "upload-offset": 0,
        }
    )

    ob = create_content()
    ob.file = None
    mng = FileManager(ob, upload_request, IContent["file"].bind(ob))
    await mng.tus_create()
    await mng.tus_patch()
    assert ob.file._upload_file_id is None
    assert ob.file.uri is not None

    assert ob.file.content_type == "image/gif"
    assert ob.file.filename == "test.gif"
    assert ob.file._size == len(_test_gif)

    assert len(await get_all_objects()) == 1
    gmng = S3FileStorageManager(ob, upload_request, IContent["file"].bind(ob))
    await gmng.delete_upload(ob.file.uri)
    assert len(await get_all_objects()) == 0


async def test_multipart_upload_with_tus(upload_request, reader):
    file_data = _test_gif
    while len(file_data) < (11 * 1024 * 1024):
        file_data += _test_gif

    upload_request.headers.update(
        {
            "Content-Type": "image/gif",
            "UPLOAD-MD5HASH": md5(file_data).hexdigest(),
            "UPLOAD-EXTENSION": "gif",
            "UPLOAD-FILENAME": "test.gif",
            "TUS-RESUMABLE": "1.0.0",
            "UPLOAD-LENGTH": len(file_data),
        }
    )

    ob = create_content()
    ob.file = None
    mng = FileManager(ob, upload_request, IContent["file"].bind(ob))
    await mng.tus_create()

    chunk = file_data[: 5 * 1024 * 1024]
    upload_request.headers.update({"Content-Length": len(chunk), "upload-offset": 0})
    reader.set(chunk)
    upload_request._cache_data = b""
    upload_request._last_read_pos = 0
    await mng.tus_patch()

    chunk = file_data[5 * 1024 * 1024 :]
    upload_request.headers.update(
        {"Content-Length": len(chunk), "upload-offset": 5 * 1024 * 1024}
    )
    reader.set(chunk)
    upload_request._cache_data = b""
    upload_request._last_read_pos = 0
    await mng.tus_patch()

    assert ob.file._upload_file_id is None
    assert ob.file.uri is not None

    assert ob.file.content_type == "image/gif"
    assert ob.file.filename == "test.gif"
    assert ob.file._size == len(file_data)

    assert len(await get_all_objects()) == 1
    gmng = S3FileStorageManager(ob, upload_request, IContent["file"].bind(ob))
    await gmng.delete_upload(ob.file.uri)
    assert len(await get_all_objects()) == 0


async def test_large_file_with_upload(upload_request, reader):
    file_data = _test_gif
    while len(file_data) < (11 * 1024 * 1024):
        file_data += _test_gif

    upload_request.headers.update(
        {
            "Content-Type": "image/gif",
            "UPLOAD-MD5HASH": md5(file_data).hexdigest(),
            "UPLOAD-EXTENSION": "gif",
            "X-UPLOAD-FILENAME": "test.gif",
            "TUS-RESUMABLE": "1.0.0",
            "X-UPLOAD-SIZE": len(file_data),
        }
    )

    ob = create_content()
    ob.file = None
    mng = FileManager(ob, upload_request, IContent["file"].bind(ob))
    reader.set(file_data)
    upload_request._cache_data = b""
    upload_request._last_read_pos = 0
    await mng.upload()

    assert ob.file._upload_file_id is None
    assert ob.file.uri is not None

    assert ob.file.content_type == "image/gif"
    assert ob.file.filename == "test.gif"
    assert ob.file._size == len(file_data)

    assert len(await get_all_objects()) == 1
    gmng = S3FileStorageManager(ob, upload_request, IContent["file"].bind(ob))
    await gmng.delete_upload(ob.file.uri)
    assert len(await get_all_objects()) == 0


async def test_multipart_upload_with_tus_and_tid_conflict(upload_request, reader):
    file_data = _test_gif
    while len(file_data) < (11 * 1024 * 1024):
        file_data += _test_gif

    upload_request.headers.update(
        {
            "Content-Type": "image/gif",
            "UPLOAD-MD5HASH": md5(file_data).hexdigest(),
            "UPLOAD-EXTENSION": "gif",
            "UPLOAD-FILENAME": "test.gif",
            "TUS-RESUMABLE": "1.0.0",
            "UPLOAD-LENGTH": len(file_data),
        }
    )

    ob = create_content()
    ob.file = None
    mng = FileManager(ob, upload_request, IContent["file"].bind(ob))
    await mng.tus_create()

    chunk = file_data[: 5 * 1024 * 1024]
    upload_request.headers.update({"Content-Length": len(chunk), "upload-offset": 0})
    reader.set(chunk)
    upload_request._cache_data = b""
    upload_request._last_read_pos = 0
    await mng.tus_patch()

    # do this chunk over again...
    ob.__uploads__["file"]["offset"] -= len(chunk)
    ob.__uploads__["file"]["_block"] -= 1
    ob.__uploads__["file"]["_multipart"]["Parts"] = ob.__uploads__["file"][
        "_multipart"
    ]["Parts"][
        :-1
    ]  # noqa
    reader.set(chunk)
    upload_request._cache_data = b""
    upload_request._last_read_pos = 0
    await mng.tus_patch()

    chunk = file_data[5 * 1024 * 1024 :]
    upload_request.headers.update(
        {"Content-Length": len(chunk), "upload-offset": 5 * 1024 * 1024}
    )
    reader.set(chunk)
    upload_request._cache_data = b""
    upload_request._last_read_pos = 0
    await mng.tus_patch()

    assert ob.file._upload_file_id is None
    assert ob.file.uri is not None

    assert ob.file.content_type == "image/gif"
    assert ob.file.filename == "test.gif"
    assert ob.file._size == len(file_data)

    assert len(await get_all_objects()) == 1
    gmng = S3FileStorageManager(ob, upload_request, IContent["file"].bind(ob))
    await gmng.delete_upload(ob.file.uri)
    assert len(await get_all_objects()) == 0


def test_gen_key(upload_request):
    container = create_content(Container, id="test-container")
    task_vars.container.set(container)
    ob = create_content()
    key = generate_key(ob)
    assert key.startswith("test-container/")
    last = key.split("/")[-1]
    assert "::" in last
    assert last.split("::")[0] == ob.__uuid__


async def test_copy(upload_request):
    upload_request.headers.update(
        {
            "Content-Type": "image/gif",
            "X-UPLOAD-MD5HASH": md5(_test_gif).hexdigest(),
            "X-UPLOAD-EXTENSION": "gif",
            "X-UPLOAD-SIZE": len(_test_gif),
            "X-UPLOAD-FILENAME": "test.gif",
        }
    )

    ob = create_content()
    ob.file = None
    mng = FileManager(ob, upload_request, IContent["file"].bind(ob))
    await mng.upload()

    items = await get_all_objects()
    assert len(items) == 1

    new_ob = create_content()
    new_ob.file = None
    gmng = S3FileStorageManager(ob, upload_request, IContent["file"].bind(ob))
    dm = DBDataManager(gmng)
    await dm.load()
    new_gmng = S3FileStorageManager(
        new_ob, upload_request, IContent["file"].bind(new_ob)
    )
    new_dm = DBDataManager(new_gmng)
    await new_dm.load()
    await gmng.copy(new_gmng, new_dm)

    new_ob.file.content_type == ob.file.content_type
    new_ob.file.size == ob.file.size
    new_ob.file.uri != ob.file.uri

    items = await get_all_objects()
    assert len(items) == 2


async def test_iterate_storage(util, upload_request, reader):

    upload_request.headers.update(
        {
            "Content-Type": "image/gif",
            "X-UPLOAD-MD5HASH": md5(_test_gif).hexdigest(),
            "X-UPLOAD-EXTENSION": "gif",
            "X-UPLOAD-SIZE": len(_test_gif),
            "X-UPLOAD-FILENAME": "test.gif",
        }
    )

    for idx in range(20):
        reader.set(_test_gif)
        upload_request._cache_data = b""
        upload_request._last_read_pos = 0
        ob = create_content()
        ob.file = None
        mng = FileManager(ob, upload_request, IContent["file"].bind(ob))
        await mng.upload()

    items = []
    async for item in util.iterate_bucket():
        items.append(item)
    assert len(items) == 20


async def test_download(upload_request, reader):

    file_data = b""
    # we want to test multiple chunks here...
    while len(file_data) < CHUNK_SIZE:
        file_data += _test_gif

    upload_request.headers.update(
        {
            "Content-Type": "image/gif",
            "X-UPLOAD-MD5HASH": md5(file_data).hexdigest(),
            "X-UPLOAD-EXTENSION": "gif",
            "X-UPLOAD-SIZE": len(file_data),
            "X-UPLOAD-FILENAME": "test.gif",
        }
    )
    reader.set(file_data)
    upload_request.send = AsyncMock()
    ob = create_content()
    ob.file = None
    mng = FileManager(ob, upload_request, IContent["file"].bind(ob))
    await mng.upload()
    assert ob.file._upload_file_id is None
    assert ob.file.uri is not None
    resp = await mng.download()
    assert int(resp.content_length) == len(file_data)


async def test_raises_not_retryable(upload_request, reader):

    file_data = b""
    # we want to test multiple chunks here...
    while len(file_data) < MAX_REQUEST_CACHE_SIZE:
        file_data += _test_gif

    upload_request.headers.update(
        {
            "Content-Type": "image/gif",
            "X-UPLOAD-MD5HASH": md5(file_data).hexdigest(),
            "X-UPLOAD-EXTENSION": "gif",
            "X-UPLOAD-SIZE": len(file_data),
            "X-UPLOAD-FILENAME": "test.gif",
        }
    )
    reader.set(file_data)

    ob = create_content()
    ob.file = None
    mng = FileManager(ob, upload_request, IContent["file"].bind(ob))
    await mng.upload()

    upload_request._retry_attempt = 1
    with pytest.raises(UnRetryableRequestError):
        await mng.upload()


async def test_save_file(upload_request):

    ob = create_content()
    ob.file = None
    mng = FileManager(ob, upload_request, IContent["file"].bind(ob))

    async def generator():
        yield 5000 * b"x"

    await mng.save_file(generator, content_type="application/data")
    assert ob.file.size == 5000
    items = await get_all_objects()
    assert len(items) == 1


async def test_save_file_multipart(upload_request):

    ob = create_content()
    ob.file = None
    mng = FileManager(ob, upload_request, IContent["file"].bind(ob))

    async def generator():
        yield CHUNK_SIZE * b"x"
        yield CHUNK_SIZE * b"x"

    await mng.save_file(generator, content_type="application/data")
    assert ob.file.size == CHUNK_SIZE * 2
    items = await get_all_objects()
    assert len(items) == 1


async def test_save_same_chunk_multiple_times(util, upload_request):
    upload_file_id = "foobar124"
    bucket_name = await util.get_bucket_name()
    multipart = {"Parts": []}
    block = 1
    mpu = await util._s3aioclient.create_multipart_upload(
        Bucket=bucket_name, Key=upload_file_id
    )

    part = await util._s3aioclient.upload_part(
        Bucket=bucket_name,
        Key=upload_file_id,
        PartNumber=block,
        UploadId=mpu["UploadId"],
        Body=b"A" * 1024 * 1024 * 5,
    )
    multipart["Parts"].append({"PartNumber": block, "ETag": part["ETag"]})
    block += 1

    part = await util._s3aioclient.upload_part(
        Bucket=bucket_name,
        Key=upload_file_id,
        PartNumber=block,
        UploadId=mpu["UploadId"],
        Body=b"B" * 1024 * 1024 * 5,
    )
    multipart["Parts"].append({"PartNumber": block, "ETag": part["ETag"]})
    block += 1

    # a couple more but do not save multipart
    await util._s3aioclient.upload_part(
        Bucket=bucket_name,
        Key=upload_file_id,
        PartNumber=block,
        UploadId=mpu["UploadId"],
        Body=b"C" * 1024 * 1024 * 5,
    )
    await util._s3aioclient.upload_part(
        Bucket=bucket_name,
        Key=upload_file_id,
        PartNumber=block,
        UploadId=mpu["UploadId"],
        Body=b"D" * 1024 * 1024 * 5,
    )

    part = await util._s3aioclient.upload_part(
        Bucket=bucket_name,
        Key=upload_file_id,
        PartNumber=block,
        UploadId=mpu["UploadId"],
        Body=b"E" * 1024 * 1024 * 5,
    )
    multipart["Parts"].append({"PartNumber": block, "ETag": part["ETag"]})
    block += 1

    await util._s3aioclient.complete_multipart_upload(
        Bucket=bucket_name,
        Key=upload_file_id,
        UploadId=mpu["UploadId"],
        MultipartUpload=multipart,
    )

    ob = await util._s3aioclient.get_object(Bucket=bucket_name, Key=upload_file_id)
    data = b""
    async with ob["Body"] as stream:
        chunk = await stream.read(CHUNK_SIZE)
        while True:
            if not chunk:
                break
            data += chunk
            chunk = await stream.read(CHUNK_SIZE)

    assert data[0 : 1024 * 1024 * 5] == b"A" * 1024 * 1024 * 5
    assert data[1024 * 1024 * 5 : 1024 * 1024 * 10] == b"B" * 1024 * 1024 * 5
    assert data[1024 * 1024 * 10 : 1024 * 1024 * 15] == b"E" * 1024 * 1024 * 5


async def test_upload_empty_file(upload_request):

    ob = create_content()
    ob.file = None
    mng = FileManager(ob, upload_request, IContent["file"].bind(ob))

    async def generator():
        if False:
            yield ""

    await mng.save_file(generator, content_type="application/data")
    assert ob.file.size == 0
    items = await get_all_objects()
    assert len(items) == 1


async def test_file_exists(upload_request):

    upload_request.headers.update(
        {
            "Content-Type": "image/gif",
            "X-UPLOAD-MD5HASH": md5(_test_gif).hexdigest(),
            "X-UPLOAD-EXTENSION": "gif",
            "X-UPLOAD-SIZE": len(_test_gif),
            "X-UPLOAD-FILENAME": "test.gif",
        }
    )

    ob = create_content()
    ob.file = None
    mng = FileManager(ob, upload_request, IContent["file"].bind(ob))
    await mng.upload()
    assert ob.file._upload_file_id is None
    assert ob.file.uri is not None

    assert ob.file.content_type == "image/gif"
    assert ob.file.filename == "test.gif"
    assert ob.file._size == len(_test_gif)
    assert ob.file.md5 is not None
    assert ob.__uuid__ in ob.file.uri

    assert len(await get_all_objects()) == 1
    s3mng = S3FileStorageManager(ob, upload_request, IContent["file"].bind(ob))

    assert await s3mng.exists()

    await s3mng.delete_upload(ob.file.uri)
    assert len(await get_all_objects()) == 0

    assert not await s3mng.exists()


@backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, max_tries=2)
async def _test_exc_backoff(util):
    error_class = util._s3aioclient.exceptions.from_code("InvalidPart")
    raise error_class(
        {
            "Error": {
                "Code": "InvalidPart",
                "Message": "One or more of the specified parts could not be "
                "found. The part might not have been uploaded, "
                "or the specified entity tag might not have matched "
                "the part's entity tag.",
            }
        },
        "CompleteMultipartUpload",
    )


async def test_catch_client_error(util, upload_request):
    with pytest.raises(botocore.exceptions.ClientError):
        await _test_exc_backoff(util)


async def test_read_range(upload_request):
    upload_request.headers.update(
        {
            "Content-Type": "image/gif",
            "X-UPLOAD-MD5HASH": md5(_test_gif).hexdigest(),
            "X-UPLOAD-EXTENSION": "gif",
            "X-UPLOAD-SIZE": len(_test_gif),
            "X-UPLOAD-FILENAME": "test.gif",
        }
    )

    ob = create_content()
    ob.file = None
    mng = FileManager(ob, upload_request, IContent["file"].bind(ob))
    await mng.upload()

    s3mng = S3FileStorageManager(ob, upload_request, IContent["file"].bind(ob))

    async for chunk in s3mng.read_range(0, 100):
        assert len(chunk) == 100
        assert chunk == _test_gif[:100]

    async for chunk in s3mng.read_range(100, 200):
        assert len(chunk) == 100
        assert chunk == _test_gif[100:200]


async def test_custom_bucket_name(util):
    # No dots in the name, delimiter is -
    bucket_name = await util.get_bucket_name()
    assert bucket_name.startswith("test-container-testbucket")

    # Dots in the name, delimiter is .
    util._bucket_name = "testbucket.com"
    bucket_name = await util.get_bucket_name()
    assert bucket_name.startswith("test-container.testbucket.com")


async def _upload_uri(client, bucket, uri):
    print(f"Start uploading {uri}")
    mpu = await client.create_multipart_upload(Bucket=bucket, Key=uri)
    multipart = {"Parts": []}
    part = None
    for block in range(2):
        part = await client.upload_part(
            Bucket=bucket,
            Key=uri,
            PartNumber=block + 1,
            UploadId=mpu["UploadId"],
            Body="X" * CHUNK_SIZE,
        )
        multipart["Parts"].append({"PartNumber": block + 1, "ETag": part["ETag"]})

    await client.complete_multipart_upload(
        Bucket=bucket,
        Key=uri,
        UploadId=mpu["UploadId"],
        MultipartUpload=multipart,
    )
    print(f"Stop uploading {uri}")


async def test_connection_leak(util):
    bucket_name = await util.get_bucket_name()
    client = util._s3aioclient

    tasks = []
    test_uri_prefix = f"{random.randint(0, 999999)}-"
    for idx in range(DEFAULT_MAX_POOL_CONNECTIONS * 2):
        tasks.append(
            asyncio.create_task(
                _upload_uri(client, bucket_name, test_uri_prefix + str(idx))
            )
        )

    done, pending = await asyncio.wait(tasks)
    assert len(pending) == 0

    for task in done:
        assert task.exception() is None, task.exception()
