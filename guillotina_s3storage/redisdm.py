from guillotina import configure
from guillotina.interfaces import IUploadDataManager
from guillotina_rediscache.files import RedisFileDataManager
from guillotina_s3storage.storage import IS3FileStorageManager


@configure.adapter(
    for_=IS3FileStorageManager,
    provides=IUploadDataManager)
class S3DataManager(RedisFileDataManager):
    pass
