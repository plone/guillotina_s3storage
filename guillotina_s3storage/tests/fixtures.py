from guillotina import task_vars
from guillotina import testing

import aiohttp
import os
import pytest


def settings_configurator(settings):
    if 'applications' in settings:
        settings['applications'].append('guillotina_s3storage')
    else:
        settings['applications'] = ['guillotina_s3storage']

    settings['load_utilities']['s3'] = {
        "provides": "guillotina_s3storage.interfaces.IS3BlobStore",
        "factory": "guillotina_s3storage.storage.S3BlobStore",
        "settings": {
            "bucket": os.environ.get('S3CLOUD_BUCKET', 'testbucket'),
            "aws_client_id": os.environ.get('S3CLOUD_ID', 'x' * 10),
            "aws_client_secret": os.environ.get('S3CLOUD_SECRET', 'x' * 10)  # noqa
        }
    }

    if 'S3CLOUD_ID' not in os.environ:
        settings['load_utilities']['s3']['settings'].update({
            'endpoint_url': 'http://localhost:19000',
            'verify_ssl': False,
            'ssl': False,
        })


testing.configure_with(settings_configurator)


class PatchedBaseRequest(aiohttp.web_request.Request):
    @property
    def content(self):
        return self._payload

    def __enter__(self):
        task_vars.request.set(self)

    def __exit__(self, *args):
        pass

    async def __aenter__(self):
        return self.__enter__()

    async def __aexit__(self, *args):
        return self.__exit__()


@pytest.fixture(scope='function')
def own_dummy_request(dummy_request, minio):
    dummy_request.__class__ = PatchedBaseRequest
    yield dummy_request
