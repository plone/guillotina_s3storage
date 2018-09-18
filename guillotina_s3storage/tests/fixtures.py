from guillotina import testing
from guillotina_s3storage.tests import mocks
import os
import pytest


AIOHTTP_MOCKER_ENABLED = False


def settings_configurator(settings):
    if 'applications' in settings:
        settings['applications'].append('guillotina_s3storage')
    else:
        settings['applications'] = ['guillotina_s3storage']

    settings.update({
        'utilities': [{
            "provides": "guillotina_s3storage.interfaces.IS3BlobStore",
            "factory": "guillotina_s3storage.storage.S3BlobStore",
            "settings": {
                "bucket": os.environ.get('S3CLOUD_BUCKET', 'testbucket'),
                "aws_client_id": os.environ.get('S3CLOUD_ID', 'accessKey1'),
                "aws_client_secret": os.environ.get('S3CLOUD_SECRET', 'verySecretKey1')  # noqa
            }
        }],
        'static': {},
        'jsapps': {}
    })

    if 'S3CLOUD_ID' not in os.environ:
        settings['utilities'][0]['settings'].update({
            'endpoint_url': 'http://localhost:8000',
            'verify_ssl': False,
            'ssl': False,
        })


testing.configure_with(settings_configurator)


@pytest.fixture(scope='function')
def aiohttp_mocks():
    if AIOHTTP_MOCKER_ENABLED:
        mocks.AiohttpMocker.start()
        yield mocks.AiohttpMocker
        mocks.AiohttpMocker.cleanup()
    else:
        yield None


@pytest.fixture(scope='function')
def own_dummy_request(dummy_request, aiohttp_mocks):
    yield dummy_request
