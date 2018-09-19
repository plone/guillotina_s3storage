from guillotina import testing

import aiohttp
import os
import pytest


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
                "aws_client_id": os.environ.get('S3CLOUD_ID', 'xxx'),
                "aws_client_secret": os.environ.get('S3CLOUD_SECRET', 'xxx')  # noqa
            }
        }],
        'static': {},
        'jsapps': {}
    })

    if 'S3CLOUD_ID' not in os.environ:
        settings['utilities'][0]['settings'].update({
            'endpoint_url': 'http://localhost:5000',
            'verify_ssl': False,
            'ssl': False,
        })


testing.configure_with(settings_configurator)


class PatchedBaseRequest(aiohttp.web_request.Request):
    @property
    def content(self):
        return self._payload


@pytest.fixture(scope='function')
def own_dummy_request(dummy_request):
    dummy_request.__class__ = PatchedBaseRequest
    yield dummy_request
