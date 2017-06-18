from guillotina.testing import TESTING_SETTINGS

import os


if 'applications' in TESTING_SETTINGS:
    TESTING_SETTINGS['applications'].append('guillotina_s3storage')
else:
    TESTING_SETTINGS['applications'] = ['guillotina_s3storage']

TESTING_SETTINGS['utilities'] = [{
    "provides": "guillotina_s3storage.interfaces.IS3BlobStore",
    "factory": "guillotina_s3storage.storage.S3BlobStore",
    "settings": {
        "bucket": os.environ.get('S3CLOUD_BUCKET', 'testbucket'),
        "aws_client_id": os.environ.get('S3CLOUD_ID', 'accessKey1'),
        "aws_client_secret": os.environ.get('S3CLOUD_SECRET', 'verySecretKey1')
    }
}]

if 'S3CLOUD_ID' not in os.environ:
    TESTING_SETTINGS['utilities'][0]['settings'].update({
        'endpoint_url': 'http://localhost:8000',
        'verify_ssl': False,
        'ssl': False
    })

pytest_plugins = [
    'guillotina.tests.fixtures'
]
