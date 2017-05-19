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
        "bucket": os.environ['S3CLOUD_BUCKET'],
        "aws_client_id": os.environ['S3CLOUD_ID'],
        "aws_client_secret": os.environ['S3CLOUD_SECRET']
    }
}]

from guillotina.tests.conftest import *  # noqa
