.. contents::

GUILLOTINA_S3STORAGE
====================

S3 blob storage for guillotina.


Example config.json:

    "utilities": [{
        "provides": "guillotina_s3storage.interfaces.IS3BlobStore",
        "factory": "guillotina_s3storage.storage.S3BlobStore",
        "settings": {
          "aws_client_id": "<client id>",
          "aws_client_secret": "<client secret>",
          "endpoint": null,
          "ssl": True,
          "verify_ssl": null,
          "region_name": null
        }
    }]
