.. contents::

GUILLOTINA_S3STORAGE
====================

S3 blob storage for guillotina.

The bucket name is built from the container id and the 'bucket' setting::

    "<container>.<bucket>"

Example config.json:

.. code-block:: json

    "applications": [
        "guillotina_s3storage"
    ]

    "load_utilities": {
        "s3": {
            "provides": "guillotina_s3storage.interfaces.IS3BlobStore",
            "factory": "guillotina_s3storage.storage.S3BlobStore",
            "settings": {
                "aws_client_id": "<client id>",
                "aws_client_secret": "<client secret>",
                "bucket": "<bucket name suffix>",
                "endpoint_url": null,
                "ssl": true,
                "verify_ssl": null,
                "region_name": null
            }
        }
    }
