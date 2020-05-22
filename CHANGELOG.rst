5.0.7 (2020-05-21)
------------------

- Open up version req


5.0.6 (2020-01-02)
------------------

- Add missing `range_supported`


5.0.5 (2020-01-02)
------------------

- Fix release

5.0.3 (2020-01-02)
------------------

- Add support for reading ranges
  [vangheem]

- add black formatting

5.0.2 (2019-11-01)
------------------

- Be able to import mypy
  [vangheem]


5.0.1 (2019-07-30)
------------------

- Support no attribute defined
  [bloodbare]


5.0.0 (2019-06-23)
------------------

- Guillotina 5 only support
  [vangheem]


2.0.4 (2019-05-31)
------------------

- S3 does not support bucket names with underscore [bloodbare]
  https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html


2.0.3 (2019-03-19)
------------------

- Fix backoff configuration
  [vangheem]


2.0.2 (2019-01-15)
------------------

- Use minio fixture as S3 server [masipcat]
- implement exists for head requests [vangheem]
- Removed unused code [masipcat]
- Fix S3BlobStore.finalize() crash [masipcat]


2.0.1 (2018-09-20)
------------------

- Pinning latest guillotina and aiohttp
  [lferran,dmanchon]


2.0.0 (2018-06-07)
------------------

- Upgrade to guillotina > 4
  [vangheem]


1.1.6 (2018-06-07)
------------------

- Pin version of guillotina
  [vangheem]


1.1.5 (2018-05-12)
------------------

- More strict object checks
  [vangheem]


1.1.4 (2018-05-09)
------------------

- Be able to provide max_pool_connections value in config
  [vangheem]


1.1.3 (2018-04-07)
------------------

- Fix error when uploading empty file
  [vangheem]


1.1.2 (2018-03-21)
------------------

- Prevent calling `dm.update` more than necessary
  [vangheem]


1.1.1 (2018-03-19)
------------------

- Be able to use `iter_data` with custom uri
  [vangheem]


1.1.0 (2018-03-19)
------------------

- Upgrade to new file manager API for Guillotina 2.5.0
  [vangheem]


1.0.43 (2018-03-13)
-------------------

- Fix download with custom URI
  [vangheem]


1.0.42 (2018-03-09)
-------------------

- Fix saving previous file
  [vangheem]


1.0.41 (2018-03-01)
-------------------

- Change when we store previous file info
  [vangheem]


1.0.40 (2018-02-22)
-------------------

- Customize more of the download
  [vangheem]


1.0.39 (2018-02-22)
-------------------

- Be able to specify uri to download
  [vangheem]


1.0.38 (2018-02-21)
-------------------

- Tweak IFileCleanup
  [vangheem]


1.0.37 (2018-02-20)
-------------------

- Implement IFileCleanup
  [vangheem]


1.0.36 (2018-02-02)
-------------------

- Fix retries to work with BytesIO data structure
  [vangheem]


1.0.35 (2017-12-28)
-------------------

- Always set _one_tus_shoot value to True/False since it could possibly already be set
  [vangheem]


1.0.34 (2017-11-03)
-------------------

- Do not allow error on aborting multipart upload


1.0.33 (2017-11-02)
-------------------

- save_file should use same tus/non tus support
  [vangheem]


1.0.32 (2017-10-25)
-------------------

- Fix issue with NoSuchKey Exception
  [vangheem]


1.0.31 (2017-10-24)
-------------------

- Do not do multipart upload for files smaller than 5mb
  [vangheem]


1.0.30 (2017-10-15)
-------------------

- Fix generating uri for one shot upload
  [vangheem]


1.0.29 (2017-10-12)
-------------------

- Make sure to register write on object for behavior files
  [vangheem]


1.0.28 (2017-10-11)
-------------------

- Return NotFound response when no file found on context
  [vangheem]


1.0.27 (2017-10-04)
-------------------

- Fix retry decorator
  [vangheem]


1.0.26 (2017-10-03)
-------------------

- Check type instead of None for existing value
  [vangheem]


1.0.25 (2017-10-02)
-------------------

- Use latest guillotina base classes
  [vangheem]

- Use field context if set
  [vangheem]


1.0.24 (2017-10-02)
-------------------

- Add copy_cloud_file method
  [vangheem]


1.0.23 (2017-09-29)
-------------------

- Limit request limit cache size to a max of the CHUNK_SIZE
  [vangheem]


1.0.22 (2017-09-29)
-------------------

- Cache data on request object in case of request conflict errors
  [vangheem]


1.0.21 (2017-09-19)
-------------------

- Retry errors to api
  [vangheem]


1.0.20 (2017-09-13)
-------------------

- Fix release


1.0.19 (2017-09-13)
-------------------

- Do not create multipart upload objects for files smaller than 5mb
  [vangheems]


1.0.18 (2017-09-11)
-------------------

- Make sure CORS headers are applied before we start sending a download result
  [vangheem]


1.0.17 (2017-09-11)
-------------------

- Be able to override disposition of download
  [vangheem]


1.0.16 (2017-09-06)
-------------------

- Fix aborting upload of existing when no multipart upload data is stored on
  the file object.
  [vangheem]

1.0.15 (2017-09-01)
-------------------

- Implement save_file method
  [vangheem]


1.0.14 (2017-08-15)
-------------------

- Provide iter_data method
  [vangheem]


1.0.13 (2017-06-21)
-------------------

- Make sure to set the loop used with the utility
  [vangheem]


1.0.12 (2017-06-18)
-------------------

- Be able to provide more s3 connection options
  [vangheem]


1.0.11 (2017-06-15)
-------------------

- Guess content type if none provided when downloading file
  [vangheem]


1.0.10 (2017-06-14)
-------------------

- Be able to customize content disposition header of file download
  [vangheem]


1.0.9 (2017-06-12)
------------------

- Make all network activity async
  [vangheem]

- Rename S3BlobStore.get_bucket to coroutine:S3BlobStore.get_bucket_name
  [vangheem]

- Rename S3BlobStore.session renamed to S3BlobStore._s3aiosession
  [vangheem]


1.0.8 (2017-05-19)
------------------

- Provide iterate_bucket method
  [vangheem]

- provide method to rename object
  [vangheem]

- Use keys that use the object's oid
  [vangheem]


1.0.7 (2017-05-02)
------------------

- Make sure to write to object when uploading
  [vangheem]


1.0.6 (2017-05-01)
------------------

- Fix reference to _md5hash instead of _md5 so serializing works
  [vangheem]

1.0.5 (2017-05-01)
------------------

- Fix bytes serialization issue
  [vangheem]


1.0.4 (2017-05-01)
------------------

- Do not inherit from BaseObject
  [vangheem]


1.0.3 (2017-05-01)
------------------

- S3File can take more all arguments in constructor now
  [vangheem]


1.0.2 (2017-04-26)
------------------

- utility needs to be able to take loop param
  [vangheem]


1.0.1 (2017-04-25)
------------------

- Compabilities with latest aiohttp
  [vangheem]


1.0.0 (2017-04-24)
------------------

- initial release
