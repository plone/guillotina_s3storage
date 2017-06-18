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
