# -*- coding: utf-8 -*-
from guillotina import configure


app_settings = {
    'cloud_storage': "guillotina_s3storage.interfaces.IS3FileField"
}


def includeme(root):
    configure.scan('guillotina_s3storage.storage')
