# -*- coding: utf-8 -*-
from plone.server import app_settings


def includeme(root):
    app_settings['cloud_storage'] = "pserver.s3storage.interfaces.IS3FileField"