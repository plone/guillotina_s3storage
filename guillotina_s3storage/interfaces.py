# -*- coding: utf-8 -*-
from guillotina.interfaces import IFile
from guillotina.interfaces import IFileField
from zope.interface import Interface


class IS3FileField(IFileField):
    """Field marked as S3FileField
    """


# Configuration Utility


class IS3BlobStore(Interface):
    """Configuration utility.
    """


class IS3File(IFile):
    """Marker for a S3File
    """
