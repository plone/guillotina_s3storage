# -*- coding: utf-8 -*-
from guillotina.interfaces import IFile
from guillotina.interfaces import IFileField
from guillotina.interfaces import IFileFinishUploaded
from zope.interface import Interface
from zope.interface import interfaces


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


# Events

class IInitialS3Upload(interfaces.IObjectEvent):
    """An upload has started
    """


class IFinishS3Upload(IFileFinishUploaded):
    """An upload has started
    """
