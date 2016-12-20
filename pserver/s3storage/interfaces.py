# -*- coding: utf-8 -*-
from zope.interface import Interface
from plone.server.interfaces import IFileField
from plone.server.interfaces.events import IFileFinishUploaded
from zope.interface import interfaces
from plone.server.interfaces import IFile


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
