# -*- coding: utf-8 -*-
from zope.interface import Interface
from plone.server.interfaces import IFileField
from plone.server.interfaces.events import IFileFinishUploaded
from zope.interface import interfaces
from plone.server.interfaces import IFile
from zope.schema import TextLine
from plone.server.directives import index
from plone.server.directives import metadata


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

    metadata('extension', 'md5')
    index('extension', type='text')
    extension = TextLine(
        title='Extension of the file',
        default='')

    index('md5', type='text')
    md5 = TextLine(
        title='MD5',
        default='')

# Events

class IInitialS3Upload(interfaces.IObjectEvent):
    """An upload has started
    """


class IFinishS3Upload(IFileFinishUploaded):
    """An upload has started
    """
