'''
Module :mod:`esgfpy.publish.factories.utils`
===================================

Module containing utility functions for factory classes.

@author: Luca Cinquini
'''

import string
import os
from esgfpy.publish.consts import THUMBNAIL_EXT, SERVICE_THUMBNAIL, SERVICE_OPENDAP, SERVICE_THREDDS
from esgfpy.publish.utils import getMimeType


def generateUrls(baseUrls, rootDirectory, filepath, isImage=False):
    '''Utility function to generate URL fields from templates.'''

    urls = []

    # add dataset/file access URLs
    relativeUrl = filepath
    if rootDirectory is not None:
        relativeUrl = relativeUrl.replace(rootDirectory,"")

    dir, filename = os.path.split(filepath)
    name, extension = os.path.splitext(filename)
    ext =  extension[1:] # remove '.' from file extension

    for serverName, serverBaseUrl in baseUrls.items():
        url = string.strip(serverBaseUrl,('/'))+relativeUrl
        if serverName == SERVICE_THUMBNAIL:
            if isImage:
                url = url.replace(ext, THUMBNAIL_EXT)
                urls.append( "%s|%s|%s" % ( url, getMimeType("jpeg"), serverName) )
        elif serverName == SERVICE_OPENDAP:
            url = "%s.html" % url # must add ".html" extension to OpenDAP URLs
            urls.append( "%s|%s|%s" % ( url, getMimeType(ext), serverName) )
        elif serverName == SERVICE_THREDDS:
            url = "%s/catalog.xml" % url # build THREDDS catalog URL
            urls.append( "%s|%s|%s" % ( url, getMimeType("thredds"), serverName) )
        else:
            urls.append( "%s|%s|%s" % ( url, getMimeType(ext), serverName) )

    return urls
