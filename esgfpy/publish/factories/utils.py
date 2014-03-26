'''
Module :mod:`esgfpy.publish.factories.utils`
===================================

Module containing utility functions for factory classes.

@author: Luca Cinquini
'''

import string
import os
from esgfpy.publish.consts import THUMBNAIL_EXT, SERVICE_THUMBNAIL, SERVICE_OPENDAP, SERVICE_THREDDS

def getMimeType(ext):
    """Returns the mime type for a given file extension."""

    ext = ext.lower()
    if ext=='jpg' or ext=='jpeg':
        return "image/jpeg"
    elif ext=="gif":
        return "image/gif"
    elif ext=="png":
        return "image/png"
    elif ext=="tiff" or ext=="tif":
        return "image/tiff"
    elif ext=="nc":
        return "application/x-netcdf"
    elif ext=="hdf":
        return "application/x-hdf"
    elif ext=="xml":
        return "text/xml"
    elif ext=="word" or ext=="wordx":
        return "application/msword"
    elif ext=="pdf":
        return "application/pdf"
    elif ext=="thredds":
        return "application/xml+thredds"
    else:
        return ""

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
