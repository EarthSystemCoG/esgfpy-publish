'''
Module :mod:`esgfpy.publish.factories.utils`
===================================

Module containing utility functions for factory classes.

@author: Luca Cinquini
'''

import string
import os
from esgfpy.publish.consts import (THUMBNAIL_EXT, SERVICE_THUMBNAIL, SERVICE_OPENDAP, SERVICE_THREDDS,
                                   VERSION, DATA_NODE, INSTANCE_ID, MASTER_ID)

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
    if relativeUrl[0] == '/': # remove leading  '/'
        relativeUrl = relativeUrl[1:]

    dir, filename = os.path.split(filepath)
    name, extension = os.path.splitext(filename)
    ext =  extension[1:] # remove '.' from file extension

    for serverName, serverBaseUrl in baseUrls.items():
        if serverBaseUrl[-1]!='/': # add trailing '/'
            serverBaseUrl = serverBaseUrl + "/"
        url = serverBaseUrl+relativeUrl
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

def generateId(identifier, metadata, addVersion=True):
    '''Generates an object 'id' starting from the supplied identifer,
       and possibly adding information from the metadata fields.
       Also sets the 'instance_id' and 'master_id' inside the metadata container.
       '''

    # 'master_id' is the same for all replicas, versions
    # Dataset example: 'gass-yotc-mip.01_NASAGMAO_GEOS5.expt1.zg'
    # File example: 'gass-yotc-mip.01_NASAGMAO_GEOS5.expt1.zg.GEOS5_AGCM.zg.1991010100-1991123118.nc'
    master_id = identifier

    # 'instace_id' is the same for all replicas of the same version
    # Dataset example: 'gass-yotc-mip.01_NASAGMAO_GEOS5.expt1.zg.v1'
    # File example: 'gass-yotc-mip.01_NASAGMAO_GEOS5.expt1.zg.GEOS5_AGCM.zg.1991010100-1991123118.nc.v1'
    instance_id = identifier
    if addVersion and VERSION in metadata:
        instance_id = "%s.%s" % (instance_id, metadata[VERSION][0])

    # 'id' is unique to each version and replica
    # Dataset example: 'gass-yotc-mip.01_NASAGMAO_GEOS5.expt1.zg.v1|esg-datanode.jpl.nasa.gov'
    # File example: 'gass-yotc-mip.01_NASAGMAO_GEOS5.expt1.zg.GEOS5_AGCM.zg.1991010100-1991123118.nc.v1|esg-datanode.jpl.nasa.gov'
    id = instance_id
    if DATA_NODE in metadata:
        id = "%s|%s" % (id, metadata[DATA_NODE][0])

    # set metadata fields
    metadata[INSTANCE_ID] = [ instance_id ]
    metadata[MASTER_ID] = [ master_id ]

    return id



