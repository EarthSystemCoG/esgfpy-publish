'''
Module :mod:`esgfpy.publish.utils`
===================================

Module containing utility functions.

@author: Luca Cinquini
'''

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

def isNull(s):
    """Checks wether a string is None or is empty."""

    if s is None or len(s.strip())==0:
        return True
    else:
        return False

def str2bool(v):
    return v.lower() in ("yes", "true", "t", "1")