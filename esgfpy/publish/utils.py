'''
Module :mod:`esgfpy.publish.utils`
===================================

Module containing utility functions.

@author: Luca Cinquini
'''

def isNull(s):
    """Checks wether a string is None or is empty."""

    if s is None or len(s.strip())==0:
        return True
    else:
        return False

def str2bool(v):
    return v.lower() in ("yes", "true", "t", "1")