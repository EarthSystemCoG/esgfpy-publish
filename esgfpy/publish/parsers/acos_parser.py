'''
HDF parser specific to ACOS files.

@author: Luca Cinquini
'''
import datetime as dt
import numpy as np
from esgfpy.publish.parsers import HdfMetadataFileParser
from esgfpy.publish.consts import TAI93_DATETIME_START
import os
import re
import abc

FILENAME_PATTERN = "acos_L2s_(?P<yymmdd>\d+)_\d\d_Evaluation_.+\.h5"

class AcosFileParser(HdfMetadataFileParser):
    
    def matches(self, filepath):
        '''Example filename: acos_L2s_100129_16_Evaluation_v150151_L2s30400_r01_PolB_130904152222c.h5'''
        dir, filename = os.path.split(filepath)
        return re.match(FILENAME_PATTERN, filename)
    
    def getLatitudes(self, h5file):
        return h5file['SoundingGeometry']['sounding_latitude'][:]

    def getLongitudes(self, h5file):
        return h5file['SoundingGeometry']['sounding_longitude'][:]
    
    def getTimes(self, h5file):
        seconds = h5file['RetrievalHeader']['sounding_time_tai93'][:]
        times = np.empty( len(seconds), dtype=dt.datetime)
        for i, secs in enumerate(seconds):
            times[i] = TAI93_DATETIME_START + dt.timedelta(seconds=int(secs))
        return times