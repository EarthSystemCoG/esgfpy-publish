'''
HDF parser specific to OCO-2 files.

@author: Luca Cinquini
'''
import datetime as dt
from esgfpy.publish.parsers import HdfMetadataFileParser
import os
import re
from dateutil.tz import tzutc
import numpy as np
from esgfpy.publish.consts import TAI93_DATETIME_START

# oco2*L2Std*.h5
FILENAME_PATTERN = "oco2.+L2Std.+\.h5"

class Oco2FileParser(HdfMetadataFileParser):
    
    def matches(self, filepath):
        '''Example filename: oco2_L2StdGL_89234a_100924_B3500_140205185958n.h5'''
        dir, filename = os.path.split(filepath)
        return re.match(FILENAME_PATTERN, filename)
    
    def getLatitudes(self, h5file):
        return h5file['RetrievalGeometry']['retrieval_latitude'][:]

    def getLongitudes(self, h5file):
        return h5file['RetrievalGeometry']['retrieval_longitude'][:]
    
    def getTimes(self, h5file):
        dateStrings = h5file['RetrievalHeader']['retrieval_time_string'][:]
        datasetTimes = [ dt.datetime.strptime(x,"%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=tzutc()) for x in dateStrings ]
        return datasetTimes
        #seconds = h5file['RetrievalHeader']['retrieval_time_tai93'][:]
        #times = np.empty( len(seconds), dtype=dt.datetime)
        #for i, secs in enumerate(seconds):
        #    times[i] = TAI93_DATETIME_START + dt.timedelta(seconds=int(secs))
        #return times