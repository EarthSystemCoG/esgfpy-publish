'''
HDF parser specific to OCO-2 files.

@author: Luca Cinquini
'''
import datetime as dt
from esgfpy.publish.parsers import HdfMetadataFileParser
import os
import re
from dateutil.tz import tzutc

# standard L2 files (HDF5)
FILENAME_PATTERN_STD = "oco2.+L2Std.+\.h5" # oco2_L2StdGL_89234a_100924_B3500_140205185958n.h5
# Lite L2 files (NetCDF4)
FILENAME_PATTERN_LTE = "oco2_L2.+\.nc4" # oco2_L2Daily_141127_B5000_150116014823s.nc4
        
class Oco2FileParser(HdfMetadataFileParser):
    '''Parser for OCO-2 L2 Standard files (HDF5 format)'''
    
    def matches(self, filepath):
        '''Example filename: oco2_L2StdGL_89234a_100924_B3500_140205185958n.h5'''
        
        dir, filename = os.path.split(filepath)
        return re.match(FILENAME_PATTERN_STD, filename)
    
    def getLatitudes(self, h5file):
        return h5file['RetrievalGeometry']['retrieval_latitude'][:]

    def getLongitudes(self, h5file):
        return h5file['RetrievalGeometry']['retrieval_longitude'][:]
    
    def getTimes(self, h5file):
        
        # use UTC time
        datasetTimes = []
        dateStrings = h5file['RetrievalHeader']['retrieval_time_string'][:]
        for x in dateStrings:
            try:
                datasetTimes.append( dt.datetime.strptime(x,"%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=tzutc()) )
            except:
                pass # ignore one bad time stamp
        return datasetTimes
    
class Oco2LiteFileParser(HdfMetadataFileParser):
    '''Parser for OCO-2 L2 Lite files (NetCDF4 format)'''
    
    def matches(self, filepath):
        '''Example filename: oco2_L2Daily_141127_B5000_150116014823s.nc4'''
        
        dir, filename = os.path.split(filepath)
        return re.match(FILENAME_PATTERN_LTE, filename)
    
    def getLatitudes(self, h5file):
        return h5file['latitude'][:]

    def getLongitudes(self, h5file):
        return h5file['longitude'][:]
    
    def getTimes(self, h5file):
        
        # use UTC time
        datasetTimes = []
        seconds = h5file['time'][:]
        for x in seconds:
            try:
                datasetTimes.append( dt.datetime.fromtimestamp(x).replace(tzinfo=tzutc()) )
            except:
                pass # ignore one bad time stamp
        return datasetTimes