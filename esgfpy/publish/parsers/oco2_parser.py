'''
HDF parser specific to OCO-2 files.

@author: Luca Cinquini
'''
import datetime as dt
from esgfpy.publish.parsers import HdfMetadataFileParser
import os
import re
from dateutil.tz import tzutc
from esgfpy.publish.consts import TAI93_DATETIME_START
import numpy as np

# standard L2 files (HDF5)
FILENAME_PATTERN_STD = "oco2_L2Std.+\.h5" # oco2_L2StdGL_89234a_100924_B3500_140205185958n.h5
# SIF L2 files (HDF5)
FILENAME_PATTERN_IDP = "oco2_L2IDP.+\.h5" # oco2_L2IDPGL_03783a_150319_B6000r_150328142340.h5
# Lite L2 files (NetCDF4)
#FILENAME_PATTERN_LTE = "oco2_L2.+\.nc4" # oco2_L2Daily_141127_B5000_150116014823s.nc4
# FIXME
FILENAME_PATTERN_LTE = "test_oco2_.+\.nc4" # oco2_L2Daily_141127_B5000_150116014823s.nc4

AQUISITION_MODE = "AcquisitionMode"

class Oco2FileParser(HdfMetadataFileParser):
    '''Base class for all OCO2 HDF file parsers.'''
    
    def parseMetadata(self, filepath):
        
        metadata = super(Oco2FileParser, self).parseMetadata(filepath)
        
        if 'GL_' in filepath:
            metadata[AQUISITION_MODE] = ['GL']
        elif 'ND_' in filepath:
            metadata[AQUISITION_MODE] = ['ND']
        elif 'TG_' in filepath:
            metadata[AQUISITION_MODE] = ['TG']
        
        return metadata
        
class Oco2L2StdFileParser(Oco2FileParser):
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
    
class Oco2L2IDPFileParser(Oco2FileParser):
    ''' Parser for OCO-2 Level 2 IMAP-DOAS preprocessor products, 
        including Solar Induced Fluorescence (SIF) fields)  
        (HDF5 format)'''
    
    def matches(self, filepath):
        '''Example filename: oco2_L2IDPGL_03783a_150319_B6000r_150328142340.h5'''
        
        dir, filename = os.path.split(filepath)
        return re.match(FILENAME_PATTERN_IDP, filename)
    
    def getLatitudes(self, h5file):
        return h5file['SoundingGeometry']['sounding_latitude'][:]

    def getLongitudes(self, h5file):
        return h5file['SoundingGeometry']['sounding_longitude'][:]
    
    def getTimes(self, h5file):
        
        datasetTimes = []
        
        # use TAI93 time
        # string sounding_time_string(phony_dim_16, phony_dim_17) ;
        seconds = h5file['SoundingGeometry']['sounding_time_tai93'][:]

        # loop over data points
        for i in range(0, seconds.shape[0]):
            
            secs = seconds[i,:]  # slice
            _secs = secs[ np.where( secs != -999999.) ]
            if len(_secs) > 0:
                time_tai93 = np.mean( _secs )
                datasetTimes.append(TAI93_DATETIME_START + dt.timedelta(seconds=int(time_tai93)) ) 
                
        # use UTC time
        #dateStrings = h5file['SoundingGeometry']['sounding_time_string'][:]
        #for x in dateStrings:
        #    try:
        #        datasetTimes.append( dt.datetime.strptime(x,"%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=tzutc()) )
        #    except:
        #        pass # ignore one bad time stamp
        
        return datasetTimes
    
class Oco2L2LiteFileParser(Oco2FileParser):
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