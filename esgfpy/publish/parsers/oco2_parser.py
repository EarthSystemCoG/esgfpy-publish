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
FILENAME_PATTERN_LTCO2 = "oco2_LtCO2.+\.nc4$" # test_oco2_b70_20150704.nc4, 
FILENAME_PATTERN_LTSIF = "oco2_LtSIF.+\.nc4$" # OCO2-SIF-L2-150317-B7000r-fv1.nc
FILENAME_PATTERN_XCO2 = "ocoX_L3CO2.+\.nc4$" # test_oco2_b70_20150704.nc4, 

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
    
class Oco2LtSIFFileParser(Oco2FileParser):
    ''' Parser for OCO-2 Level 2 Lite SIF files (NetCDF)'''
    
    def matches(self, filepath):
        '''Example filename: oco2_LtSIF_150831_B7101_150903013836s.nc4'''
        
        dir, filename = os.path.split(filepath)
        return re.match(FILENAME_PATTERN_LTSIF, filename)
    
    def getLatitudes(self, h5file):
        return h5file['latitude'][:]

    def getLongitudes(self, h5file):
        return h5file['longitude'][:]
    
    def getTimes(self, h5file):
        
        datasetTimes = []
        
        # use TAI93 time
        # string sounding_time_string(phony_dim_16, phony_dim_17) ;
        seconds = h5file['time'][:]

        # loop over data points
        for secs in seconds:
            datasetTimes.append(TAI93_DATETIME_START + dt.timedelta(seconds=int(secs)) ) 
                                    
        return datasetTimes
    
class Oco2LtCO2FileParser(Oco2FileParser):
    '''Parser for OCO-2 L2 Lite files (NetCDF4 format)'''
    
    def matches(self, filepath):
        '''Example filename: oco2_LtCO2_150831_B7101A_150903021130s.nc4'''
        
        dir, filename = os.path.split(filepath)
        return re.match(FILENAME_PATTERN_LTCO2, filename)
    
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
                datasetTimes.append( dt.datetime.utcfromtimestamp(int(x)).replace(tzinfo=tzutc()) )
            except:
                pass # ignore one bad time stamp
            
        return datasetTimes
    
class Xco2FileParser(Oco2FileParser):
    '''Parser for XCO2 (NetCDF4 format)'''
    
    def matches(self, filepath):
        '''Example filename: ocoX_L3CO2_170105_170112_B8101_a7310Ao7305Br_170721052306s.nc4'''
        
        dir, filename = os.path.split(filepath)
        return re.match(FILENAME_PATTERN_XCO2, filename)
    
    def getLatitudes(self, h5file):
        return h5file['latitude'][:]

    def getLongitudes(self, h5file):
        return h5file['longitude'][:]
    
    def getTimes(self, h5file):
        
        # use UTC time
        datasetTimes = []
        
        # FIXME
        #datasetTimes.append( dt.datetime.utcfromtimestamp(int(x)).replace(tzinfo=tzutc()) )
        datasetTimes.append( dt.datetime(2017, 5, 21, 0, 0, 0, 0, tzinfo=tzutc()) )
        datasetTimes.append( dt.datetime(2017, 5, 29, 0, 0, 0, 0, tzinfo=tzutc()) )
                    
        return datasetTimes