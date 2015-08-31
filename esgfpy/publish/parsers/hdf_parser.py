'''
Parses metadata from HDF files.
'''

from esgfpy.publish.parsers.abstract_parser import AbstractMetadataFileParser
from esgfpy.publish.consts import (DATETIME_START, DATETIME_STOP, GEO,
                                   NORTH_DEGREES, SOUTH_DEGREES, EAST_DEGREES, WEST_DEGREES,
                                   VARIABLE)
try:
    import h5py
except ImportError:
    pass
import numpy as np
import logging
import abc

INVALID_VALUE = -9999.

class HdfMetadataFileParser(AbstractMetadataFileParser):
    '''Currently fake implementation: all metadata is hard-wired'''
    
    __metaclass__ = abc.ABCMeta
        
    def parseMetadata(self, filepath):
        
        metadata = {} # empty metadata dictionary
        
        if self.matches(filepath):
        
            logging.info("Parsing file=%s" % filepath)
            
            # open HDF file
            h5file = h5py.File(filepath,'r')
            
            # extract data from file
            lats = self.getLatitudes(h5file)
            lons = self.getLongitudes(h5file)
            datetimes = self.getTimes(h5file)
            variables = self.getVariables(h5file)
            
            # store metadata
            storeMetadata(metadata, lons, lats, datetimes, variables)
            
            # close HDF file
            h5file.close()

        return metadata        
    
    @abc.abstractmethod
    def matches(self, filepath):
        '''
        Returns true if the file matches a regular expression,
        and therefore needs to be parsed.
        '''
        pass
        
    @abc.abstractmethod
    def getLatitudes(self, h5file):
        '''Returns a numpy array of latitude values.'''
        pass

    @abc.abstractmethod
    def getLongitudes(self, h5file):
        '''Returns a numpy array of longitude values.'''
        pass
    
    @abc.abstractmethod
    def getTimes(self, h5file):
        '''Returns a numpy array of python datetime values.'''
        pass
    
    def getVariables(self, h5file):
        '''
        Returns a list of variable names.
        The default implementation looks for all Group/Dataset objects in the HDF file.
        May be overwritten by subclasses.
        '''
        variables = []
        
        # loop over HDF datasets/groups
        for gname, gobj in h5file.items():
                    
            # top-level dataset    
            if gobj.__class__.__name__=='Dataset':
                variables.append("%s" % gname)
            
            # group/dataset
            elif gobj.__class__.__name__=='Group':
    
                # loop over HDF datasets
                for dname, dobj in gobj.items():
                    if dobj.__class__.__name__ =='Dataset':
                        variables.append("%s/%s" % (gname, dname))

        return variables
        
def storeMetadata(metadata, lons, lats, datetimes, variables):
    '''Utility method to process the numeric arrays and extract metadata values to store.'''
    
    # latitudes
    minLat = INVALID_VALUE
    maxLat = INVALID_VALUE
    if len(lats)>0:
        lats = lats[ np.where( lats >= -90) ] # exclude missing values
        lats = lats[ np.where( lats <=  90 )] 
        if len(lats)>0:
            minLat = np.min(lats) 
            maxLat = np.max(lats)
            logging.debug("Latitude min=%s max=%s" % (minLat, maxLat))
        
    # longitudes
    minLon = INVALID_VALUE
    maxLon = INVALID_VALUE
    if len(lons)>0:
        lons = lons[ np.where( lons >= -180) ] # exclude missing values
        lons = lons[ np.where( lons <=  360) ] 
        if len(lons)>0:
        
            if np.max(lons) > 180: # shift longitudes ?
                lons = lons - 360
                
            minLon = np.min(lons)
            maxLon = np.max(lons)
            logging.debug("Longitude min=%s max=%s" % (minLon, maxLon))
    
    if minLon >= -180 and maxLon<=180 and minLat>=-90 and maxLat<=90:
        
        # store geographic bounds
        metadata[NORTH_DEGREES] = [maxLat]
        metadata[SOUTH_DEGREES] = [minLat]
        metadata[WEST_DEGREES] = [minLon]
        metadata[EAST_DEGREES] = [maxLon]            
        # minX minY maxX maxY
        metadata[GEO] = ["%s %s %s %s" % (minLon, minLat, maxLon, maxLat)]
    
    # datetimes
    try:
        minDateTime = np.min(datetimes)
        maxDateTime = np.max(datetimes)
        logging.debug("Datetime min=%s max=%s" % (minDateTime, maxDateTime))
        metadata[DATETIME_START] = [ minDateTime.strftime('%Y-%m-%dT%H:%M:%SZ') ]
        metadata[DATETIME_STOP] = [ maxDateTime.strftime('%Y-%m-%dT%H:%M:%SZ') ]
    except ValueError as e:
        logging.warn(e)
    
    # variables
    metadata[VARIABLE] = variables