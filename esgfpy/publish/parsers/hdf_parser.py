'''
Parses metadata from HDF files.
'''

from esgfpy.publish.parsers.abstract_parser import AbstractMetadataFileParser
from esgfpy.publish.consts import (DATETIME_START, DATETIME_STOP, GEO,
                                   NORTH_DEGREES, SOUTH_DEGREES, EAST_DEGREES, WEST_DEGREES,
                                   VARIABLE)
import h5py
import numpy as np
import logging
import abc

class HdfMetadataFileParser(AbstractMetadataFileParser):
    '''Currently fake implementation: all metadata is hard-wired'''
    
    __metaclass__ = abc.ABCMeta
        
    def parseMetadata(self, filepath):
        
        metadata = {} # empty metadata dictionary
        
        if self.matches(filepath):
        
            logging.info("Parsing HDF file=%s" % filepath)
            
            # open HDF file
            h5file = h5py.File(filepath,'r')
            
            # latitudes
            lats = self.getLatitudes(h5file)
            lats = lats[ np.where( lats >= -90) ] # exclude missing values
            lats = lats[ np.where( lats <=  90 )] 
            minLat = np.min(lats) 
            maxLat = np.max(lats)
            logging.debug("Latitude min=%s max=%s" % (minLat, maxLat))
                
            # longitudes
            lons = self.getLongitudes(h5file)
            lons = lons[ np.where( lons >= -180) ] # exclude missing values
            lons = lons[ np.where( lons <=  360) ] 
            
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
                times = self.getTimes(h5file)
                minDateTime = np.min(times)
                maxDateTime = np.max(times)
                logging.debug("Datetime min=%s max=%s" % (minDateTime, maxDateTime))
                metadata[DATETIME_START] = [ minDateTime.strftime('%Y-%m-%dT%H:%M:%SZ') ]
                metadata[DATETIME_STOP] = [ maxDateTime.strftime('%Y-%m-%dT%H:%M:%SZ') ]
            except ValueError as e:
                logging.warn(e)
            
            # variables
            variables = self.getVariables(h5file)
            metadata[VARIABLE] = variables
            
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
        
        # loop over HDF groups
        for gname, gobj in h5file.items():
            if gobj.__class__.__name__=='Group':
    
                # loop over HDF datasets
                for dname, dobj in gobj.items():
                    if dobj.__class__.__name__ =='Dataset':
                        variables.append("%s/%s" % (gname, dname))

        return variables
        