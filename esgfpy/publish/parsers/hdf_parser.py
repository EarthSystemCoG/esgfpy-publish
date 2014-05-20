'''
Parses metadata from HDF files.
'''

from esgfpy.publish.parsers.abstract_parser import AbstractMetadataFileParser
import datetime as dt
from dateutil.tz import tzutc
from esgfpy.publish.consts import (DATETIME_START, DATETIME_STOP, GEO,
                                   NORTH_DEGREES, SOUTH_DEGREES, EAST_DEGREES, WEST_DEGREES,
                                   VARIABLE)
import re
import os
import h5py
import numpy as np
import datetime as dt
import logging

class HdfMetadataFileParser(AbstractMetadataFileParser):
    '''Currently fake implementation: all metadata is hard-wired'''
    
  
        
    def parseMetadata(self, filepath):
        
        logging.info("Parsing HDF file=%s" % filepath)
        
        metadata = {} # empty metadata dictionary
        
        # open HDF file
        h5File = h5py.File(filepath,'r')
        
        # latitudes
        lats = h5File['SoundingGeometry']['sounding_latitude'][:]
        minLat = np.min(lats)
        maxLat = np.max(lats)
        logging.debug("Latitude min=%s max=%s" % (minLat, maxLat))

        # longitudes
        lons = h5File['SoundingGeometry']['sounding_longitude'][:]
        minLon = np.min(lons)
        maxLon = np.max(lons)
        logging.debug("Longitude min=%s max=%s" % (minLon, maxLon))
        
        metadata[NORTH_DEGREES] = [maxLat]
        metadata[SOUTH_DEGREES] = [minLat]
        metadata[WEST_DEGREES] = [minLon]
        metadata[EAST_DEGREES] = [maxLon]

        # minX minY maxX maxY
        metadata[GEO] = ["%s %s %s %s" % (minLon, minLat, maxLon, maxLat)]
        
        # datetimes
        taiDateTimeStart = dt.datetime(1993, 1, 1, 0, 0, 0, tzinfo=tzutc())
        seconds = h5File['RetrievalHeader']['sounding_time_tai93'][:]
        minSecs = np.min(seconds)
        maxSecs = np.max(seconds)
        minDateTime = taiDateTimeStart + dt.timedelta(seconds=int(minSecs))
        maxDateTime = taiDateTimeStart + dt.timedelta(seconds=int(maxSecs))
        logging.debug("Datetime min=%s max=%s" % (minDateTime, maxDateTime))
        metadata[DATETIME_START] = [ minDateTime.strftime('%Y-%m-%dT%H:%M:%SZ') ]
        metadata[DATETIME_STOP] = [ maxDateTime.strftime('%Y-%m-%dT%H:%M:%SZ') ]
        
        # variables
        variables = []
        
        # loop over HDF groups
        for gname, gobj in h5File.items():
            if gobj.__class__.__name__=='Group':

                # loop over HDF datasets
                for dname, dobj in gobj.items():
                    if dobj.__class__.__name__ =='Dataset':
                        variables.append("%s/%s" % (gname, dname))
                        
        metadata[VARIABLE] = variables
        
        # close HDF file
        h5File.close()

        return metadata
