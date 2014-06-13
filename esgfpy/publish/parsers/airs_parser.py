'''
HDF parser specific to AIRS files.

@author: Luca Cinquini
'''

from esgfpy.publish.parsers.abstract_parser import AbstractMetadataFileParser
from esgfpy.publish.consts import (DATETIME_START, DATETIME_STOP, GEO,
                                   NORTH_DEGREES, SOUTH_DEGREES, EAST_DEGREES, WEST_DEGREES,
                                   VARIABLE)
import os
import re
import logging
import datetime as dt
from dateutil.tz import tzutc
import numpy as np

try:
    import pyhdf
    from pyhdf.SD import SD, SDC
    from pyhdf.V import *
    from pyhdf.HDF import *
except ImportError:
    pass

FILENAME_PATTERN = "AIRS\.(?P<yyyy>\d+)\.(?P<mm>\d+)\.(?P<dd>\d+)\..+\.hdf"
INVALID_VALUE = -9999.

class AirsFileParser(AbstractMetadataFileParser):

    def parseMetadata(self, filepath):

        metadata = {}
    
        dir, filename = os.path.split(filepath)
        if re.match(FILENAME_PATTERN, filename):
            logging.info("Parsing HDF file=%s" % filepath)

            # open HDF file
            try:
                hdfFile = SD(filepath, SDC.READ)
            except HDF4Error as e:
                logging.info(e)
                raise e

            # variables
            metadata[VARIABLE] = hdfFile.datasets().keys()

            # time fields
            year = hdfFile.select('Year')[:]
            month = hdfFile.select('Month')[:]
            day = hdfFile.select('Day')[:]
            hour = hdfFile.select('Hour')[:]
            minute = hdfFile.select('Minute')[:]
            second = hdfFile.select('Seconds')[:]

            # space fields
            lon = hdfFile.select('Longitude')[:]
            lat = hdfFile.select('Latitude')[:]

            datetimes = []
            lats = []
            lons = []
            for t in range(22):
                for x in range(15):
                    if year[t,x] != -9999:

                        datetimes.append( dt.datetime(year[t,x],month[t,x],day[t,x],hour[t,x],minute[t,x],second[t,x], tzinfo=tzutc()) )
                        lons.append( lon[t,x] )
                        lats.append( lat[t,x] )

            # store metadata
            datetimes = np.asarray(datetimes)
            minDateTime = np.min( datetimes )
            maxDateTime = np.max( datetimes )
            logging.debug("Datetime min=%s max=%s" % (minDateTime, maxDateTime))
            metadata[DATETIME_START] = [ minDateTime.strftime('%Y-%m-%dT%H:%M:%SZ') ]
            metadata[DATETIME_STOP] = [ maxDateTime.strftime('%Y-%m-%dT%H:%M:%SZ') ]

            minLat = INVALID_VALUE
            maxLat = INVALID_VALUE
            lats = np.asarray(lats)
            lats = lats[ np.where( lats >= -90) ] # exclude missing values
            lats = lats[ np.where( lats <=  90 )]
            if len(lats)>0:
                minLat = np.min(lats)
                maxLat = np.max(lats)
                logging.debug("Latitude min=%s max=%s" % (minLat, maxLat))

            minLon = INVALID_VALUE
            maxLon = INVALID_VALUE
            lons = np.asarray(lons)
            lons = lons[ np.where( lons >= -180) ] # exclude missing values
            lons = lons[ np.where( lons <=  360) ]
            if len(lons)>0:
                if np.max(lons) > 180: # shift longitudes ?
                    lons = lons - 360
                minLon = np.min(lons)
                maxLon = np.max(lons)
                logging.debug("Longitude min=%s max=%s" % (minLon, maxLon))

            # store geographic bounds
            if minLon >= -180 and maxLon<=180 and minLat>=-90 and maxLat<=90:
                metadata[NORTH_DEGREES] = [maxLat]
                metadata[SOUTH_DEGREES] = [minLat]
                metadata[WEST_DEGREES] = [minLon]
                metadata[EAST_DEGREES] = [maxLon]
                # minX minY maxX maxY
                metadata[GEO] = ["%s %s %s %s" % (minLon, minLat, maxLon, maxLat)]

            # clode HDF file
            hdfFile.end()


        return metadata




