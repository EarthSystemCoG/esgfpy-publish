'''
HDF parser specific to AIRS files.

@author: Luca Cinquini
'''

from esgfpy.publish.parsers import HdfMetadataFileParser
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
    #import pyhdf
    from pyhdf.SD import SD, SDC
    #from pyhdf.V import *
    from pyhdf.HDF import HDF4Error
except ImportError:
    pass

FILENAME_PATTERN = "AIRS\.(?P<yyyy>\d+)\.(?P<mm>\d+)\.(?P<dd>\d+)\..+\.hdf"
INVALID_VALUE = -9999.

class AirsFileParser(HdfMetadataFileParser):

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
            variables = hdfFile.datasets().keys()

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
                        
            # store metadata values
            self._storeMetadata(metadata, np.asarray(lons), np.asarray(lats), np.asarray(datetimes), variables)

            # close HDF file
            hdfFile.end()

        return metadata