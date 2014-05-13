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

class HdfMetadataFileParser(AbstractMetadataFileParser):
    '''Currently fake implementation: all metadata is hard-wired'''
        
    def parseMetadata(self, filepath):
        
        metadata = {} # empty metadata dictionary
        
        # FIXME: remove when proper metadata is read from HDF files
        if "acos" in filepath:
            
            # obtain filename from filepath
            dir, filename = os.path.split(filepath)
            pattern = "acos_L2s_(?P<yymmdd>\d+)_\d\d_Evaluation_.+\.h5"
            match = re.match(pattern, filename)
            if match:
                yymmdd = match.group('yymmdd')
                yyyy = 2000 + int(yymmdd[0:2])
                mm = int(yymmdd[2:4])
                dd = int(yymmdd[4:6])
                startDate = dt.datetime(yyyy, mm, dd, tzinfo=tzutc())
            
            # N-W quadrant
            minLon = -60
            maxLon = -30
            minLat = 10
            maxLat = 30
            
        elif "AIRS" in filepath:
            
            startDate = dt.datetime(2001, 1, 1, tzinfo=tzutc())
            
            # N-E quadrant
            minLon = 30
            maxLon = 60
            minLat = 10
            maxLat = 30
            
        elif "TES" in filepath:
            
            startDate = dt.datetime(2002, 1, 1, tzinfo=tzutc())
            
            # S-E quadrant
            minLon = 30
            maxLon = 60
            minLat = -30
            maxLat = -10

        elif "OCO-2" in filepath:
            
            startDate = dt.datetime(2003, 1, 1, tzinfo=tzutc())
            
            # S-W quadrant
            minLon = -60
            maxLon = -30
            minLat = -30
            maxLat = -10
            
        stopDate = startDate + dt.timedelta(days=1)
        metadata[DATETIME_START] = [ startDate.strftime('%Y-%m-%dT%H:%M:%SZ') ]
        metadata[DATETIME_STOP] = [ stopDate.strftime('%Y-%m-%dT%H:%M:%SZ') ]
                
        metadata[NORTH_DEGREES] = [maxLat]
        metadata[SOUTH_DEGREES] = [minLat]
        metadata[WEST_DEGREES] = [minLon]
        metadata[EAST_DEGREES] = [maxLon]
            
        # minX minY maxX maxY
        metadata[GEO] = ["%s %s %s %s" % (minLon, minLat, maxLon, maxLat)]
        
        metadata[VARIABLE] = ["xco2", "wind_speed"]

        return metadata
