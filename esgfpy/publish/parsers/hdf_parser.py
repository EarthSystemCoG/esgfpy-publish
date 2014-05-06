'''
Parses metadata from HDF files.
'''

from esgfpy.publish.parsers.abstract_parser import AbstractMetadataFileParser
import datetime as dt
from dateutil.tz import tzutc

class HdfMetadataFileParser(AbstractMetadataFileParser):
    '''Currently fake implementation: all metadata is hard-wired'''
        
    def parseMetadata(self, filepath):
        
        metadata = {} # empty metadata dictionary
        
        # FIXME: remove when proper metadata is read from HDF files
        if "acos" in filepath:
            
            startDate = dt.datetime(2000, 1, 1, tzinfo=tzutc())
            
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
        metadata["datetime_start"] = [ startDate.strftime('%Y-%m-%dT%H:%M:%SZ') ]
        metadata["datetime_stop"] = [ stopDate.strftime('%Y-%m-%dT%H:%M:%SZ') ]
                
        metadata["north_degrees"] = [maxLat]
        metadata["south_degrees"] = [minLat]
        metadata["west_degrees"] = [minLon]
        metadata["east_degrees"] = [maxLon]
            
        # minX minY maxX maxY
        metadata["geo"] = ["%s %s %s %s" % (minLon, minLat, maxLon, maxLat)]

        return metadata
