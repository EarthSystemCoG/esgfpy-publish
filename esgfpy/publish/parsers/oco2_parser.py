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

# oco2*L2Std*.h5
FILENAME_PATTERN_L1 = "oco2_L1b.+\.h5"    # oco2_L1bScGL_89234a_100924_B3500_140205015904n.h5
FILENAME_PATTERN_L2 = "oco2.+L2Std.+\.h5" # oco2_L2StdGL_89234a_100924_B3500_140205185958n.h5

class Oco2L1FileParser(HdfMetadataFileParser):
    
    def matches(self, filepath):
        ''' Example filename: oco2_L1bScGL_89234a_100924_B3500_140205015904n.h5 '''
        
        dir, filename = os.path.split(filepath)
        return re.match(FILENAME_PATTERN_L1, filename)
    
    def getLatitudes(self, h5file):
        return h5file['SoundingGeometry']['sounding_latitude'][:]

    def getLongitudes(self, h5file):
        return h5file['SoundingGeometry']['sounding_longitude'][:]
    
    def getTimes(self, h5file):
        
                #seconds = h5file['RetrievalHeader']['sounding_time_tai93'][:]
        #times = np.empty( len(seconds), dtype=dt.datetime)
        #for i, secs in enumerate(seconds):
        #    times[i] = TAI93_DATETIME_START + dt.timedelta(seconds=int(secs))
        #return times

        
        # use TAI93 times because 'sounding_time_string' causes segmentation fault
        datasetTimes = []
        tai93Times = h5file['SoundingGeometry']['sounding_time_tai93'][:]
        for timeArray in tai93Times:
            for secs in timeArray:
                try:
                    dateTime = TAI93_DATETIME_START + dt.timedelta(seconds=int(secs))
                    datasetTimes.append( dateTime.replace(tzinfo=tzutc()) )
                except:
                    pass # ignore one bad values
        return datasetTimes
    
class Oco2L2FileParser(HdfMetadataFileParser):
    
    def matches(self, filepath):
        '''Example filename: oco2_L2StdGL_89234a_100924_B3500_140205185958n.h5'''
        
        dir, filename = os.path.split(filepath)
        return re.match(FILENAME_PATTERN_L2, filename)
    
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