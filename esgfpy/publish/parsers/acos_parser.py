'''
HDF parser specific to ACOS files.

@author: Luca Cinquini
'''
import datetime as dt
from esgfpy.publish.parsers import HdfMetadataFileParser
#from esgfpy.publish.consts import TAI93_DATETIME_START
import os
import re
from dateutil.tz import tzutc
import numpy as np

FILENAME_PATTERN_V33 = "acos_L2s_(?P<yymmdd>\d+)_\d\d_Production_.+\.h5"
FILENAME_PATTERN_V34 = "acos_L2s_(?P<yymmdd>\d+)_\d\d_Evaluation_.+\.h5"
FILENAME_LITE_PATTERN_V34R02 = "acos_b34_L2lite_(?P<yyyymmdd>\d+)_r02c.nc"
FILENAME_LITE_PATTERN_V34R03 = "acos_b34_L2lite_(?P<yyyymmdd>\d+)_r03n.nc"

class AcosFileParser(HdfMetadataFileParser):
    '''Works for: ACOSv3.3, ACOSv3.4_r01, ACOSv3.4_r02.'''
    
    def matches(self, filepath):
        '''Example filename: acos_L2s_100129_16_Evaluation_v150151_L2s30400_r01_PolB_130904152222c.h5'''
        dir, filename = os.path.split(filepath)
        return re.match(FILENAME_PATTERN_V33, filename) or re.match(FILENAME_PATTERN_V34, filename)
    
    def getLatitudes(self, h5file):
        return h5file['SoundingGeometry']['sounding_latitude'][:]

    def getLongitudes(self, h5file):
        return h5file['SoundingGeometry']['sounding_longitude'][:]
    
    def getTimes(self, h5file):
        
        # use TAI93 time
        #seconds = h5file['RetrievalHeader']['sounding_time_tai93'][:]
        #times = np.empty( len(seconds), dtype=dt.datetime)
        #for i, secs in enumerate(seconds):
        #    times[i] = TAI93_DATETIME_START + dt.timedelta(seconds=int(secs))
        #return times
        
        # use UTC time
        dateStrings = h5file['RetrievalHeader']['sounding_time_string'][:]
        datasetTimes = []
        for x in dateStrings:
            try:
                datasetTimes.append( dt.datetime.strptime(x[:19],"%Y-%m-%dT%H:%M:%S").replace(tzinfo=tzutc()) )
            except:
                pass  # bad date
        return datasetTimes


class AcosLiteFileParser_v34r02(HdfMetadataFileParser):
    
    def matches(self, filepath):
        '''Example filename: acos_b34_L2lite_20090601_r02c.nc'''
        
        dir, filename = os.path.split(filepath)
        return re.match(FILENAME_LITE_PATTERN_V34R02, filename)
    
    def getLatitudes(self, h5file):
        return h5file['Sounding']['latitude'][:]

    def getLongitudes(self, h5file):
        return h5file['Sounding']['longitude'][:]
    
    def getTimes(self, h5file):
                
        # string time:comment = "Array of seven integers containing the observation time: 
        # year, month (1-12), day (1-31), hour (0-23), minute (0-59), second (0-59), millisecond(0-999)"
        # example: [2009    6    1    0   25   47    4]
        datasetTimes = []
        times = h5file['Sounding']['time'][:]
        for time in times:
            datasetTimes.append( dt.datetime(time[0], time[1], time[2], time[3], time[4], time[5], time[6]) )
        return np.asarray( datasetTimes )

class AcosLiteFileParser_v34r03(HdfMetadataFileParser):
    
    def matches(self, filepath):
        '''Example filename: acos_b34_L2lite_20090601_r03n.nc'''
        
        dir, filename = os.path.split(filepath)
        return re.match(FILENAME_LITE_PATTERN_V34R03, filename)
    
    def getLatitudes(self, h5file):
        return h5file['latitude'][:]

    def getLongitudes(self, h5file):
        return h5file['longitude'][:]
    
    def getTimes(self, h5file):
                
        datasetTimes = []
        times = h5file['time'][:]
        for time in times:
            datasetTimes.append( dt.datetime.utcfromtimestamp(time) )
        return np.asarray( datasetTimes )

if __name__ == '__main__':
    
    #filepath = '/usr/local/co2/data/ACOS/3.4_r01/acos_L2s_100101_44_Evaluation_v150151_L2s30400_r01_PolB_130914015757c.h5'
    filepath = '/usr/local/co2/data/ACOS/3.4_r01/acos_L2s_100101_06_Evaluation_v150151_L2s30400_r01_PolB_130904132249c.h5'
    
    parser = AcosFileParser()
    parser.parseMetadata(filepath)