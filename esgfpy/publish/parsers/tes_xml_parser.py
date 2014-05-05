'''
Metadata parser specific to TES XML schema.
'''

from esgfpy.publish.parsers.abstract_parser import AbstractMetadataFileParser
import logging
import os
import datetime as dt
from xml.etree.ElementTree import fromstring
from esgfpy.publish.consts import (CHECKSUM, CHECKSUM_TYPE)

class TesXmlMetadataFileParser(AbstractMetadataFileParser):
    
    def parseMetadata(self, filepath):
        """Parses the content of the <GranuleMetaDataFile> element into the file metadata dictionary."""
        
        metadata = {} # empty metadata dictionary
        
        if "TES" in filepath:
            xmlpath = "%s.xml" % filepath
            if os.path.exists(xmlpath):
                logging.debug("TesXmlMetadataFileParser: parsing file=%s" % xmlpath)
                    
                xmlFile = open(xmlpath, 'r')
                xml = xmlFile.read()
                rootEl = fromstring(xml)
                
                # loop over children of root element
                '''
                for childEl in rootEl:
                    key = childEl.tag
                    value = childEl.text
                    # apply optional mappings for key, value
                    if key in self.metadataKeyMappings:
                        key = self.metadataKeyMappings[key]           
                    if value in self.metadataValueMappings:
                        value = self.metadataValueMappings[value]
                    self._addMetadata(metadata, key, value)
                '''
            
                xmlFile.close()
                
                # FIXME
                #<RangeDateTime>
                #    <RangeEndingTime>15:27:04.746093</RangeEndingTime>
                #    <RangeEndingDate>2012-09-17</RangeEndingDate>
                #    <RangeBeginningTime>09:09:48.934692</RangeBeginningTime>
                #    <RangeBeginningDate>2012-09-17</RangeBeginningDate>
                # </RangeDateTime>
                startDate = dt.datetime.strptime("2012-09-17 09:09:48.934692", "%Y-%m-%d %H:%M:%S.%f")
                stopDate = dt.datetime.strptime("2012-09-17 15:27:04.746093", "%Y-%m-%d %H:%M:%S.%f")
                metadata["datetime_start"] = [ startDate.strftime('%Y-%m-%dT%H:%M:%SZ') ]
                metadata["datetime_stop"] = [ stopDate.strftime('%Y-%m-%dT%H:%M:%SZ') ]

                # <WestBoundingCoordinate>-180.0</WestBoundingCoordinate>
                # <NorthBoundingCoordinate>90.0</NorthBoundingCoordinate>
                # <EastBoundingCoordinate>180.0</EastBoundingCoordinate>
                # <SouthBoundingCoordinate>-90.0</SouthBoundingCoordinate>                    
                metadata["north_degrees"] = [90]
                metadata["south_degrees"] = [-90]
                metadata["west_degrees"] = [-180]
                metadata["east_degrees"] = [180]
                
                # <ChecksumType>MD5</ChecksumType>
                # <Checksum>a7fc67ad798c9a4ea61eda96f3545d6b</Checksum>
                metadata[CHECKSUM] = ['a7fc67ad798c9a4ea61eda96f3545d6b']
                metadata[CHECKSUM_TYPE] = ['MD5']
            
        return metadata
