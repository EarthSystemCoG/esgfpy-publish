from esgfpy.publish.parsers.abstract_parser import AbstractMetadataFileParser
import re
import os
import logging

class FilenameMetadataParser(AbstractMetadataFileParser):
    '''Parses a filename into metadata fields.'''
    
    def __init__(self, filenamePatterns):
        self.filenamePatterns = filenamePatterns
                
    def parseMetadata(self, filepath):
        logging.debug('FilenameMetadataParser: parsing filename=%s' % filepath)
        
        metadata = {} # empty metadata dictionary
        
        # obtain filename from filepath
        dir, filename = os.path.split(filepath)
        
        match = False
        for pattern in self.filenamePatterns:
            match = re.match(pattern, filename)
            if match:
                #print '\tFilename: %s matches template: %s' % (filename, pattern)
                for key in match.groupdict().keys():
                    #print 'File Metadata: key=%s value=%s' % (key,  match.group(key))
                    metadata[key] = [ match.group(key) ]
                break # no more matching
        if not match:
            logging.warn('\tNo matching pattern found for filename: %s' % filename)
            
        return metadata