from esgfpy.publish.parsers.abstract_parser import AbstractMetadataFileParser
from collections import OrderedDict
import re
import os
import logging

class DirectoryMetadataParser(AbstractMetadataFileParser):
    '''Parses a directory structure into metadata fields.'''
    
    def __init__(self, rootDirectory, subDirs):
        self.rootDirectory = rootDirectory
        self.subDirs = subDirs
                
    def parseMetadata(self, filepath):
        logging.debug('DirectoryMetadataParser: parsing directory=%s' % filepath)
        
        metadata = {}
        
        # remove rootDirectory, then split remaining path into subdirectories
        directory = filepath.replace(self.rootDirectory,"")
        if directory.startswith("/"):
            directory = directory[1:]
        parts = directory.split(os.sep)
        for subDirs in self.subDirs:
            if len(parts) == len(subDirs):      
                print 'Parsing directory: %s'  % directory  
                # loop over sub-directories bottom-to-top
                for i, part in enumerate(parts):                       
                    subDir = subDirs[i] 
                    metadata[subDir] = [part] # list of one element
                    
        return metadata
