from esgfpy.publish.parsers.abstract_parser import AbstractMetadataFileParser
from os.path import splitext, expanduser
from xml.etree.ElementTree import fromstring
import ConfigParser
from esgfpy.publish.consts import METADATA_MAPPING_FILE
import os
import re
import logging

class XMLMetadataFileParser(AbstractMetadataFileParser):
    """
    Implementation of AbstractMetadataFileParser that parses metadata from ancillary XML files.
    :param startDirectory: optional parent directory where metadata XML files can be located
                           (new values for the same metadata key will be added to the list).
    """
    
    def __init__(self, metadata_mapping_file=METADATA_MAPPING_FILE, startDirectory=None):
        
        # read optional mappings for facet keys and values
        config = ConfigParser.RawConfigParser()
        try:
            config.read( expanduser(metadata_mapping_file) )
            self.metadataKeyMappings = dict(config.items('keys'))
            self.metadataValueMappings = dict(config.items('values'))
            
        except Exception as e:
            logging.warn("Metadata mapping file %s NOT found" % metadata_mapping_file)
            logging.warn(e)
            
        # starting directory to look for XML metadata files
        self.startDirectory = startDirectory    
        
    def parseMetadata(self, filepath):
        """Parses the XML tag <tag_name>tag_value</tag_name> into the dictionary entry tag_name=tag_value."""
        
        metadata = {} # empty metadata dictionary
        
        # look for XML metadata files associated with this file or directory
        metadataFilepaths = self._metadataFilePaths(filepath)
        
        for metadataFilepath in metadataFilepaths:
        
            if os.path.exists(metadataFilepath):
                logging.debug("XMLMetadataFileParser: parsing file=%s" % metadataFilepath)
            
                xmlFile = open(metadataFilepath, 'r')
                xml = xmlFile.read()
                rootEl = fromstring(xml)
                
                # loop over children of root element
                for childEl in rootEl:
                    key = childEl.tag
                    value = childEl.text
                    # apply optional mappings for key, value
                    if key in self.metadataKeyMappings:
                        key = self.metadataKeyMappings[key]           
                    if value in self.metadataValueMappings:
                        value = self.metadataValueMappings[value]
                    self._addMetadata(metadata, key, value)
            
                xmlFile.close()
            
        return metadata
    
    def _metadataFilePaths(self, filepath):
        '''Method to build the paths of the XML metadata files associated to the requested filepath.'''
        
        # list of XML files to parse 
        xmlFiles = []
                
        # for directories: <dirpath>/<lastsubdirename>.xml
        if os.path.isdir(filepath):
            
            # traverse the directory tree from self.startDirectory, if provided
            if self.startDirectory is None:
                startDir = filepath
            else:
                startDir = self.startDirectory
            # compare startDir='/usr/local/model' (no trailing '/')
            # to      filepath='/usr/local/model/experiment/tas/' (with trailing '/')
            if startDir.endswith('/'):
                startDir = startDir[0:-1]
            if not filepath.endswith('/'):
                filepath = filepath + '/'
            for i in list(re.finditer('/', filepath)):
                path = filepath[0:i.start()] # example: path = '/usr/local/model/experiment' (no trailing '/')
                if len(path) >= len(startDir):
                    (head, tail) = os.path.split(path)
                    xmlFiles.append( os.path.join(path, tail+".xml") )
                                 
        # for files: <filepath>+".xml
        else:
            xmlFiles.append( filepath +".xml" )
            
        return xmlFiles