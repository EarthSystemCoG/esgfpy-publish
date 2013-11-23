from esgfpy.publish.metadata_parsers.abstract_parser import AbstractMetadataFileParser
from os.path import splitext, expanduser
from xml.etree.ElementTree import fromstring
import ConfigParser
from esgfpy.publish.consts import METADATA_MAPPING_FILE
import os

class XMLMetadataFileParser(AbstractMetadataFileParser):
    """
    Implementation of AbstractMetadataFileParser that parses metadata from XML files
    (specifically, files ending in '.xml').
    """
    
    def __init__(self, metadata_mapping_file=METADATA_MAPPING_FILE):
        
        # read optional mappings for facet keys and values
        config = ConfigParser.RawConfigParser()
        try:
            config.read( expanduser(metadata_mapping_file) )
            self.metadataKeyMappings = dict(config.items('keys'))
            self.metadataValueMappings = dict(config.items('values'))
            
        except Exception as e:
            print "Metadata mapping file %s NOT found" % metadata_mapping_file
            print e
    
    def isMetadataFile(self, filepath):
        """Files ending in '.xml' are considered ancillary metadata files."""
        
        print 'Checking filepath=%s' % filepath
        return os.path.exists(filepath)
        '''
        if os.path.isdir(filepath):
            print 'its a directory'
            return True
        elif os.path.isfile(filepath):
            print 'its a file'
            return True
        else:
            return False
        '''
        
        '''
        name, ext = splitext(filepath)
        if ext.lower() == ".xml":
            return True
        else:
            return False
        '''
        
    def parseMetadata(self, filepath):
        """Parses the XML tag <tag_name>tag_value</tag_name> into the dictionary entry tag_name=tag_value."""
        
        metadata = {} # empty metadata dictionary
        
        metadataFilepath = self._metadataFilePath(filepath)
        print 'metadataFilepath=%s' % metadataFilepath
        
        if os.path.exists(metadataFilepath):
            print 'XMLMetadataFileParser: parsing file=%s' % metadataFilepath
        
            xml = open(metadataFilepath, 'r').read()
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
            
        return metadata
    
    def _metadataFilePath(self, filepath):
        '''Method to build the path of the XML metadata file associated to the requested filepath.'''
        if os.path.isdir(filepath):
            if filepath.endswith('/'):
                filepath = filepath[0:-1] # must remove trailing slash before splitting path
            (head, tail) = os.path.split(filepath)
            return os.path.join(filepath, tail+".xml")
        else:
            return filepath +".xml"