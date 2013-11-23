'''
Module for parsing records metadata from ancillary files.
'''

from os.path import splitext, expanduser
from xml.etree.ElementTree import fromstring
from esgfpy.publish.consts import METADATA_MAPPING_FILE
import ConfigParser
import abc
from netCDF4 import Dataset
import os

class AbstractMetadataFileParser(object):
    """API for parsing metadata from files."""
    
    __metaclass__ = abc.ABCMeta
        
    @abc.abstractmethod
    def isMetadataFile(self, filepath):
        """Determines whether the given file is a metadata file."""
        raise NotImplementedError
    @abc.abstractmethod
    def parseMetadata(self, filepath):
        """
        Parses the given file into a dictionary of metadata elements.
        :param filepath: file location on local file system
        :return: dictionary of (name, values) metadata elements
        """
        raise NotImplementedError
    
    def _addMetadata(self, metadata, key, value):
        '''Method to append a new metadata value for a given key.'''
        
        if not key in metadata:
            metadata[key] = [] # initialize empty list
        metadata[key].append(value)
    
class OptOutMetadataFileParser(AbstractMetadataFileParser):
    """
    Implementation of AbstractMetadataFileParser that does nothing
    (i.e. doesn't parse metadata from any file).
    """
    
    def isMetadataFile(self, filepath):
        """Implementation that ignores all files."""
        return False
    
    def parseMetadata(self, filepath):
        """Returns an empty dictionary."""
        return {}
    
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
    
class NetcdfMetadataFileParser(AbstractMetadataFileParser):
    '''Parses metadata from NetCDF files.'''
    
    def isMetadataFile(self, filepath):
        return filepath.lower().endswith(".nc")
    
    def parseMetadata(self, filepath):
        print 'NetcdfMetadataFileParser: parsing filepath=%s' % filepath
        
        metadata = {} # empty metadata dictionary
    
        try:
            # open file
            nc = Dataset(filepath, 'r')
            
            # loop over global attributes
            for attname in nc.ncattrs():
                self._addMetadata(metadata, attname, getattr(nc, attname) )
            
            # loop over variable attributes
            for key, variable in nc.variables.items():
                if key.lower()!='longitude' and key.lower()!='latitude' and key.lower()!='altitude' and key.lower()!='time' and key.lower()!='level':
                    # IMPORTANT: the variable arrays must have the same number of entries
                    self._addMetadata(metadata, 'variable', key)
                    self._addMetadata(metadata, 'variable_long_name', getattr(variable, 'long_name', None) )
                    self._addMetadata(metadata, 'cf_standard_name', getattr(variable, 'stanadard_name', None) )
                    self._addMetadata(metadata, 'units', getattr(variable, 'units', None) )

        except Exception as e:
            print e
        finally:
            nc.close()
            
        return metadata