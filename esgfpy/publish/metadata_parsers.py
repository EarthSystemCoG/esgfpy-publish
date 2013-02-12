'''
Module for parsing records metadata from ancillary files.
'''

from os.path import splitext
from xml.etree.ElementTree import fromstring

class AbstractMetadataFileParser(object):
    """API for parsing metadata from local file system."""
    
    def isMetadataFile(self, filepath):
        """Determines whether the given file is a metadata file."""
        raise NotImplementedError
    
    def parseMetadata(self, filepath):
        """
        Parses the given file into a dictionary of metadata elements.
        :param filepath: file location on local file system
        :return: dictionary of (name, values) metadata elements
        """
        raise NotImplementedError
    
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
    
    def isMetadataFile(self, filepath):
        """Files ending in '.xml' are considered ancillary metadata files."""
        name, ext = splitext(filepath)
        if ext.lower() == ".xml":
            return True
        else:
            return False
        
    def parseMetadata(self, filepath):
        """Parses the XML tag <tag_name>tag_value</tag_name> into the dictionary entry tag_name=tag_value."""
        
        metadata = {} # empty metadata dictionary
        xml = open(filepath, 'r').read()
        rootEl = fromstring(xml)
        
        # loop over children of root element
        for childEl in rootEl:
            #print childEl.tag, childEl.attrib, childEl.text
            if not childEl.tag in metadata:
                metadata[childEl.tag] = []
            metadata[childEl.tag].append(childEl.text)
            
        return metadata