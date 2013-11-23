import abc

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