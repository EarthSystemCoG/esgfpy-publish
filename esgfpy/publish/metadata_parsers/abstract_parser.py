import abc

class AbstractMetadataFileParser(object):
    """API for parsing metadata from files and directories."""
    
    __metaclass__ = abc.ABCMeta
        
    @abc.abstractmethod
    def parseMetadata(self, filepath):
        """
        Parses the given file into a dictionary of metadata elements.
        :param filepath: file location on local file system
        :return: dictionary of (name, values) metadata elements
        """
        raise NotImplementedError
    
    def _addMetadata(self, metadata, key, value):
        '''Utility method to append a new metadata value for a given key.'''
        
        if not key in metadata:
            metadata[key] = [] # initialize empty list
        metadata[key].append(value)