from esgfpy.publish.metadata_parsers.abstract_parser import AbstractMetadataFileParser

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