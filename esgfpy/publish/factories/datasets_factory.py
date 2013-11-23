import string
import os
import re

from esgfpy.publish.models import DatasetRecord
from esgfpy.publish.parsers import XMLMetadataFileParser


class AbstractDatasetRecordFactory(object):
    """API for generating ESGF records of type Dataset."""
    
    def create(self, uri, metadata={}):
        """
        Generates a DatasetRecord from the given resource URI.
        :param uri: string representing the metadata source (a local file system path, a remote URL, a database connection, etc.)
        :param metadata: optional additional dictionary of metadata (name, values) pairs to be added to the dataset record instance
        :return: DatasetRecord object
        """
        raise NotImplementedError
    
class DirectoryDatasetRecordFactory(AbstractDatasetRecordFactory):
    """
    Class that generates DatasetRecord objects 
    from a structured directory tree.
    """
    
    def __init__(self, rootId, rootDirectory="/", subDirs=[], fields={}, 
                 metadataParsers = [ XMLMetadataFileParser() ],
                 metadataMapper=None):
        """
        :param rootId: root of assigned dataset identifiers
        :param rootDirectory: root filepath removed before parsing for subdirectories
        :param subDirs: list of one or more directory templates.
                        Datasets and files will be published only if they are stored in a directory that matches one of the templates.
        :param fields: constants metadata fields as (key, values) pairs
        :param metadataMapper: optional class to map metadata values to controlled vocabulary
        """
        self.rootId = rootId
        self.rootDirectory = rootDirectory
        self.subDirs = subDirs
        self.fields = fields
        self.metadataParsers = metadataParsers
        self.metadataMapper = metadataMapper
       
    def create(self, directory, metadata={}):
        """
        Generates a single dataset from the given directory,
        provided it conforms to one of the specified templates: rootDirectory + subDirs
        (otherwise None is returned).
        """   
        
        if os.path.isdir(directory):
             
            if self.rootDirectory in directory:    
        
                # remove rootDirectory, then split remaining path into subdirectories
                directory = directory.replace(self.rootDirectory,"")
                if directory.startswith("/"):
                    directory = directory[1:]
                parts = directory.split(os.sep)
                for subDirs in self.subDirs:
                    if len(parts) == len(subDirs):      
                        print 'Parsing directory: %s'  % directory  
                        # loop over sub-directories bottom-to-top
                        subValues = []
                        fields = {}
                        title = ""
                        for i, part in enumerate(parts):                       
                            subValues.append(part)
                            subDir = subDirs[i] 
                            fields[subDir] = [part] # list of one element
                            title += "%s=%s, " % (string.capitalize(subDir), part)
                        
                        id = "%s.%s" % (self.rootId, string.join(subValues,'.'))
                        
                                       
                        # add dataset-level metadata from configured parsers
                        met = {}
                        dirpath = os.path.join(self.rootDirectory, directory)
                        for parser in self.metadataParsers:
                            newmet = parser.parseMetadata(dirpath)
                            met = dict(met.items() + newmet.items()) # NOTE: newmet items override met items

                                         
                        # add constant metadata fields + instance metadata fields
                        for (key, values) in (self.fields.items() + metadata.items() + met.items()):
                            fields[key] = values
                                                            
                        # optional mapping of metadata values        
                        if self.metadataMapper is not None:
                            for key, values in fields.items():
                                for i, value in enumerate(values):
                                    values[i] = self.metadataMapper.mappit(key, value)
                          
                        # create and return one Dataset record
                        return DatasetRecord(id, title[:-2], fields)
 
                            
        # no Dataset record created - return None
        print "Directory %s does NOT match any sub-directory template" % directory
        return None