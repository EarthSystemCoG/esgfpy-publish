'''
Module containing factories for creating Record objects from different sources.

The abstract classes :class:`AbstractDatasetRecordFactory` and :class:`AbstractFileRecordFactory`
define the API for generating records of type Dataset and File, respectively.

Their subclasses provide implementation for different sources (remote metadata catalog. local file system, database etc.).

@author: Luca Cinquini
'''

'''
Module :mod:`pyesgf.publish.factories`
=====================================

Module containing factories for creating Record objects.

'''

import string
import os

from .models import DatasetRecord, FileRecord
from .consts import FILE_SUBTYPES, SUBTYPE_IMAGE, THUMBNAIL_WIDTH, THUMBNAIL_HEIGHT
from .utils import getMimeType
import Image

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
    
    def __init__(self, rootId, rootDirectory="/", subDirs=[], fields={}):
        """
        :param rootId: root of assigned dataset identifiers
        :param rootDirectory: root filepath removed before parsing for subdirectories
        :param subDirs: list of named subdirectories above the file locations.
                        Files must be located in a directory structure at least as deep
                        as the number of given subdirectories. 
                        Subdirectories above the structure will be ignored as far as assigning facet values.
        :param fields: constants metadata fields as (key, values) pairs
        """
        self.rootId = rootId
        self.rootDirectory = rootDirectory
        self.subDirs = subDirs
        self.fields = fields
       
    def create(self, directory, metadata={}):
        """
        Generates a single dataset from the given directory,
        provided it conforms to the specified template: rootDirectory + subDirs
        (otherwise None is returned).
        """   
        
        if os.path.isdir(directory):
            
            if self.rootDirectory in directory:
        
                # remove rootDirectory, then split remaining path into subdirectories
                directory = directory.replace(self.rootDirectory,"")
                if directory.startswith("/"):
                    directory = directory[1:]
                parts = directory.split(os.sep)
                if len(parts) == len(self.subDirs):    
                    print 'Generating Dataset record for directory: %s'  % directory         
                    # loop over sub-directories bottom-to-top
                    subValues = []
                    fields = {}
                    title = ""
                    for i, part in enumerate(parts):                       
                        subValues.append(part)
                        subDir = self.subDirs[i] 
                        fields[subDir] = [part] # list of one element
                        title += "%s=%s, " % (string.capitalize(subDir), part)
                    
                    id = string.join(subValues,'.')
                                     
                    # add constant metadata fields + instance metadata fields
                    for (key, values) in (self.fields.items() + metadata.items()):
                        fields[key] = values
                        
                    # create and return one Dataset record
                    return DatasetRecord(id, title[:-2], fields)
            
        # no Dataset record created
        return None
    
        
class AbstractFileRecordFactory(object):
    """API for generating ESGF records of type File."""
    
    def create(self, datasetRecord, uri, metadata={}):
        """
        Generates a FileRecord from the given resource URI and parent Dataset record.
        :param datasetRecord: parent dataset record
        :param uri: source URI for generating file record
        :param metadata: optional additional dictionary of metadata (name, values) pairs to be added to the file record instance
        :return: FileRecord object
        """
        raise NotImplementedError
    
class FilepathFileRecordFactory(AbstractFileRecordFactory):
    """Class that generates FileRecord objects from a filepath in the local file system."""
    
    def __init__(self, fields={}, rootDirectory=None, baseUrls={}, generateThumbnails=False):
        """
        :param fields: constants metadata fields as (key, values) pairs
        :param rootDirectory: root directory of file location, will be removed when creating the file access URL
        :param baseUrls: map of (base server URL, server name) to create file access URLs
        :param generateThumbnails: set to True to automatically generate thumbnails when publishing image files
        """

        self.fields = fields
        self.rootDirectory = rootDirectory
        self.baseUrls = baseUrls
        self.generateThumbnails = generateThumbnails
    
    def create(self, datasetRecord, filepath, metadata={}):
        
        if os.path.isfile(filepath):
            
            dir, filename = os.path.split(filepath)
            name, extension = os.path.splitext(filepath)
            ext =  extension[1:] # remove '.' from file extension
            id = string.join( [datasetRecord.id, filename], '.')
            title = filename
            fields = {}
            fields['format'] = [ext]
            for subtype, values in FILE_SUBTYPES.items():
                if ext in values:
                    fields['subtype'] = [subtype]
                    
            # create image thumbnail
            if self.generateThumbnails:
                if 'subtype' in fields.keys() and SUBTYPE_IMAGE in fields['subtype']:
                    thumbnailPath = os.path.join(dir, "%s.thumbnail.jpg" % name)
                    if not os.path.exists(thumbnailPath) or os.path.getsize(thumbnailPath)==0:
                        print '\nGenerating thumbnail: %s' % thumbnailPath
                        try:
                            im = Image.open(filepath)
                            if im.mode != "RGB":
                                im = im.convert("RGB")
                            size = THUMBNAIL_WIDTH, THUMBNAIL_HEIGHT
                            im.thumbnail(size)
                            im.save(thumbnailPath, "JPEG")
                        except IOError as error:
                            print "Cannot create thumbnail for", filepath
                            print error
                 
                    
            # add file access URLs
            relativeUrl = filepath
            if self.rootDirectory is not None:
                relativeUrl = relativeUrl.replace(self.rootDirectory,"")
            urls = []
            for serverBaseUrl, serverName in self.baseUrls.items():
                url = string.strip(serverBaseUrl,('/'))+relativeUrl
                urls.append( "%s|%s|%s" % ( url, getMimeType(ext), serverName) )
            if len(urls)>0:
                fields["url"] = urls
                
            # add constant metadata fields + instance metadata fields
            for (key, values) in (self.fields.items() + metadata.items()):
                fields[key] = values

                    
            return FileRecord(datasetRecord, id, title, fields)
            
        else:
            raise Exception("%s is not a file" % filepath)  