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
import re

from .models import DatasetRecord, FileRecord
from .consts import FILE_SUBTYPES, SUBTYPE_IMAGE, THUMBNAIL_WIDTH, THUMBNAIL_HEIGHT, THUMBNAIL_EXT, SERVICE_THUMBNAIL
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
        :param subDirs: list of one or more directory templates.
                        Datasets and files will be published only if they are stored in a directory that matches one of the templates.
        :param fields: constants metadata fields as (key, values) pairs
        """
        self.rootId = rootId
        self.rootDirectory = rootDirectory
        self.subDirs = subDirs
        self.fields = fields
       
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
                                         
                        # add constant metadata fields + instance metadata fields
                        for (key, values) in (self.fields.items() + metadata.items()):
                            fields[key] = values
                            
                        # create and return one Dataset record
                        return DatasetRecord(id, title[:-2], fields)
 
                            
        # no Dataset record created - return None
        print "Directory %s does NOT match any sub-directory template" % directory
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
    
    def __init__(self, fields={}, rootDirectory=None, filenamePatterns=[], baseUrls={}, generateThumbnails=False):
        """
        :param fields: constants metadata fields as (key, values) pairs
        :param rootDirectory: root directory of file location, will be removed when creating the file access URL
        :param filenamePatterns: optional list of matching filename patterns (with named groups)
        :param baseUrls: map of (base server URL, server name) to create file access URLs
        :param generateThumbnails: set to True to automatically generate thumbnails when publishing image files
        """

        self.fields = fields
        self.rootDirectory = rootDirectory
        self.filenamePatterns = filenamePatterns
        self.baseUrls = baseUrls
        self.generateThumbnails = generateThumbnails
    
    def create(self, datasetRecord, filepath, metadata={}):
        
        if os.path.isfile(filepath):
            
            dir, filename = os.path.split(filepath)
            name, extension = os.path.splitext(filename)
            ext =  extension[1:] # remove '.' from file extension
            id = string.join( [datasetRecord.id, filename], '.')
            # set record title to filename, unless overridden by file-specific metadata
            try:
                title = metadata['title'][0]
                del metadata['title']
            except KeyError:
                title = filename
            fields = {}
            fields['format'] = [ext]
            isImage = False
            for subtype, values in FILE_SUBTYPES.items():
                if ext in values:
                    fields['subtype'] = [subtype]
                    if subtype==SUBTYPE_IMAGE:
                        isImage=True
                    
            # create image thumbnail
            if self.generateThumbnails and isImage:
                thumbnailPath = os.path.join(dir, "%s.%s" % (name, THUMBNAIL_EXT) )
                self._generateThumbnail(filepath, thumbnailPath)                 
                    
            # add file access URLs
            relativeUrl = filepath
            if self.rootDirectory is not None:
                relativeUrl = relativeUrl.replace(self.rootDirectory,"")
            urls = []
            for serverName, serverBaseUrl in self.baseUrls.items():
                url = string.strip(serverBaseUrl,('/'))+relativeUrl
                if serverName == SERVICE_THUMBNAIL:
                    if isImage:
                        url = url.replace(ext, THUMBNAIL_EXT)
                        urls.append( "%s|%s|%s" % ( url, getMimeType("jpeg"), serverName) )
                else:                   
                    urls.append( "%s|%s|%s" % ( url, getMimeType(ext), serverName) )
                    
            if len(urls)>0:
                fields["url"] = urls
                
            # extract information from file names
            for pattern in self.filenamePatterns:
                match = re.match(pattern, filename)
                if match:
                    #print 'Filename:%s matches template' % filename
                    for key in match.groupdict().keys():
                        #print 'File Metadata: key=%s value=%s' % (key,  match.group(key))
                        fields[key] = [ match.group(key) ]
                
            # add constant metadata fields + instance metadata fields
            for (key, values) in (self.fields.items() + metadata.items()):
                fields[key] = values

            return FileRecord(datasetRecord, id, title, fields)
            
        else:
            raise Exception("%s is not a file" % filepath)  
        
    def _generateThumbnail(self, filePath, thumbnailPath):
        
        if not os.path.exists(thumbnailPath) or os.path.getsize(thumbnailPath)==0:
            print '\nGenerating thumbnail: %s' % thumbnailPath
            try:
                im = Image.open(filePath)
                if im.mode != "RGB":
                    im = im.convert("RGB")
                size = THUMBNAIL_WIDTH, THUMBNAIL_HEIGHT
                im.thumbnail(size)
                im.save(thumbnailPath, "JPEG")
            except IOError as error:
                print "Cannot create thumbnail for", filepath
                print error
