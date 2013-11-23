import string
import os
import re

from esgfpy.publish.models import FileRecord
from esgfpy.publish.consts import FILE_SUBTYPES, SUBTYPE_IMAGE, THUMBNAIL_WIDTH, THUMBNAIL_HEIGHT, THUMBNAIL_EXT, SERVICE_THUMBNAIL, SERVICE_OPENDAP
from esgfpy.publish.utils import getMimeType
from esgfpy.publish.parsers import NetcdfMetadataFileParser, XMLMetadataFileParser
import Image


    
        
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
    
    def __init__(self, fields={}, rootDirectory=None, filenamePatterns=[], baseUrls={}, 
                 metadataParsers = [ NetcdfMetadataFileParser(), XMLMetadataFileParser() ],
                 generateThumbnails=False):
        """
        :param fields: constants metadata fields as (key, values) pairs
        :param rootDirectory: root directory of file location, will be removed when creating the file access URL
        :param filenamePatterns: optional list of matching filename patterns (with named groups)
        :param baseUrls: map of (base server URL, server name) to create file access URLs
        :param metadataParsers: list of metadata parsers to generate or extract file-level metadata
        :param generateThumbnails: set to True to automatically generate thumbnails when publishing image files
        """

        self.fields = fields
        self.rootDirectory = rootDirectory
        self.filenamePatterns = filenamePatterns
        self.metadataParsers = metadataParsers
        self.baseUrls = baseUrls
        self.generateThumbnails = generateThumbnails
            
    def create(self, datasetRecord, filepath, metadata={}):
        
        if os.path.isfile(filepath):
            
            dir, filename = os.path.split(filepath)
            name, extension = os.path.splitext(filename)
            ext =  extension[1:] # remove '.' from file extension
            id = string.join( [datasetRecord.id, filename], '.')

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
                elif serverName == SERVICE_OPENDAP:
                    url = "%s.html" % url # must add ".html" extension to OpenDAP URLs
                    urls.append( "%s|%s|%s" % ( url, getMimeType(ext), serverName) )
                else:                   
                    urls.append( "%s|%s|%s" % ( url, getMimeType(ext), serverName) )
                    
            if len(urls)>0:
                fields["url"] = urls
                
            # extract information from file names
            match = False
            for pattern in self.filenamePatterns:
                match = re.match(pattern, filename)
                if match:
                    #print '\tFilename: %s matches template: %s' % (filename, pattern)
                    for key in match.groupdict().keys():
                        #print 'File Metadata: key=%s value=%s' % (key,  match.group(key))
                        fields[key] = [ match.group(key) ]
                    break # no more matching
            if not match:
                print '\tNo matching pattern found for filename: %s' % filename
               
            # add file-level metadata from configured parsers
            met = {}
            for parser in self.metadataParsers:
                newmet = parser.parseMetadata(filepath)
                met = dict(met.items() + newmet.items()) # NOTE: newmet items override met items
                    
            # add constant metadata fields + instance metadata fields
            for (key, values) in (self.fields.items() + metadata.items() + met.items()):
                fields[key] = values
                
            # set record title to filename, unless already set by file-specific metadata
            try:
                title = fields['title'][0]
                del fields['title']
            except KeyError:
                title = filename

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
