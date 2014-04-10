import string
import os

from esgfpy.publish.models import FileRecord
from esgfpy.publish.consts import (FILE_SUBTYPES, SUBTYPE_IMAGE, THUMBNAIL_EXT,
                                   CHECKSUM, CHECKSUM_TYPE, TRACKING_ID)

from esgfpy.publish.factories.utils import generateUrls
from esgfpy.publish.parsers import NetcdfMetadataFileParser, XMLMetadataFileParser, FilenameMetadataParser
import Image
from esgfpy.publish.factories.utils import generateId
from esgfpy.publish.consts import MASTER_ID
import hashlib
import logging
from uuid import uuid4

class AbstractFileRecordFactory(object):
    """API for generating ESGF records of type File."""

    def create(self, datasetRecord, uri):
        """
        Generates a FileRecord from the given resource URI and parent Dataset record.
        :param datasetRecord: parent dataset record
        :param uri: source URI for generating file record
        :return: FileRecord object
        """
        raise NotImplementedError

class FilepathFileRecordFactory(AbstractFileRecordFactory):
    """Class that generates FileRecord objects from a filepath in the local file system."""

    def __init__(self, fields={}, rootDirectory=None, filenamePatterns=[], baseUrls={},
                 generateThumbnails=False, generateChecksum=False, generateTrackingId=True):
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
        self.generateChecksum = generateChecksum
        self.generateTrackingId = generateTrackingId

        # define list of metadata parsers
        self.metadataParsers = [ FilenameMetadataParser(self.filenamePatterns),
                                 NetcdfMetadataFileParser(),
                                 XMLMetadataFileParser() ]

    def create(self, datasetRecord, filepath):

        if os.path.isfile(filepath):

            dir, filename = os.path.split(filepath)
            name, extension = os.path.splitext(filename)
            ext =  extension[1:] # remove '.' from file extension

            fields = {}
            fields['format'] = [ext]
            isImage = False
            for subtype, values in FILE_SUBTYPES.items():
                if ext in values:
                    fields['subtype'] = [subtype]
                    if subtype==SUBTYPE_IMAGE:
                        isImage=True

            # create image thumbnail ?
            if self.generateThumbnails and isImage:
                thumbnailPath = os.path.join(dir, "%s.%s" % (name, THUMBNAIL_EXT) )
                self._generateThumbnail(filepath, thumbnailPath)

            # add file access URLs
            urls = generateUrls(self.baseUrls, self.rootDirectory, filepath, isImage=isImage)
            if len(urls)>0:
                fields["url"] = urls

            # add file-level metadata from configured parsers to fixed metadata
            metadata = self.fields.copy()
            metadata = dict(metadata.items() + fields.items()) # FIXME
            for parser in self.metadataParsers:
                met = parser.parseMetadata(filepath)
                metadata = dict(metadata.items() + met.items()) # NOTE: met items override metadata items

            # build 'id', 'instance_id, 'master_id'
            # start from dataset 'master_id' since it has no version, data_node information
            identifier = string.join( [datasetRecord.fields[MASTER_ID][0], filename], '.')
            id = generateId(identifier, metadata)

            # set record title to filename - rename 'title' global attribute if found
            try:
                title = metadata['title'][0]
                metadata['file title'] = [ title ]
                del metadata['title']
            except KeyError:
                pass
            title = filename

            # create Md5 checksum ?
            if self.generateChecksum:
                logging.debug('Computing Md5 checksum for file: %s ...' % filepath)
                md5 = md5_for_file(filepath, hr=True)
                logging.debug('...Md5 checksum=%s' % md5)
                metadata[CHECKSUM] = [md5]
                metadata[CHECKSUM_TYPE] = ['MD5']

            # generate tracking ID ?
            if self.generateTrackingId:
                metadata[TRACKING_ID] = [str(uuid4())]


            return FileRecord(datasetRecord, id, title, metadata)

        else:
            raise Exception("%s is not a file" % filepath)

def md5_for_file(path, block_size=256*128, hr=False):
    '''
    Function to compute the Md5 checksum of a file.

    Block size directly depends on the block size of your filesystem to avoid performances issues.
    Here I have blocks of 4096 octets (Default NTFS).

    Acknowledgment: http://stackoverflow.com/questions/1131220/get-md5-hash-of-big-files-in-python
    '''
    md5 = hashlib.md5()
    with open(path,'rb') as f:
        for chunk in iter(lambda: f.read(block_size), b''):
            md5.update(chunk)
    if hr:
        return md5.hexdigest()
    return md5.digest()