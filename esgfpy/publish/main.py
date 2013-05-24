'''
Module :mod:`pyesgf.publish.main`
=================================

Example driver program for publishing/unpublishing records to ESGF.

The static configuration parameters are read from CONFIG_FILE.
Example configuration file:

[NCPP]
ROOT_DIR = /data/ncpp/dip/Evaluation/Dataset
BASE_URL = http://hydra.fsl.noaa.gov/thredds/fileServer/ncpp-dip/Evaluation/Dataset
HOSTNAME = hydra.fsl.noaa.gov
SOLR_URL = http://localhost:8984/solr

@author: Luca Cinquini
'''

from esgfpy.publish.factories import DirectoryDatasetRecordFactory, FilepathFileRecordFactory
from esgfpy.publish.services import FileSystemIndexer, PublishingClient
from esgfpy.publish.metadata_parsers import XMLMetadataFileParser
from esgfpy.publish.consts import SERVICE_HTTP, SERVICE_THUMBNAIL
import sys, os
import ConfigParser

CONFIG_FILE = "/usr/local/esgf/config/esgfpy-publish.cfg"


def str2bool(v):
    return v.lower() in ("yes", "true", "t", "1")
                                     
if __name__ == '__main__':
    
    # process command line arguments
    if len(sys.argv) != 4:  # the program name and two arguments
        # stop the program and print an error message
        sys.exit("Usage: esgfpy/publish/main.py <project> <directory (relative to ROOT_DIR)> <True|False (to publish/unpublish)>")
    project = sys.argv[1]
    relativeDirectory = sys.argv[2]
    if relativeDirectory == ".":
        relativeDirectory = ""
    publish = str2bool(sys.argv[3])
    
    # read static system-specific configuration
    config = ConfigParser.RawConfigParser()
    try:
        config.read( os.path.expanduser(CONFIG_FILE) )
        ROOT_DIR = config.get(project, "ROOT_DIR")
        BASE_URL = config.get(project, "BASE_URL")
        HOSTNAME = config.get(project, "HOSTNAME")
        # URL of ESGF publishing service
        # NOTE: must NOT end in '/'
        #! TODO: replace with ESGF publishing service
        SOLR_URL = config.get(project, "SOLR_URL")
        
    except Exception as e:
        print "ERROR: esgfpy-publish configuration file not found"
        print e
        sys.exit(-1)
        
    # sub-directory struture
    subDirs = [ "method", "protocol", "dataset", "metrics", "group", "metrics_type" ]
    
    # constant dataset-level metadata
    datasetFields = { "project": ["NCPP"],
                      "index_node": [HOSTNAME],
                      "metadata_format":["THREDDS"], # currently needed by ESGF web-fe to add datasets to data cart
                      "data_node":[HOSTNAME] }
    
    # constant file-level metadata
    fileFields = {  "index_node": [HOSTNAME],
                    "data_node":[HOSTNAME] }
        
    # Dataset records factory
    myDatasetRecordFactory = DirectoryDatasetRecordFactory("noaa.esrl", rootDirectory=ROOT_DIR, subDirs=subDirs, fields=datasetFields)
    
    # Files records factory
    myFileRecordFactory = FilepathFileRecordFactory(fields=fileFields, 
                                                    rootDirectory=ROOT_DIR,
                                                    baseUrls={ SERVICE_HTTP      : BASE_URL,
                                                               SERVICE_THUMBNAIL : BASE_URL },
                                                    generateThumbnails=True
                                                    )
    indexer = FileSystemIndexer(myDatasetRecordFactory, myFileRecordFactory, metadataFileParser=XMLMetadataFileParser())
    #indexer = FileSystemIndexer(myDatasetRecordFactory, myFileRecordFactory)
    publisher = PublishingClient(indexer, SOLR_URL)
    startDirectory = os.path.join(ROOT_DIR, relativeDirectory)
            
    if publish:
        print 'Publishing...'
        publisher.publish(startDirectory)
    else:
        print 'Un-Publishing...'
        publisher.unpublish(startDirectory)
