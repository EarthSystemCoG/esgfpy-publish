'''
Module :mod:`pyesgf.publish.main`
=================================

Example driver program for publishing/unpublishing records to ESGF.

The static configuration parameters are read from CONFIG_FILE.
Example configuration file:

[GASS-YoTC-MIP]
ROOT_DIR = /Users/cinquini/data/davarchive/data/archive
ROOT_ID = gass-yotc-mip
BASE_URL = http://localhost:8000/site_media/data/ncpp
HOSTNAME = localhost:8080
SOLR_URL = http://localhost:8984/solr
PROJECT = GASS-YoTC-MIP
SUBDIRS = model, experiment, variable

@author: Luca Cinquini
'''

from esgfpy.publish.factories import DirectoryDatasetRecordFactory, FilepathFileRecordFactory
from esgfpy.publish.services import FileSystemIndexer, PublishingClient
from esgfpy.publish.metadata_parsers import XMLMetadataFileParser
from esgfpy.publish.metadata_mappers import ConfigFileMetadataMapper
from esgfpy.publish.consts import SERVICE_HTTP, SERVICE_THUMBNAIL, SERVICE_OPENDAP
import sys, os
import ConfigParser

CONFIG_FILE = "/usr/local/esgf/config/esgfpy-publish.cfg"
MAPPING_FILE = '/usr/local/esgf/config/gass-ytoc-mip_facets_mapping.cfg'

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
        ROOT_ID = config.get(project, "ROOT_ID")
        BASE_URL_HTTP = config.get(project, "BASE_URL_HTTP")
        BASE_URL_OPENDAP = config.get(project, "BASE_URL_OPENDAP")
        HOSTNAME = config.get(project, "HOSTNAME")
        PROJECT = config.get(project, "PROJECT")
        # URL of ESGF publishing service
        # NOTE: must NOT end in '/'
        #! TODO: replace with ESGF publishing service
        SOLR_URL = config.get(project, "SOLR_URL")
        # [['activity', 'evaluation_data', 'variable', 'metric', 'frequency', 'period', 'region'], 
        #  ['activity', 'comparison_data', 'evaluation_data', 'comparison_metric', 'parameter', 'metric', 'frequency', 'period', 'region']]
        SUBDIRS = [template.split(",") for template in config.get(project, "SUBDIRS").replace(" ","").replace("\n","").split("|")]
        
    except Exception as e:
        print "ERROR: esgfpy-publish configuration file not found"
        print e
        sys.exit(-1)
        
    # class that maps metadata values from a configuration file
    metadataMapper = ConfigFileMetadataMapper(MAPPING_FILE)
            
    # constant dataset-level metadata
    datasetFields = { "project": [PROJECT],
                      "index_node": [HOSTNAME],
                      "metadata_format":["THREDDS"], # currently needed by ESGF web-fe to add datasets to data cart
                      "data_node":[HOSTNAME] }
    
    # constant file-level metadata
    fileFields = {  "index_node": [HOSTNAME],
                    "data_node":[HOSTNAME] }
    
                            
    # possible filename patterns
    FILENAME_PATTERNS = [ "(?P<model_name>[^\.]*)\.(?P<variable>[^\.]*)\.(?P<start>\d+)-(?P<stop>\d+)\.nc", # ModelE.tvapbl.2000010100-2000123118.nc
                          "(?P<model_name>[^\.]*)\.(?P<variable>[^\.]*)\.(?P<start>\d+)\.00Z\.nc" ] # ModelE.zg.20100109.00Z.nc

                                     
    # Dataset records factory
    myDatasetRecordFactory = DirectoryDatasetRecordFactory(ROOT_ID, rootDirectory=ROOT_DIR, subDirs=SUBDIRS, 
                                                           fields=datasetFields, metadataMapper=metadataMapper)
    
    # Files records factory
    # fields={}, rootDirectory=None, filenamePatterns=[], baseUrls={}, generateThumbnails=False
    myFileRecordFactory = FilepathFileRecordFactory(fields=fileFields, 
                                                    rootDirectory=ROOT_DIR,
                                                    filenamePatterns=FILENAME_PATTERNS,
                                                    baseUrls={ SERVICE_HTTP    : BASE_URL_HTTP,
                                                               SERVICE_OPENDAP : BASE_URL_OPENDAP },
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
