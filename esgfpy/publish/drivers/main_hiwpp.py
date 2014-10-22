'''
Module :mod:`pyesgf.publish.main`
=================================

Driver program for publishing/unpublishing HIWPP records to ESGF.

The static configuration parameters are read from CONFIG_FILE.
Example configuration file:

[HIWPP]
ROOT_DIR = /data/hiwpp
ROOT_ID = hiwpp
BASE_URL_THREDDS = http://hydra.fsl.noaa.gov/thredds/catalog/
BASE_URL_HTTP = http://hydra.fsl.noaa.gov/thredds/fileServer/
BASE_URL_OPENDAP = http://hydra.fsl.noaa.gov/thredds/dodsC/
HOSTNAME = hydra.fsl.noaa.gov
SOLR_URL = http://localhost:8983/solr
PROJECT = HIWPP
SUBDIRS = model, format


@author: Luca Cinquini
'''

from esgfpy.publish.factories import DirectoryDatasetRecordFactory, FilepathFileRecordFactory
from esgfpy.publish.services import FileSystemIndexer, PublishingClient
from esgfpy.publish.metadata_mappers import ConfigFileMetadataMapper
from esgfpy.publish.consts import SERVICE_HTTP, SERVICE_OPENDAP, SERVICE_THREDDS
from esgfpy.publish.utils import str2bool
import sys, os
import ConfigParser
import logging

logging.basicConfig(level=logging.DEBUG)

CONFIG_FILE = "/usr/local/esgf/config/esgfpy-publish.cfg"
MAPPING_FILE = '/usr/local/esgf/config/hiwpp_facets_mapping.cfg'


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
        BASE_URL_THREDDS = config.get(project, "BASE_URL_THREDDS")
        BASE_URL_HTTP = config.get(project, "BASE_URL_HTTP")
        BASE_URL_OPENDAP = config.get(project, "BASE_URL_OPENDAP")
        HOSTNAME = config.get(project, "HOSTNAME")
        PROJECT = config.get(project, "PROJECT")
        VERSION = config.get(project, "VERSION")
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
                      "data_node":[HOSTNAME],
                      "version":[VERSION] }

    # constant file-level metadata
    fileFields = {  "index_node": [HOSTNAME],
                    "data_node":[HOSTNAME],
                    "version":[VERSION] }

    # possible filename patterns
    FILENAME_PATTERNS = [ "(?P<numbers>\d+)\.nc", # spp/netcdf/1429012000012.nc
                          "(?P<numbers>\d+)",     # fim9z/0p125/dup1/1429100000002
                        ] #


    # Dataset records factory
    myDatasetRecordFactory = DirectoryDatasetRecordFactory(ROOT_ID, rootDirectory=ROOT_DIR, subDirs=SUBDIRS,
                                                           fields=datasetFields, metadataMapper=metadataMapper,
                                                           baseUrls={ SERVICE_THREDDS : BASE_URL_THREDDS },
                                                           )

    # Files records factory
    # fields={}, rootDirectory=None, filenamePatterns=[], baseUrls={}, generateThumbnails=False
    myFileRecordFactory = FilepathFileRecordFactory(fields=fileFields,
                                                    rootDirectory=ROOT_DIR,
                                                    filenamePatterns=FILENAME_PATTERNS,
                                                    baseUrls={ SERVICE_HTTP    : BASE_URL_HTTP,
                                                               SERVICE_OPENDAP : BASE_URL_OPENDAP },
                                                    generateChecksum=True
                                                    )

    # metadata fields to copy Dataset <--> File
    append=False
    fileMetadataKeysToCopy = {'variable_long_name':append, 'variable':append, 'units':append }
    datasetMetadataKeysToCopy = {'project':append, 'model':append, 'experiment':append }

    indexer = FileSystemIndexer(myDatasetRecordFactory, myFileRecordFactory,
                                fileMetadataKeysToCopy=fileMetadataKeysToCopy, datasetMetadataKeysToCopy=datasetMetadataKeysToCopy)
    #indexer = FileSystemIndexer(myDatasetRecordFactory, myFileRecordFactory)
    publisher = PublishingClient(indexer, SOLR_URL)
    startDirectory = os.path.join(ROOT_DIR, relativeDirectory)

    if publish:
        print 'Publishing...'
        publisher.publish(startDirectory)
    else:
        print 'Un-Publishing...'
        publisher.unpublish(startDirectory)
