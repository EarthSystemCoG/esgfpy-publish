'''
Module :mod:`pyesgf.publish.main`
=================================

Example driver program for publishing/unpublishing records to Solr.

The static configuration parameters are read from CONFIG_FILE.
Example configuration file:

[CO2-VSDE]
ROOT_DIR = /usr/local/co2/data
ROOT_ID = nasa.jpl.co2
BASE_URL_THREDDS = http://co2dev.jpl.nasa.gov/thredds/catalog
BASE_URL_HTTP = http://co2dev.jpl.nasa.gov/thredds/fileServer
BASE_URL_OPENDAP = http://co2dev.jpl.nasa.gov/thredds/dodsC
HOSTNAME = co2dev.jpl.nasa.gov
SOLR_URL = http://localhost:8080/solr
PROJECT = CO2-VSDE
SUBDIRS = instrument, version
#VERSION = 3.4

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
from esgfpy.publish.parsers import (AcosFileParser, AcosLiteFileParser_v34r03,
                                    Oco2L2StdFileParser, Oco2L2LiteFileParser, Oco2L2IDPFileParser,
                                    TesFileParser, AirsFileParser)

logging.basicConfig(level=logging.DEBUG)

CONFIG_FILE = "/usr/local/esgf/config/esgfpy-publish.cfg"
MAPPING_FILE = '/usr/local/esgf/config/co2_facets_mapping.cfg'

if __name__ == '__main__':

    # process command line arguments
    if len(sys.argv) != 4:  # the program name and two arguments
        # stop the program and print an error message
        sys.exit("Usage: python esgfpy/publish/main.py <project> <directory (relative to ROOT_DIR)> <True|False (to publish/unpublish)>")
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
        #VERSION = config.get(project, "VERSION")
        # Solr URL (NOTE: must NOT end in '/')
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
                      "data_node":[HOSTNAME],
                      "processing_level":["L2"] }

    # constant file-level metadata
    fileFields = {  "index_node": [HOSTNAME],
                    "data_node":[HOSTNAME] }

    # possible filename patterns
    FILENAME_PATTERNS = [ 
                          # acos_L2s_130101_34_Production_v150151_L2s30300_r01_PolB_130225030149.h5
                          "acos_L2s_(?P<yymmdd>\d+)_\d\d_Production_.+\.h5",
                          # acos_L2s_100131_30_Evaluation_v150151_L2s30400_r01_PolB_130904153155c.h5
                          "acos_L2s_(?P<yymmdd>\d+)_\d\d_Evaluation_.+\.h5",
                          # acos_b34_L2lite_20130129_r02c.nc
                          "acos_b34_L2lite_(?P<yyyymmdd>\d+)_r02c.nc",
                          # acos_b34_L2lite_20100327_r03n.nc
                          "acos_b34_L2lite_(?P<yyyymmdd>\d+)_r03n.nc",                          
                          # AIRS.2010.01.01.031.L2.CO2_Std.v5.4.11.0.CO2.T10034082113.hdf
                          "AIRS\.(?P<yyyy>\d+)\.(?P<mm>\d+)\.(?P<dd>\d+)\..+\.hdf",
                          # TES-Aura_L2-CO2-Nadir_r0000015508_C01_F07_10.he5
                          "TES-Aura_L2-CO2-Nadir_.+\.he5",
                          # oco2_L2StdGL_03783a_150319_B6000r_150328205055.h5
                          "oco2_L2StdGL.+.h5", "oco2_L2StdND.+.h5", "oco2_L2StdTG.+.h5", 
                          # oco2_L2IDPGL_03783a_150319_B6000r_150328142340.h5
                          "oco2_L2IDPGL.+.h5", "oco2_L2IDPND.+.h5", "oco2_L2IDPTG.+.h5", 
                          # oco2_L1bScGL_89234a_100924_B3500_140205015904n.h5
                          #"oco2_L1b.+\.h5",
                          # oco2_L2Daily_141127_B5000_150116014823s.nc4
                          "oco2_L2Std.+\.nc4",
                          # FIXME
                          "test_oco2.+\.nc4",
                       ]


    # Dataset records factory
    myDatasetRecordFactory = DirectoryDatasetRecordFactory(ROOT_ID, rootDirectory=ROOT_DIR, subDirs=SUBDIRS,
                                                           fields=datasetFields, metadataMapper=metadataMapper,
                                                           baseUrls={ SERVICE_THREDDS : BASE_URL_THREDDS },
                                                           addVersion=False,
                                                           )

    # Files records factory
    maxDaysPast = int(os.getenv('MAX_DAYS_PAST', -1)) # optional environment to harvest only the most recent files
    myFileRecordFactory = FilepathFileRecordFactory(fields=fileFields,
                                                    rootDirectory=ROOT_DIR,
                                                    filenamePatterns=FILENAME_PATTERNS,
                                                    baseUrls={ SERVICE_HTTP    : BASE_URL_HTTP,
                                                               SERVICE_OPENDAP : BASE_URL_OPENDAP },
                                                    generateChecksum=False,
                                                    metadataMapper=metadataMapper,
                                                    maxDaysPast=maxDaysPast
                                                    )
    # use special list of metadata parsers
    myFileRecordFactory.metadataParsers = [AcosLiteFileParser_v34r03(), AcosFileParser(),
                                           Oco2L2StdFileParser(), Oco2L2IDPFileParser(), Oco2L2LiteFileParser(),
                                           TesFileParser(),
                                           AirsFileParser() ]

    # metadata fields to copy Dataset <--> File
    append=False
    fileMetadataKeysToCopy = {}
    datasetMetadataKeysToCopy = {'project':append, 'instrument':append, 'version':append, "institute":append,
                                 "processing_level":append, 'mission':append, 'collection':append, 'product':append }

    indexer = FileSystemIndexer(myDatasetRecordFactory, myFileRecordFactory,
                                fileMetadataKeysToCopy=fileMetadataKeysToCopy, datasetMetadataKeysToCopy=datasetMetadataKeysToCopy)
    maxRecords = -1 # publish all records
    publisher = PublishingClient(indexer, SOLR_URL, maxRecords=maxRecords)
    startDirectory = os.path.join(ROOT_DIR, relativeDirectory)

    if publish:
        print 'Publishing...(maxDaysPast=%s)' % maxDaysPast
        publisher.publish(startDirectory)
    else:
        print 'Un-Publishing...'
        publisher.unpublish(startDirectory)
