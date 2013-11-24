'''
Module :mod:`pyesgf.publish.main`
=================================

Example driver program for publishing/unpublishing records to ESGF.

The static configuration parameters are read from CONFIG_FILE.
Example configuration file:

[NCPP]
ROOT_DIR = /data/ncpp/dip/Evaluation/Dataset
ROOT_ID = ncpp.dip-2013
BASE_URL = http://hydra.fsl.noaa.gov/thredds/fileServer/ncpp-dip/Evaluation/Dataset
HOSTNAME = hydra.fsl.noaa.gov
SOLR_URL = http://localhost:8984/solr
PROJECT = NCPP
SUBDIRS = method, protocol, dataset, metrics, group, metrics_type

@author: Luca Cinquini
'''

from esgfpy.publish.factories import DirectoryDatasetRecordFactory, FilepathFileRecordFactory
from esgfpy.publish.services import FileSystemIndexer, PublishingClient
from esgfpy.publish.consts import SERVICE_HTTP, SERVICE_THUMBNAIL
from esgfpy.publish.utils import str2bool
import sys, os
import ConfigParser
import logging

logging.basicConfig(level=logging.DEBUG)

CONFIG_FILE = "/usr/local/esgf/config/esgfpy-publish.cfg"
                                     
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
        BASE_URL = config.get(project, "BASE_URL")
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
            
    # constant dataset-level metadata
    datasetFields = { "project": [PROJECT],
                      "index_node": [HOSTNAME],
                      "metadata_format":["THREDDS"], # currently needed by ESGF web-fe to add datasets to data cart
                      "data_node":[HOSTNAME] }
    
    # constant file-level metadata
    fileFields = {  "index_node": [HOSTNAME],
                    "data_node":[HOSTNAME] }
    
    # 12 fields example: QED2013_evalData_ARRM_CGCM3_P1_G1_tasmax_ann_tx90p_US48_19712000.jpg
    # 10 fields example: QED2013_evalData_Maurer02_P1_G2agro_tasmax_ann_GSL_US48_19712000.jpg
    # 13 fields example: QED2013_comparData_obsosb_Maurer02_PRISM_P1_G1_tasmax_bias_day_stdev_US48_19712000.jpg
    # 14 fields example: QED2013_comparData_downscObs_ARRM_CGCM3_Maurer02_P1_G2agro_tasmax_bias_ann_GSL_US48_19712000.jpg
    
    
    # 6 fields structure:  evaluation_data|parameter|metric|frequency|period|region.jpg
    # 6 fields example: ARRM-CGCM3_tx90p_median_mon7_1971-2000_US48.jpg
    # 8 fields structure: comparison_data|evaluation_data|comparison_metric|parameter|metric|frequency|period|region.jpg
    # 8 fields example: Maurer02_PRISM_bias_tasmax_stdev_seaWin_1971-2000_US48.jpg
    #                   maurer02v2_arrm_ccsm_bias_praavga_mean_annual_1971-2000.png
    """
    FILENAME_PATTERNS = [ "(?P<evaluation_data>[^_]*)_(?P<variable>[^_]*)_(?P<metric>[^_]*)" \
                         +"_(?P<frequency>[^_]*)_(?P<period>[^_]*)_(?P<region>[^_]*)\.\w+",
                          
                          "(?P<comparison_data>[^_]*)_(?P<evaluation_data>[^_]*)_(?P<comparison_metric>[^_]*)" \
                         +"_(?P<variable>[^_]*)_(?P<metric>[^_]*)_(?P<frequency>[^_]*)" \
                         +"_(?P<perdio>[^_]*)_(?P<region>[^_]*)" ]
                         """
    FILENAME_PATTERNS = []
                                     
    # Dataset records factory
    myDatasetRecordFactory = DirectoryDatasetRecordFactory(ROOT_ID, rootDirectory=ROOT_DIR, subDirs=SUBDIRS, fields=datasetFields)
    
    # Files records factory
    myFileRecordFactory = FilepathFileRecordFactory(fields=fileFields, 
                                                    rootDirectory=ROOT_DIR,
                                                    filenamePatterns=FILENAME_PATTERNS,
                                                    baseUrls={ SERVICE_HTTP      : BASE_URL,
                                                               SERVICE_THUMBNAIL : BASE_URL },
                                                    generateThumbnails=True
                                                    )
    indexer = FileSystemIndexer(myDatasetRecordFactory, myFileRecordFactory)
    #indexer = FileSystemIndexer(myDatasetRecordFactory, myFileRecordFactory)
    publisher = PublishingClient(indexer, SOLR_URL)
    startDirectory = os.path.join(ROOT_DIR, relativeDirectory)
            
    if publish:
        print 'Publishing...'
        publisher.publish(startDirectory)
    else:
        print 'Un-Publishing...'
        publisher.unpublish(startDirectory)
