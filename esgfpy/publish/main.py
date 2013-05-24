'''
Module :mod:`pyesgf.publish.main`
=================================

Example driver program for publishing/unpublishing records to ESGF.

@author: Luca Cinquini
'''

from esgfpy.publish.factories import DirectoryDatasetRecordFactory, FilepathFileRecordFactory
from esgfpy.publish.services import FileSystemIndexer, PublishingClient
from esgfpy.publish.metadata_parsers import XMLMetadataFileParser
from esgfpy.publish.consts import SERVICE_HTTP, SERVICE_THUMBNAIL
import sys

# laptop parameters
ROOT_DIR = "/Users/cinquini/data/Evaluation/Dataset"
BASE_URL = "http://localhost:8000/site_media/data/ncpp/Evaluation/Dataset"
HOSTNAME = "localhost:8080"

# dev-hydra parameters
#ROOT_DIR = "/data/ncpp/dip/Evaluation/Dataset"
#BASE_URL = "http://dev-hydra.esrl.svc/thredds/fileServer/ncpp-dip/Evaluation/Dataset"
#HOSTNAME = "dev-hydra.wx.noaa.gov"

# hydra parameters
#ROOT_DIR = "/data/ncpp/dip/Evaluation/Dataset"
#BASE_URL = "http://hydra.fsl.noaa.gov/thredds/fileServer/ncpp-dip/Evaluation/Dataset"
#HOSTNAME = "hydra.fsl.noaa.gov"


def str2bool(v):
    return v.lower() in ("yes", "true", "t", "1")
                                     
if __name__ == '__main__':
    
    # process command line arguments
    if len(sys.argv) != 2:  # the program name and one argument
        # stop the program and print an error message
        sys.exit("Usage: esgfpy/publish/main.py True|False (to publish/unpublish)")
    startDirectory = "/Users/cinquini/data/Evaluation/Dataset/Hayhoe/Protocol1/GCM1Data/Metrics/Group1/Temperature"
    publish = str2bool(sys.argv[1])
        
    # URL of ESGF publishing service
    # NOTE: must NOT end in '/'
    #! TODO: replace with ESGF publishing service
    solrBaseUrl = "http://localhost:8984/solr"
    
    # root directory where the data are stored
    rootDirectory = ROOT_DIR
    
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
    myDatasetRecordFactory = DirectoryDatasetRecordFactory("noaa.esrl", rootDirectory=rootDirectory, subDirs=subDirs, fields=datasetFields)
    
    # Files records factory
    myFileRecordFactory = FilepathFileRecordFactory(fields=fileFields, 
                                                    rootDirectory=rootDirectory,
                                                    baseUrls={ SERVICE_HTTP      : BASE_URL,
                                                               SERVICE_THUMBNAIL : BASE_URL },
                                                    generateThumbnails=True
                                                    )
    indexer = FileSystemIndexer(myDatasetRecordFactory, myFileRecordFactory, metadataFileParser=XMLMetadataFileParser())
    #indexer = FileSystemIndexer(myDatasetRecordFactory, myFileRecordFactory)
    publisher = PublishingClient(indexer, solrBaseUrl)
    if publish:
        print 'Publishing...'
        publisher.publish(startDirectory)
    else:
        print 'Un-Publishing...'
        publisher.unpublish(startDirectory)
