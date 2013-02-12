'''
Module :mod:`pyesgf.publish.main`
=================================

Example driver program for publishing/unpublishing records to ESGF.

@author: Luca Cinquini
'''

from esgfpy.publish.factories import DirectoryDatasetRecordFactory, FilepathFileRecordFactory
from esgfpy.publish.services import FileSystemIndexer, PublishingClient
from esgfpy.publish.metadata_parsers import XMLMetadataFileParser
import sys

def str2bool(v):
    return v.lower() in ("yes", "true", "t", "1")
                                     
if __name__ == '__main__':
    
    # process command line arguments
    if len(sys.argv) != 2:  # the program name and one argument
        # stop the program and print an error message
        sys.exit("Usage: esgfpy/publish/main.py True|False (to publish/unpublish)")
    publish = str2bool(sys.argv[1])
        
    # URL of ESGF publishing service
    # NOTE: must NOT end in '/'
    #! TODO: replace with ESGF publishing service
    solrBaseUrl = "http://localhost:8984/solr"
    
    # root directory where the data are stored
    rootDirectory = "/Users/cinquini/data/Evaluation/Dataset"
    
    # sub-directory struture
    subDirs = [ "method", "protocol", "dataset", "metrics", "group", "metrics_type" ]
    
    # constant dataset-level metadata
    datasetFields = { "project": ["NCPP"],
                      "index_node": ["localhost:8080"],
                      "data_node":["localhost:8080"] }
    
    # constant file-level metadata
    fileFields = {  "index_node": ["localhost:8080"],
                    "data_node":["localhost:8080"] }
        
    # Dataset records factory
    myDatasetRecordFactory = DirectoryDatasetRecordFactory("noaa.esrl", rootDirectory=rootDirectory, subDirs=subDirs, fields=datasetFields)
    
    # Files records factory
    myFileRecordFactory = FilepathFileRecordFactory(fields=fileFields, 
                                                    rootDirectory=rootDirectory,
                                                    baseUrls={ "http://localhost:8000/site_media/data/ncpp/Evaluation/Dataset":"HTTP Download"},
                                                    generateThumbnails=True
                                                    )
    indexer = FileSystemIndexer(myDatasetRecordFactory, myFileRecordFactory, metadataFileParser=XMLMetadataFileParser())
    #indexer = FileSystemIndexer(myDatasetRecordFactory, myFileRecordFactory)
    publisher = PublishingClient(indexer, solrBaseUrl)
    if publish:
        print 'Publishing...'
        publisher.publish(rootDirectory)
    else:
        print 'Unpublishing...'
        publisher.unpublish(rootDirectory)
