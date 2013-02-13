'''
Module :mod:`esgfpy.publish.consts`
===================================

Constants used by esgfpy.publish module.

@author: Luca Cinquini
'''

# Record types
TYPE_DATASET = 'Dataset'
TYPE_FILE = 'File'
TYPE_AGGREGATION = 'Aggregation'

# dictionary that contains mappings from record type to Solr core
SOLR_CORES = { 
              TYPE_DATASET:'datasets',
              TYPE_FILE: 'files',
              TYPE_AGGREGATION: 'aggregations'
             }

SUBTYPE_IMAGE = "image"
SUBTYPE_MOVIE = "movie"
SUBTYPE_DOCUMENT = "document"
SUBTYPE_XML = "xml"
SUBTYPE_DATA = "data"

# dictionary that maps file sub-types to file extensions
FILE_SUBTYPES = { SUBTYPE_IMAGE: ["jpg", "jpeg", "gif", "png", "tif", "tiff"],
                  SUBTYPE_MOVIE: ["mov"],
                  SUBTYPE_DOCUMENT: ["doc", "docx", "pdf", "txt", "ppt", "key"],
                  SUBTYPE_XML: ["xml"], 
                  SUBTYPE_DATA: ["nc", "hdf"]
                }

# service names
SERVICE_HTTP = "HTTP Download"
SERVICE_THUMBNAIL = "Thumbnail"

# dimension of dynamically generated thumbnails
THUMBNAIL_WIDTH = 60
THUMBNAIL_HEIGHT = 60
THUMBNAIL_EXT = "thumbnail.jpg"
