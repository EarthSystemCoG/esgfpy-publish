'''
Module :mod:`esgfpy.publish.consts`
===================================

Constants used by esgfpy.publish module.

@author: Luca Cinquini
'''

import datetime as dt
from dateutil.tz import tzutc

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
SUBTYPE_XCEL = "xcel"

# dictionary that maps file sub-types to file extensions
FILE_SUBTYPES = { SUBTYPE_IMAGE: ["jpg", "jpeg", "gif", "png", "tif", "tiff"],
                  SUBTYPE_MOVIE: ["mov"],
                  SUBTYPE_DOCUMENT: ["doc", "docx", "pdf", "txt", "ppt", "key"],
                  SUBTYPE_XML: ["xml"],
                  SUBTYPE_DATA: ["nc", "hdf"],
                  SUBTYPE_XCEL: ["xlsx", "xls", "csv"]
                }

# service names (must conform to THREDDS Controlled Vocabulary)
SERVICE_HTTP = "HTTPServer" # NOTE: ESGF wget script generator mandates use of 'HTTPServer'
SERVICE_OPENDAP = "OpenDAP"
SERVICE_THUMBNAIL = "Thumbnail"
SERVICE_THREDDS = "THREDDS"

# dimension of dynamically generated thumbnails
THUMBNAIL_WIDTH = 60
THUMBNAIL_HEIGHT = 60
THUMBNAIL_EXT = "thumbnail.jpg"

# default file for mapping facet keys and values
METADATA_MAPPING_FILE = "/usr/local/esgf/config/metadata-mapping.cfg"

# core metadata fields
VERSION = 'version'
DATA_NODE = 'data_node'
INDEX_NODE = 'index_node'
INSTANCE_ID = 'instance_id'
MASTER_ID = 'master_id'
CHECKSUM = 'checksum'
CHECKSUM_TYPE = 'checksum_type'
TRACKING_ID = 'tracking_id'
SIZE = 'size'
VARIABLE = 'variable'
DATETIME_START = 'datetime_start'
DATETIME_STOP = 'datetime_stop'
GEO = 'geo'
NORTH_DEGREES = "north_degrees"
SOUTH_DEGREES = "south_degrees"
EAST_DEGREES = "east_degrees"
WEST_DEGREES = "west_degrees"

TAI93_DATETIME_START = dt.datetime(1993, 1, 1, 0, 0, 0, tzinfo=tzutc())

COORDINATES = ['longitude', 'lon', 'grid_center_lon', 'grid_corner_lon',
               'latitude', 'lat', 'grid_center_lat', 'grid_corner_lat',
               'altitude', 'alt', 'level', 'levels', 'lev', 'pressure', 'p',
               'time', 'char_time',
               'grid_dims', 'period_length']
