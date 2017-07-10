# sample script that reads a JSON configuration file for ESGF quality control flags
# and publishes the information to the Solr index

import logging
from esgfpy.update.utils import updateSolr
from pprint import pprint
import json

logging.basicConfig(level=logging.DEBUG)

SOLR_URL = 'http://esgf-node.jpl.nasa.gov:8984/solr'

# read indicators from JSON file
json_file = 'obs4mips_indicators_jpl.json' # FIXME: use GitHub URL
with open(json_file) as data_file:    
    json_data = json.load(data_file)

pprint(json_data)

# publish to Solr
#print "Publishing metadata: %s" % myDict
#jDict = json.dumps(myDict)
#print jDict
updateSolr(json_data, update='set', solr_url=SOLR_URL, solr_core='datasets')

