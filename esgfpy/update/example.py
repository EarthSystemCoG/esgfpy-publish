import logging
from esgfpy.update.utils import updateSolr

logging.basicConfig(level=logging.DEBUG)

SOLR_URL = 'http://esgf-node.jpl.nasa.gov:8982/solr'

# dictionary containing fields to be "set" (i.e. will override fields with the same name)
myDict = { 'project:GASS-YoTC-MIP&institute:MRI': {'shard':['yyyy'] } }
          
updateSolr(myDict, update='set', solr_url=SOLR_URL, solr_core='datasets')
