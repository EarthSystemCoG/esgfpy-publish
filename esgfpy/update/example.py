import logging
from esgfpy.update.utils import sendSolrXml, buildSolrXml

logging.basicConfig(level=logging.DEBUG)

SOLR_URL = 'http://esgf-node.jpl.nasa.gov:8982/solr'

# dictionary containing fields to be "set" (i.e. will override fields with the same name)
myDict = { 'project:GASS-YoTC-MIP': {'shard':['localhost:8989'] } }
          
# start XML document containing all update instructions
xmlDoc  = buildSolrXml(myDict, update='set', solr_url=SOLR_URL, solr_core='datasets')

sendSolrXml(xmlDoc, solr_url=SOLR_URL, solr_core='datasets')

