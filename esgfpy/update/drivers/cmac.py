'''
Script to change metadata for all CMAC records from:
"index_node":"esg-datanode.jpl.nasa.gov"
to:
"index_node":"esgf_node.jpl.nasa.gov"
'''

import logging
from esgfpy.update.utils import sendSolrXml, buildSolrXml

logging.basicConfig(level=logging.DEBUG)

SOLR_URL = 'http://esgf-node.jpl.nasa.gov:8982/solr'

# set index_node=esgf-node.jpl.nasa.gov for all datasets, files
#myDict = {'project:CMAC': {'index_node':['esgf-node.jpl.nasa.gov'] } }
#for core in ['datasets','files']:
#    xmlDoc  = buildSolrXml(myDict, update='set', solr_url=SOLR_URL, solr_core=core)
#    sendSolrXml(xmlDoc, solr_url=SOLR_URL, solr_core=core)

# update datasets metadata 
myDict = {'project:CMAC': {'shard':['localhost:8982'] },
          'id:CMAC.NASA-GSFC.MODIS.mon.v1|esgf-node.jpl.nasa.gov': { 'number_of_files':['10'] },
          'id:CMAC.NASA-GSFC.AIRS.mon.v1|esgf-node.jpl.nasa.gov': { 'number_of_files':['24'] },
          }
xmlDoc  = buildSolrXml(myDict, update='set', solr_url=SOLR_URL, solr_core='datasets')
sendSolrXml(xmlDoc, solr_url=SOLR_URL, solr_core='datasets')