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

# 1) set index_node=esgf-node.jpl.nasa.gov for all datasets, files
#myDict = {'project:CMAC': {'index_node':['esgf-node.jpl.nasa.gov'] } }
#for core in ['datasets','files']:
#    xmlDoc  = buildSolrXml(myDict, update='set', solr_url=SOLR_URL, solr_core=core)
#    sendSolrXml(xmlDoc, solr_url=SOLR_URL, solr_core=core)

# 2) set shard=localhost:8982 for all datasets
myDict = {'project:CMAC': {'shard':['localhost:8982'] } }
xmlDoc  = buildSolrXml(myDict, update='set', solr_url=SOLR_URL, solr_core='datasets')
sendSolrXml(xmlDoc, solr_url=SOLR_URL, solr_core='datasets')