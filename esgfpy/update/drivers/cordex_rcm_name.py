'''
Example script to execute atomic updates on CORDEX dataset metadata:

o insert new facet 'rcm_name' with value equal to the 'model' facet
o remove the 'model' facet

This script should have NO dependencies beyond the standard Python libraries.

This script should be executed on the same server that hosts the master Solr shard,
or a server that has access to that host on port 8984.

Installation:




Suggested execution:

o Stop the ESGF node
o Backup the master Solr index directory: /esg/solr-index/master-8984
o Restart the ESGF node

o git clone https://github.com/EarthSystemCoG/esgfpy-publish.git
o cd esgfpy-publish
o export PYTHONPATH=.:$PYTHONPATH
o python esgfpy/update/drivers/cordex_rcm_name.py

o Look for changes into the master Solr (port 8984) or wait for the changes to be propagated to the slave Solr (port 8983)


@author: Luca Cinquini
'''

import logging
from esgfpy.update.utils import updateSolr

logging.basicConfig(level=logging.DEBUG)

# change accordingly: must target the master Solr on port 8984
SOLR_URL = 'http://localhost:8984/solr'

# dictionary of matching records and metadata fields to be updated
myDict = {'project:obs4MIPs': {'rcm_name':['$model'], 'model':None } }

# execute the atomi updates
updateSolr(myDict, update='set', solr_url=SOLR_URL, solr_core='datasets')