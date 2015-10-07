import logging
from esgfpy.update.utils import sendSolrXml, buildSolrXml

logging.basicConfig(level=logging.DEBUG)

# dictionary containing fields to be "set" (i.e. will override fields with the same name)
myDict = {'id:test.test.v1.testData.nc|esgf-dev.jpl.nasa.gov': 
          {'xlink':['http://esg-datanode.jpl.nasa.gov/thredds/fileServer/esg_dataroot/obs4MIPs/technotes/zosTechNote_AVISO_L4_199210-201012.pdf|AVISO Sea Surface Height Technical Note|summary']}}

# start XML document containing all update instructions
xmlDoc  = buildSolrXml(myDict, update='set', solr_url='http://esgf-dev.jpl.nasa.gov:8984/solr', solr_core='files')

sendSolrXml(xmlDoc, solr_url='http://esgf-dev.jpl.nasa.gov:8984/solr', solr_core='files')

