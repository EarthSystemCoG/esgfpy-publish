import logging
from esgfpy.update.utils import sendSolrXml, buildSolrXml

logging.basicConfig(level=logging.DEBUG)

SOLR_URL = 'http://esgf-node.jpl.nasa.gov:8984/solr'

# associate tech note to dataset
myDict = {'id:obs4MIPs.NASA-JPL.AIRS.mon.v1|esgf-node.jpl.nasa.gov': 
          {'xlink':['https://earthsystemcog.org/site_media/projects/obs4mips/taTechNote_AIRS_L3_RetStd-v5_200209-201105.pdf|AIRS Air Temperature Technical Note|technote',
                    'https://earthsystemcog.org/site_media/projects/obs4mips/husTechNote_AIRS_L3_RetStd-v5_200209-201105.pdf|AIRS Specific Humidity Technical Note|technote']}}

xmlDoc  = buildSolrXml(myDict, update='set', solr_url=SOLR_URL, solr_core='datasets')
sendSolrXml(xmlDoc, solr_url=SOLR_URL, solr_core='datasets')

# associate tech note to each file
myDict = {'dataset_id:obs4MIPs.NASA-JPL.AIRS.mon.v1|esgf-node.jpl.nasa.gov&variable:ta*': 
            {'xlink':['https://earthsystemcog.org/site_media/projects/obs4mips/taTechNote_AIRS_L3_RetStd-v5_200209-201105.pdf|AIRS Air Temperature Technical Note|technote']},
          'dataset_id:obs4MIPs.NASA-JPL.AIRS.mon.v1|esgf-node.jpl.nasa.gov&variable:hus*':
            {'xlink':['https://earthsystemcog.org/site_media/projects/obs4mips/husTechNote_AIRS_L3_RetStd-v5_200209-201105.pdf|AIRS Specific Humidity Technical Note|technote']},
          }

xmlDoc  = buildSolrXml(myDict, update='set', solr_url=SOLR_URL, solr_core='files')
sendSolrXml(xmlDoc, solr_url=SOLR_URL, solr_core='files')

