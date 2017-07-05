import logging
from esgfpy.update.utils import updateSolr

logging.basicConfig(level=logging.DEBUG)

SOLR_URL = 'http://esgf-node.jpl.nasa.gov:8984/solr'

# associate supplementary data to datasets
# http://esgf-data.jpl.nasa.gov/thredds/fileServer/obs4MIPs/technotes/cltTechNote_MODIS_L3_C5_200003-201109.pdf
myDict = {'id:obs4MIPs.NASA-JPL.TES.tro3.mon.v20110608|esgf-data.jpl.nasa.gov':
            {'xlink':['https://esgf-data.jpl.nasa.gov/thredds/fileServer/obs4MIPs/supplementary_data/TES-SUPPLEMENTARY.zip|TES Supplementary Data|supdata']},
          }

updateSolr(myDict, update='set', solr_url=SOLR_URL, solr_core='datasets')
