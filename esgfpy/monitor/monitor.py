#
import json
import logging
import urllib
import urllib2
from xml.etree.ElementTree import Element, SubElement, tostring

ESGF_SEARCH_URL = 'https://esgf-node.jpl.nasa.gov/esg-search/search'
PROJECT = 'CMIP5'
RETURNED_FIELDS = 'id,version,latest,replica,master_id,instance_id'
MAX_ROWS = 10
OUTPUT_FILE = 'bad_datasets.out'

logging.basicConfig(format='%(message)s', level=logging.DEBUG)

# dictionary of detected inconsistent datasets
bad_data = {}

# 1) distributed query for all 'latest' datasets
params = [ ('project',PROJECT), 
           ('latest','true'),
           ('distrib','true'),
           ('fields', RETURNED_FIELDS), 
           ('format','application/solr+json'), 
           ('offset', 0), ('limit', MAX_ROWS) ]

# execute query to Solr
url = ESGF_SEARCH_URL + "?"+urllib.urlencode(params)
logging.info('Executing ESGF search URL=%s' % url)
fh = urllib2.urlopen( url )
response = fh.read().decode("UTF-8")
jobj = json.loads(response)

numFound = jobj['response']['numFound']
numRecords = len( jobj['response']['docs'] )
logging.info("Total number of records found: %s number of records returned: %s" % (numFound, numRecords))

# 2) loop over latest datasets, search all versions, check consistency
for doc in jobj['response']['docs'] :
    master_id = doc['master_id']
    _params = [ ('master_id', master_id),
                ('latest','true'),
                ('distrib','true'),
                ('fields', RETURNED_FIELDS), 
                ('facets', 'version'),
                ('format','application/solr+json'), 
                ('offset', 0), ('limit', MAX_ROWS) ]

    _url = ESGF_SEARCH_URL + "?"+urllib.urlencode(_params)
    logging.debug('\tExecuting ESGF search URL=%s' % _url)
    _fh = urllib2.urlopen( _url )
    _response = _fh.read().decode("UTF-8")
    _jobj = json.loads(_response)
    _numFound = _jobj['response']['numFound']
    # "facet_counts":{
    #  "facet_queries":{},
    #  "facet_fields":{
    #    "version":[
    #      "20110601",2]},
    #   "facet_dates":{},
    #   "facet_ranges":{},
    #   "facet_intervals":{},
    #   "facet_heatmaps":{}
    #  }
    _numVersions = len(_jobj['facet_counts']['facet_fields']['version'])/2 # (facet name, facet count)
    logging.debug("\t\tTotal number of records found: %s, number of 'latest' versions: %s" % (_numFound, _numVersions))
    
    if _numVersions == 1:
        logging.warn("\t\tWARNING: found inconsistent latest/version information for dataset with master_id=%s" % master_id)
        bad_data[master_id] = _jobj['response']['docs']
        for _doc in _jobj['response']['docs'] :
            logging.warn("id=%s version=%s latest=%s replica=%s" % (_doc['id'], _doc['version'], _doc['latest'], _doc['replica']))
  
for master_id, instance in bad_data.items():          
    logging.info("Inconsistent dataset detected: master_id=%s instances=%s" % (master_id, instance))
    
with open(OUTPUT_FILE, 'w') as outfile:
    json.dump(bad_data, outfile, 
              sort_keys = True, indent = 4, ensure_ascii = False)
    
    