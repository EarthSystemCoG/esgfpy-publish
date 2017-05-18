# Script to monitor the consistency of the 'latest' datasets throughout the Earth System Grid Federation

# Usage: python esgfpy/monitor/monitor.py <optional space-separated list of search constraints>
# Example: python esgfpy/monitor/monitor.py  project=CMIP5
# Example: python esgfpy/monitor/monitor.py  project=CMIP5 from=2015-01-01T00:00:00Z to=2015-12-31T23:59:29Z

import sys
import json
import logging
import urllib
import urllib2

ESGF_SEARCH_URL = 'https://esgf-node.jpl.nasa.gov/esg-search/search'
RETURNED_FIELDS = 'id,version,latest,replica,master_id,instance_id'
LIMIT = 100 
FORMAT = 'application/solr+json'
OUTPUT_FILE = 'bad_datasets.out'

logging.basicConfig(format='%(message)s', level=logging.INFO)


# function that checks all datasets matching a given set of constraints
def check_datasets(constraints):
    
    # dictionary of detected inconsistent datasets
    bad_datasets = {}
    
    cparams = []
    for constraint in constraints:
        cparams.append( tuple(constraint.split('=')) )

    # 1) execute distributed query to ESGF Search API for all 'latest' datasets
    offset = 0
    numFound = 0
    
    while (offset==0 or offset < numFound):
        params = [ ('latest', 'true'),
                   ('distrib', 'true'),
                   ('fields', RETURNED_FIELDS), 
                   ('format', FORMAT), 
                   ('offset', offset), 
                   ('limit', LIMIT) ] + cparams
        
        # execute query to Solr
        url = ESGF_SEARCH_URL + "?"+urllib.urlencode(params)
        logging.info('Executing ESGF search URL=%s' % url)
        fh = urllib2.urlopen( url )
        response = fh.read().decode("UTF-8")
        jobj = json.loads(response)
        
        numFound = jobj['response']['numFound']
        numRecords = len( jobj['response']['docs'] )
        offset += numRecords
        logging.info("Total number of records found: %s number of records returned: %s" % (numFound, numRecords))
        if numFound==0:
            return bad_datasets # no records matching the query
  
        # 2) loop over latest datasets, search all versions, check consistency
        for doc in jobj['response']['docs'] :
            master_id = doc['master_id']
            _params = [ ('master_id', master_id),
                        ('latest', 'true'),
                        ('distrib', 'true'),
                        ('fields', RETURNED_FIELDS), 
                        ('facets', 'version'),
                        ('format', FORMAT), 
                        ('offset', 0), 
                        ('limit', LIMIT) ]
        
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
            
            if _numVersions > 1:
                logging.warn("\t\tWARNING: found inconsistent latest/version information for dataset with master_id=%s" % master_id)
                # store this dataset
                bad_datasets[master_id] = _jobj['response']['docs']
                for _doc in _jobj['response']['docs'] :
                    logging.warn("\t\t\tid=%s version=%s latest=%s replica=%s" % (_doc['id'], _doc['version'], _doc['latest'], _doc['replica']))

    # return results
    logging.info("Total number of inconsistent datasets found: %s" % len(bad_datasets))
    return bad_datasets

      
# main program
if __name__ == '__main__':
    
    # capture optional search constraints from command line arguments
    if len(sys.argv)>1:
        constraints = sys.argv[1:]
    else:
        constraints = []
    
    # execute distributed queries
    bad_datasets = check_datasets(constraints)
    
    # write out results
    with open(OUTPUT_FILE, 'w') as outfile:
        json.dump(bad_datasets, outfile, 
                  sort_keys = True, indent = 4, ensure_ascii = False)
    
    