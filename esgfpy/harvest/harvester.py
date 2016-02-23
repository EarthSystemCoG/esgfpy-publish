'''
Created on Feb 22, 2016

@author: cinquini
'''

import solr
import logging
import urllib, urllib2
import json
import dateutil.parser
from datetime import timedelta
from esgfpy.migrate.solr2solr import migrate

logging.basicConfig(level=logging.DEBUG)

DEFAULT_QUERY = "*:*"
CORE_DATASETS = 'datasets'
CORE_FILES = 'files'
CORE_AGGREGATIONS = 'aggregations'


def delete_solr_records(solr_base_url, core, query):
    
    solr_url = solr_base_url +"/" + core
    solr_server = solr.Solr(solr_url)
    solr_server.delete_query(query)
    #solr_server.optimize()

def sync_solrs(source_solr_base_url, target_solr_base_url, core=None, query=DEFAULT_QUERY):
    '''Method to sync two Solr servers.'''
    
    retDict = check_sync(source_solr_base_url, target_solr_base_url, core=core, query=index_query)
    
    if retDict['status']:
        logging.info("Solr servers are in sync, no further action necessary")
        
    else:
        
        # use largest possible datetime interval
        if retDict['target']['timestamp_max'] is not None:
            datetime_max = dateutil.parser.parse(max(retDict['source']['timestamp_max'], retDict['target']['timestamp_max']))  
        else:
            datetime_max = dateutil.parser.parse(retDict['source']['timestamp_max'])
        if retDict['target']['timestamp_min'] is not None:
            datetime_min = dateutil.parser.parse(min(retDict['source']['timestamp_min'], retDict['target']['timestamp_min']))
        else:
            datetime_min = dateutil.parser.parse(retDict['source']['timestamp_min'])
        logging.info("Syncing the Solr servers between overall time interval: start=%s stop= %s " % (datetime_min, datetime_max))
    
        # loop backward one day at a time
        datetime_stop = datetime_max
        datetime_start = datetime_max
        #datetime_start = datetime_max - timedelta(days=1)
        while datetime_stop > datetime_min:
            
            datetime_stop = datetime_start
            datetime_start = datetime_stop - timedelta(seconds=1) # FIXME: use days
            logging.debug("Checking Solr synchronization within time bin from: %s to: %s" % (datetime_start, datetime_stop))
            
            # use specific time limits
            datetime_start_string = datetime_start.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            datetime_stop_string = datetime_stop.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            #datetime_start_string = datetime_start.isoformat()
            #datetime_stop_string = datetime_stop.isoformat()
            #logging.info('Using datetime ISO strings start=%s and stop=%s' % (datetime_start_string, datetime_stop_string))
            timestamp_query = "_timestamp:[%s TO %s]" % (datetime_start_string, datetime_stop_string)
            
            retDict = check_sync(source_solr_base_url, target_solr_base_url, core=core, query=index_query, fq=timestamp_query)
            
            # migrate records source_solr --> target_solr
            if not retDict['status']:
                logging.info("\tMUST EXECUTE SYNCHRONIZATON: between times start=%s stop=%s source counts=%s target counts=%s" % (datetime_start_string, datetime_stop_string, 
                                                                                                                                  retDict['source']['counts'], retDict['target']['counts']))
                
                # first delete all records in timestamp bin from target solr
                delete_query = "(%s)AND(%s)" % (index_query, timestamp_query)
                delete_solr_records(target_solr_base_url, 'datasets', delete_query)
                
                # then migrate records from source solr
                migrate(source_solr_base_url, target_solr_base_url, core='datasets', query=index_query, fq=timestamp_query)
        
def check_sync(source_solr_base_url, target_solr_base_url, core=None, query=DEFAULT_QUERY, fq="_timestamp:[* TO *]"):
    '''
    Method to assert whether two Solr servers are synchronized between the datetimes included in the query, 
    or over all times if no specific datetime query is provided.
    '''
            
    [counts1, timestamp_min1, timestamp_max1, timestamp_mean1] = _query_solr_stats(source_solr_base_url, core, query, fq)
    [counts2, timestamp_min2, timestamp_max2, timestamp_mean2] = _query_solr_stats(target_solr_base_url, core, query, fq)
    
    retDict = { 'source': {'counts':counts1, 'timestamp_min':timestamp_min1, 'timestamp_max':timestamp_max1, 'timestamp_mean':timestamp_mean1 },
                'target': {'counts':counts2, 'timestamp_min':timestamp_min2, 'timestamp_max':timestamp_max2, 'timestamp_mean':timestamp_mean2 }  }
    
    if counts1==counts2 and timestamp_min1 == timestamp_min2 and timestamp_max1==timestamp_max2 and timestamp_mean1 == timestamp_mean2:
        retDict['status'] = True
    else:
        retDict['status'] = False
    
    return retDict
    
def _query_solr_stats(solr_base_url, core, query, fq):
    '''Query Solr stats.
       Note: cannot use solrpy because it crashes on parsing,
       must use standard python HTTP libraries. "'''
    
    solr_url = solr_base_url + '/' + core
    
    # send request
    params = { "q": query,
               "fq": fq,
               "wt":"json",
               "indent":"true",
               "stats":"true",
               "stats.field":["_timestamp"],
               "rows":"0"
              } 
    url = solr_url+"/select?"+urllib.urlencode(params, doseq=True)
    logging.debug("Solr request: %s" % url)
    fh = urllib2.urlopen( url )
    jdoc = fh.read().decode("UTF-8")
    response = json.loads(jdoc)
    
    # parse response
    #logging.debug("Solr Response: %s" % response)
    counts = response['response']['numFound']
    try:
        timestamp_min = response['stats']['stats_fields']['_timestamp']['min']
    except KeyError:
        timestamp_min = None
    try:
        timestamp_max = response['stats']['stats_fields']['_timestamp']['max'] 
    except KeyError:
        timestamp_max = None
    try:
        timestamp_mean = response['stats']['stats_fields']['_timestamp']['mean'] 
    except KeyError:
        timestamp_mean = None
    
    # return output
    return [counts, timestamp_min, timestamp_max, timestamp_mean]

        
if __name__ == '__main__':
    
    #  arguments
    source_solr_base_url = 'http://esgf-node.jpl.nasa.gov/solr'
    target_solr_base_url = 'http://esgf-cloud.jpl.nasa.gov:8983/solr'
    source_index_node = 'esgf-node.jpl.nasa.gov'
    core = CORE_DATASETS
    index_query = 'index_node:%s' % source_index_node

    sync_solrs(source_solr_base_url, target_solr_base_url, core=core, query=index_query)
