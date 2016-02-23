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
TIMEDELTA = timedelta(seconds=1) # FIXME

class Harvester(object):
    '''Class that harvests records from a source Solr server into a target Solr server.'''
    
    def __init__(self, source_solr_base_url, target_solr_base_url):
        
        self.source_solr_base_url = source_solr_base_url
        self.target_solr_base_url = target_solr_base_url

    def sync(self, core=None, query=DEFAULT_QUERY):
        '''Main method to sync from the source Solr to the target Solr.'''
        
        retDict = self._check_sync(core=core, query=query)
        
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
        
            # loop backward one TIMEDELTA at a time
            datetime_stop = datetime_max
            datetime_start = datetime_max
            while datetime_stop > datetime_min:
                
                datetime_stop = datetime_start
                datetime_start = datetime_stop - TIMEDELTA
                logging.debug("Checking Solr synchronization within time bin from: %s to: %s" % (datetime_start, datetime_stop))
                
                # use specific time limits
                datetime_start_string = datetime_start.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                datetime_stop_string = datetime_stop.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                timestamp_query = "_timestamp:[%s TO %s]" % (datetime_start_string, datetime_stop_string)
                
                retDict = self._check_sync(core=core, query=query, fq=timestamp_query)
                
                # migrate records source_solr --> target_solr
                if not retDict['status']:
                    logging.info("\tMUST EXECUTE SYNCHRONIZATON between times start=%s stop=%s source counts=%s target counts=%s" % (datetime_start_string, datetime_stop_string, 
                                                                                                                                      retDict['source']['counts'], retDict['target']['counts']))
                    
                    # first delete all records in timestamp bin from target solr
                    delete_query = "(%s)AND(%s)" % (query, timestamp_query)
                    self._delete_solr_records(target_solr_base_url, 'datasets', delete_query)
                    
                    # then migrate records from source solr
                    migrate(source_solr_base_url, target_solr_base_url, core=CORE_DATASETS, query=query, fq=timestamp_query)
        
    def _check_sync(self, core=None, query=DEFAULT_QUERY, fq="_timestamp:[* TO *]"):
        '''
        Method that asserts whether the source and target Solrs are synchronized between the datetimes included in the query, 
        or over all times if no specific datetime query is provided.
        The method implementation relies on the total number of counts, minimum, maximum and mean of timestamp in the given interval.
        '''
                
        [counts1, timestamp_min1, timestamp_max1, timestamp_mean1] = self._query_solr_stats(self.source_solr_base_url, core, query, fq)
        [counts2, timestamp_min2, timestamp_max2, timestamp_mean2] = self._query_solr_stats(self.target_solr_base_url, core, query, fq)
        
        retDict = { 'source': {'counts':counts1, 'timestamp_min':timestamp_min1, 'timestamp_max':timestamp_max1, 'timestamp_mean':timestamp_mean1 },
                    'target': {'counts':counts2, 'timestamp_min':timestamp_min2, 'timestamp_max':timestamp_max2, 'timestamp_mean':timestamp_mean2 }  }
        
        if counts1==counts2 and timestamp_min1 == timestamp_min2 and timestamp_max1==timestamp_max2 and timestamp_mean1 == timestamp_mean2:
            retDict['status'] = True
        else:
            retDict['status'] = False
        
        return retDict
    
    def _query_solr_stats(self, solr_base_url, core, query, fq):
        '''Method to query the Solr stats.
           Note: cannot use solrpy because it does not work with 'stats'. "'''
        
        # add core ?    
        solr_url = (solr_base_url +"/" + core if core is not None else solr_base_url)
        
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

    def _delete_solr_records(self, solr_base_url, core=None, query=DEFAULT_QUERY):
        
        solr_url = (solr_base_url +"/" + core if core is not None else solr_base_url)
        solr_server = solr.Solr(solr_url)
        solr_server.delete_query(query)
        
if __name__ == '__main__':
    
    # source and target Solrs
    source_solr_base_url = 'http://esgf-node.jpl.nasa.gov/solr'
    target_solr_base_url = 'http://esgf-cloud.jpl.nasa.gov:8983/solr'
    harvester = Harvester(source_solr_base_url, target_solr_base_url)
    
    # specific query to sub-select the records to sync
    source_index_node = 'esgf-node.jpl.nasa.gov'
    core = CORE_DATASETS
    index_query = 'index_node:%s' % source_index_node
    harvester.sync(core=core, query=index_query)
