'''
Created on Feb 22, 2016

@author: cinquini
'''

import logging
logging.basicConfig(level=logging.INFO)

import solr
import urllib, urllib2
import json
import dateutil.parser
from datetime import timedelta
from monthdelta import monthdelta
from esgfpy.migrate.solr2solr import migrate

DEFAULT_QUERY = "*:*"

CORE_DATASETS = 'datasets'
CORE_FILES = 'files'
CORE_AGGREGATIONS = 'aggregations'
#CORES = [CORE_DATASETS, CORE_FILES, CORE_AGGREGATIONS]
CORES = [CORE_DATASETS, CORE_FILES] # FIXME

TIMEDELTA_MONTH = monthdelta(1)
TIMEDELTA_DAY = timedelta(days=1) 
TIMEDELTA_HOUR = timedelta(hours=1) 

class Harvester(object):
    '''Class that harvests records from a source Solr server into a target Solr server.'''
    
    def __init__(self, source_solr_base_url, target_solr_base_url):
        
        self.source_solr_base_url = source_solr_base_url
        self.target_solr_base_url = target_solr_base_url

    def sync(self, query=DEFAULT_QUERY):
        '''Main method to sync from the source Solr to the target Solr.'''
        
        # flag to trigger commit/harvest
        synced = False
        numRecordsSynced = { CORE_DATASETS:0, CORE_FILES:0, CORE_AGGREGATIONS: 0}
        
        # loop over cores
        for core in CORES:
            
            retDict = self._check_sync(core=core, query=query)
            
            if retDict['status']:
                logging.info("Solr cores '%s' are in sync, no further action necessary" % core)
                
            else:
                
                # must issue commit/optimize before existing
                synced = True
                
                # 0) full datetime interval to synchronize
                (datetime_min, datetime_max) = self._get_sync_datetime_interval(retDict)
                logging.info("SYNCING: CORE=%s start=%s stop=%s # records=%s --> %s" % (core, datetime_min, datetime_max,
                             retDict['source']['counts'], retDict['target']['counts']))
            
                # 1) loop over MONTHS
                datetime_stop_month = datetime_max
                datetime_start_month = datetime_max
                while datetime_stop_month > (datetime_min + TIMEDELTA_MONTH):
                    
                    datetime_stop_month = datetime_start_month
                    datetime_start_month = datetime_stop_month - TIMEDELTA_MONTH  
                    logging.info("\tMONTH check: start=%s stop=%s" % (datetime_start_month, datetime_stop_month))

                    # use specific time limits
                    datetime_start_mpnth_string = datetime_start_month.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                    datetime_stop__month_string = datetime_stop_month.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                    timestamp_query_month = "_timestamp:[%s TO %s]" % (datetime_start_mpnth_string, datetime_stop__month_string)

                    retDict = self._check_sync(core=core, query=query, fq=timestamp_query_month)
                        
                    # migrate records source_solr --> target_solr
                    if not retDict['status']:
                        logging.info("\tMONTH sync=%s start=%s stop=%s # records=%s --> %s" % (core, datetime_start_month, datetime_stop_month,
                                     retDict['source']['counts'], retDict['target']['counts']))

                
                        # 2) loop over DAYS
                        datetime_stop_day = datetime_stop_month
                        datetime_start_day = datetime_stop_month
                        while datetime_stop_day > (datetime_start_month + TIMEDELTA_DAY):
                            
                            datetime_stop_day = datetime_start_day
                            datetime_start_day = datetime_stop_day - TIMEDELTA_DAY  
                            logging.info("\t\tDAY check: start=%s stop=%s" % (datetime_start_day, datetime_stop_day))
                          
                            # use specific time limits
                            datetime_start_day_string = datetime_start_day.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                            datetime_stop_day_string = datetime_stop_day.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                            timestamp_query_day = "_timestamp:[%s TO %s]" % (datetime_start_day_string, datetime_stop_day_string)
                            
                            retDict = self._check_sync(core=core, query=query, fq=timestamp_query_day)
                            
                            # migrate records source_solr --> target_solr
                            if not retDict['status']:
                                logging.info("\t\tDAY sync=%s start=%s stop=%s # records=%s --> %s" % (core, datetime_start_day, datetime_stop_day,
                                             retDict['source']['counts'], retDict['target']['counts']))
                                
                                # 3) loop over HOURS
                                datetime_stop_hour = datetime_stop_day
                                datetime_start_hour = datetime_stop_day
                                while datetime_stop_hour > (datetime_start_day + TIMEDELTA_HOUR):
                                    
                                    datetime_stop_hour = datetime_start_hour
                                    datetime_start_hour = datetime_stop_hour - TIMEDELTA_HOUR
                                    logging.info("\t\t\tHOUR check start=%s stop=%s" % (datetime_start_hour, datetime_stop_hour))
                                    
                                    # use specific time limits
                                    datetime_start_hour_string = datetime_start_hour.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                                    datetime_stop_hour_string = datetime_stop_hour.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                                    timestamp_query_hour = "_timestamp:[%s TO %s]" % (datetime_start_hour_string, datetime_stop_hour_string)
                                    
                                    retDict = self._check_sync(core=core, query=query, fq=timestamp_query_hour)
                                    
                                    # migrate records source_solr --> target_solr
                                    if not retDict['status']:
                                        logging.info("\t\t\tHOUR sync=%s start=%s stop=%s # records=%s --> %s" % (core, datetime_start_hour, datetime_stop_hour,
                                                     retDict['source']['counts'], retDict['target']['counts']))
                                        
                                        numRecordsSynced[core] += self._sync_records(core, query, timestamp_query_hour)
                                        
                                        # check DAY sync again to determine whether the hour loop can be stopped
                                        retDict = self._check_sync(core=core, query=query, fq=timestamp_query_day)
                                        if retDict['status']:
                                            logging.info("\t\tSolr servers are now in sync for DAY: %s" % timestamp_query_day)
                                            break # out of the HOUR bin loop
                                
                                # check MONTH sync again to determine whether the day loop can be stopped
                                retDict = self._check_sync(core=core, query=query, fq=timestamp_query_month)
                                if retDict['status']:
                                    logging.info("\tSolr servers are now in sync for MONTH: %s" % timestamp_query_month)
                                    break # out of the DAY bin loop
                            
                            # check FULL sync again to determine whether the day loop can be stopped
                            retDict = self._check_sync(core=core, query=query)
                            if retDict['status']:
                                logging.info("Solr servers are now in sync for FULL DATETIME INTERVAL")
                                break # out of the MONTH bin loop

                        
        # if any synchronization took place
        if synced:
            
            # commit changes and optimize the target index
            self._commit_solr(self.target_solr_base_url)
            self._optimize_solr(self.target_solr_base_url)
        
            # check status before existing    
            for core in CORES:
                logging.info("Core=%s number of records migrated=%s" % (core, numRecordsSynced[core]))
                retDict = self._check_sync(core=core, query=query)
                logging.info("Core=%s sync status=%s number of source records=%s number of target records=%s" % (core, retDict['status'], 
                                                                                                                 retDict['source']['counts'],
                                                                                                                 retDict['target']['counts']))
            
     
    def _get_sync_datetime_interval(self, retDict):
        '''Method to compute the full datetime interval over which to wynchornize the two Solrs.'''
        
        # use largest possible datetime interval
        if retDict['target']['timestamp_max'] is not None:
            datetime_max = dateutil.parser.parse(max(retDict['source']['timestamp_max'], retDict['target']['timestamp_max']))  
        else:
            datetime_max = dateutil.parser.parse(retDict['source']['timestamp_max'])
        if retDict['target']['timestamp_min'] is not None:
            datetime_min = dateutil.parser.parse(min(retDict['source']['timestamp_min'], retDict['target']['timestamp_min']))
        else:
            datetime_min = dateutil.parser.parse(retDict['source']['timestamp_min'])
        
        # enlarge [datetime_min, datetime_max] to an integer number of months
        datetime_max = datetime_max + TIMEDELTA_MONTH
        datetime_max = datetime_max.replace(day=1, hour=0, minute=0, second=0, microsecond=0) # beginning of month after datetime_max
        datetime_min = datetime_min.replace(day=1, hour=0, minute=0, second=0, microsecond=0) # beginning of datetime_min month

        return (datetime_min, datetime_max)

        
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
    
    def _sync_records(self, core, query, timestamp_query):
        '''Method that performs the actual synchronization of records within a given time interval.'''
        
        # first delete all records in timestamp bin from target solr
        # will NOT commit the changes yet
        delete_query = "(%s)AND(%s)" % (query, timestamp_query)
        self._delete_solr_records(target_solr_base_url, core, delete_query)
        
        # then migrate records from source solr
        # do NOT commit changes untill all cores are processed
        # do NOT optimize the index yet
        numRecords = migrate(source_solr_base_url, target_solr_base_url, core, query=query, fq=timestamp_query,
                             commit=False, optimize=False)
        logging.info("\t\t\tNumber or records migrated=%s" % numRecords)
        return numRecords


    def _delete_solr_records(self, solr_base_url, core=None, query=DEFAULT_QUERY):
        
        solr_url = (solr_base_url +"/" + core if core is not None else solr_base_url)
        solr_server = solr.Solr(solr_url)
        solr_server.delete_query(query)
        solr_server.close()
        
    def _optimize_solr(self, solr_base_url):
        
        for core in CORES:
        
            solr_url = solr_base_url +"/" + core
            logging.info("Optimizing Solr index: %s" % solr_url)
            solr_server = solr.Solr(solr_url)
            solr_server.optimize()
            solr_server.close()
            
    def _commit_solr(self, solr_base_url):
        
        for core in CORES:
        
            solr_url = solr_base_url +"/" + core
            logging.info("Committing to Solr index: %s" % solr_url)
            solr_server = solr.Solr(solr_url)
            solr_server.commit()
            solr_server.close()
        
if __name__ == '__main__':
    
    # source and target Solrs
    source_solr_base_url = 'http://pcmdi.llnl.gov/solr'
    #source_solr_base_url = 'http://esgf-node.jpl.nasa.gov/solr'
    target_solr_base_url = 'http://esgf-cloud.jpl.nasa.gov:8983/solr'
    harvester = Harvester(source_solr_base_url, target_solr_base_url)
    
    # specific query to sub-select the records to sync
    source_index_node = 'pcmdi.llnl.gov'
    #source_index_node = 'esgf-node.jpl.nasa.gov'
    
    index_query = 'index_node:%s' % source_index_node
    harvester.sync(query=index_query)
