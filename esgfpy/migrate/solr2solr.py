'''
Created on Dec 19, 2014

@author: cinquini
'''

import solr
import logging
import datetime
import argparse

MAX_RECORDS_PER_REQUEST = 100
DEFAULT_QUERY='*:*'
logging.basicConfig(level=logging.DEBUG)

def migrate(sourceSolrUrl, targetSolrUrl, core=None, query=DEFAULT_QUERY, start=0):
    
    surl = (sourceSolrUrl +"/" + core if core is not None else sourceSolrUrl)
    turl = (targetSolrUrl +"/" + core if core is not None else targetSolrUrl)
        
    t1 = datetime.datetime.now()
        
    s1 = solr.Solr(surl)
    s2 = solr.Solr(turl)

    numFound = start+1
    while start < numFound:
                
        # try migrating MAX_RECORDS_PER_REQUEST records at once
        try:
            (_numFound, _numRecords) = _migrate(s1, s2, query, core, start, MAX_RECORDS_PER_REQUEST)
            numFound = _numFound
            start += _numRecords
    
        # migrate 1 record at a time
        except:
            for i in range(MAX_RECORDS_PER_REQUEST):
                if start < numFound:
                    try:
                        (_numFound, _numRecords) = _migrate(s1, s2, query, core, start, 1)
                    except Exception as e:
                        print 'ERROR: %s' % e
                    start += 1
                    

               
        
    # optimize full index
    s2.optimize()
    
    # close connections
    s1.close()
    s2.close()
    
    t2 = datetime.datetime.now()
    logging.info("Total number of records migrated: %s" % start)
    logging.info("Total elapsed time: %s" % (t2-t1))
    
def _migrate(s1, s2, query, core, start, howManyMax):
    '''Migrates 'howManyMax' records starting at 'start'.'''
    
    logging.debug("Request: start record=%s max records per request=%s" % (start, howManyMax) )
    
    response = s1.select(query, start=start, rows=howManyMax)
    _numFound = response.numFound
    _numRecords = len(response.results)
    
    # FIX broken dataset records
    if core=='datasets':
        for result in response.results:
            for field in ['height_bottom', 'height_top']:
                value = result.get(field, None)
                if value:
                    try:
                        result[field] = float(value)
                    except ValueError:
                        result[field] = 0.
    
    s2.add_many(response.results, commit=True)
    
    logging.debug("Response: current number of records=%s total number of records=%s" % (start+_numRecords, _numFound))
    return (_numFound, _numRecords)
    
    

if __name__ == '__main__':
    
    # parse command line arguments
    parser = argparse.ArgumentParser(description="Migration tool for Solr indexes")
    parser.add_argument('sourceSolrUrl', type=str, help="URL of source Solr (example: http://localhost:8983/solr)")
    parser.add_argument('targetSolrUrl', type=str, help="URL of target Solr (example: http://localhost:8984/solr)")
    parser.add_argument('--core', dest='core', type=str, help="URL of target Solr (example: --core datasets)", default=None)
    parser.add_argument('--query', dest='query', type=str, help="Optional query to sub-select records (example: --query project:xyz)", default=DEFAULT_QUERY)
    parser.add_argument('--start', dest='start', type=int, help="Optional first record to be migrated (example: --start 1000000)", default=0)
    args_dict = vars( parser.parse_args() )
    
    # execute migration
    migrate(args_dict['sourceSolrUrl'], args_dict['targetSolrUrl'], 
            core=args_dict['core'], query=args_dict['query'], start=args_dict['start'])