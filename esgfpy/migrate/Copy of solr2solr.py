'''
Created on Dec 19, 2014

@author: cinquini
'''

import solr
import logging
import datetime

MAX_RECORDS = 100
logging.basicConfig(level=logging.DEBUG)

if __name__ == '__main__':
    
    t1 = datetime.datetime.now()
    
    
    sourceSolrUrl = 'http://esg-datanode.jpl.nasa.gov:8983/solr'
    targetSolrUrl = 'http://localhost:8080/solr'
    core = 'datasets'
    
    
    s1 = solr.Solr(sourceSolrUrl+"/"+core)
    s2 = solr.Solr(targetSolrUrl+"/"+core)

    numRecords = 0
    numFound = 1
    while numRecords < numFound:
        logging.debug("Request: start=%s rows=%s" % (numRecords, MAX_RECORDS) )
        response = s1.select('*:*', start=numRecords, rows=MAX_RECORDS)
        numFound = response.numFound
        numRecords += len(response.results)
        logging.debug("Response: current=%s total=%s" % (numRecords, numFound))
        s2.add_many(response.results, commit=True)

    # optinmize full index
    s2.optimize()
    
    # close connections
    s1.close()
    s2.close()
    
    t2 = datetime.datetime.now()
    logging.info("Total elapsed time: %s" % (t2-t1))