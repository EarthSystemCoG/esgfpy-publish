'''
Python module to harvest ALL records from a source Solr to a target Solr
'''

import argparse
import solr
import logging
from esgfpy.migrate.solr2solr import MAX_RECORDS_PER_REQUEST
logging.basicConfig(level=logging.DEBUG)

MAX_RECORDS_PER_SESSION = 100000

from esgfpy.migrate.solr2solr import migrate

def harvest(sourceSolrUrl, targetSolrUrl, core):
    
    # retrieve total number of records at source
    surl = (sourceSolrUrl +"/" + core if core is not None else sourceSolrUrl)
    s1 = solr.Solr(surl)
    response = s1.select("*:*")
    num_found = response.numFound
    logging.info("Total number of records found in source Solr=%s" %  num_found)
    s1.close()

    num_migrated = 0
    while num_migrated < num_found:

        max_records_to_migrate_this_session = min(num_found-num_migrated,
                                                  MAX_RECORDS_PER_SESSION)
        num_migrated_this_session = migrate(sourceSolrUrl, targetSolrUrl,
                                            core=core, start=num_migrated,
                                            maxRecords=max_records_to_migrate_this_session)
        num_migrated += num_migrated_this_session


if __name__ == '__main__':
    
    # parse command line arguments
    parser = argparse.ArgumentParser(description="Migration tool for Solr indexes")
    parser.add_argument('sourceSolrUrl', type=str, help="URL of source Solr (example: http://localhost:8983/solr)")
    parser.add_argument('targetSolrUrl', type=str, help="URL of target Solr (example: http://localhost:8984/solr)")
    parser.add_argument('--core', dest='core', type=str, help="Solr core to harvest (example: --core datasets)", default=None)
    args_dict = vars( parser.parse_args() )
    
    harvest(args_dict['sourceSolrUrl'], args_dict['targetSolrUrl'], args_dict['core'])