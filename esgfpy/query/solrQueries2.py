'''
Script for benchmarking queries to Solr servers
'''

import solr
from random import randint

# Solr cores
CORES = ['datasets', 'files']

# Solr servers
SERVERS = ['esg-node.jpl.nasa.gov', 'esgf-cloud1.jpl.nasa.gov']

# Solr queries
FQS = [ ['*:*'], 
        ['project:CMIP5'], 
        ['checksum:2891ad8d750cef035304dcad5c598e84'],
        ['time_frequency:mon','product:observations','institute:NASA-JPL'],
        ["datetime_stop:[1992-10-16T12:00:00Z TO *]", "datetime_start:[* TO 2010-12-16T12:00:00Z]" ],
      ]

OUTPUT_FILE = './solr-queries.txt'
SEPARATOR = ","

def get_shards(core):
    '''Builds list of localhost shards for a given core.'''
    
    #solr_ports = [8983, 8985, 8986, 8987, 8988, 8989, 8990, 8991, 8992, 8993, 8994, 8995, 8996, 8997, 8998, 8999, 9000 ]
    solr_ports = [8983] + range(8985, 9000+1) 
    shards = ",".join( ["localhost:%s/solr/%s" % (p, core) for p in solr_ports])
    
    return shards

if __name__ == '__main__':
    
    # 1) query "esg-datanode" for for datasets
    start = randint(0, 100000)
    s1d = solr.Solr('http://esg-datanode.jpl.nasa.gov:8983/solr/datasets')
    print "Querying: %s starting at: %s" % (s1d.url, start)
    response = s1d.select('*:*', fq=['project:CMIP5'], shards=get_shards('datasets'), start=start, rows=10)
    print "Query time: %s number of results: %s" % (response.header['QTime'], response.numFound)
    dataset_ids = []
    for doc in response.results:
        print "Found dataset: %s" % doc['id']
        dataset_ids.append( doc['id'] )
    
    # 2) query "esg-datanode" and "esgf-cloud1" for files
    s1f = solr.Solr('http://esg-datanode.jpl.nasa.gov:8983/solr/files')
    s2f = solr.Solr('http://esgf-cloud1.jpl.nasa.gov:8983/solr/files')
    for dataset_id in dataset_ids:
        
        print "Querying: %s for dataset_id: %s" % (s1f.url, dataset_id)
        response = s1f.select('*:*', fq=['dataset_id:%s' % dataset_id], shards=get_shards('files'))
        print "Query time: %s number of results: %s" % (response.header['QTime'], response.numFound)

        print "Querying: %s for dataset_id: %s" % (s2f.url, dataset_id)
        response = s2f.select('*:*', fq=['dataset_id:%s' % dataset_id])
        print "Query time: %s number of results: %s" % (response.header['QTime'], response.numFound)
    
    s1d.close()
    s1f.close()
    
    
    
