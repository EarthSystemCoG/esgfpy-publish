'''
Script to index data into Solr for scaling studies.

@author: Luca Cinquini
'''

from esgfpy.migrate.solr2solr import migrate

if __name__ == '__main__':
    
    sourceSolrUrl = "http://pcmdi9.llnl.gov/solr"
    targetSolrUrl = "http://localhost:8983/solr"
    core = "datasets"
    replace = None
    #replace = "pcmdi9.llnl.gov:esgf-node.jpl.nasa.gov"
    #replace = "pcmdi9.llnl.gov:others"
    
    # total number of records indexed = maxRecords * numIterations
    maxRecords = 150    # maximum number of records per migration
    numIterations = 10  # number of migrations
    
    for i in range(1, 1+numIterations):
        
        suffix = ".%s" % i
        migrate(sourceSolrUrl, targetSolrUrl, core, maxRecords=maxRecords, suffix=suffix, replace=replace)