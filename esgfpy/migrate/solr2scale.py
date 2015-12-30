'''
Script to index data into Solr for scaling studies.

@author: Luca Cinquini
'''

from esgfpy.migrate.solr2solr import migrate

if __name__ == '__main__':
    
    sourceSolrUrl = "http://pcmdi9.llnl.gov/solr"
    targetSolrUrl = "http://localhost:8983/solr"
    core = "datasets"
    #replace = None
    #replace = "pcmdi9.llnl.gov:esgf-node.jpl.nasa.gov"
    #replace = "pcmdi9.llnl.gov:others"
    
    # total number of records indexed = maxRecords * numIterations
    maxRecords = 20000    # maximum number of records per migration
    #numIterations = 20  # number of migrations
    
    for replace in [None, "pcmdi9.llnl.gov:esgf-node.jpl.nasa.gov", "pcmdi9.llnl.gov:others"]:
        
        if replace == "pcmdi9.llnl.gov:others":
            numIterations = 10
        else:
            numIterations = 20 
        
        for i in range(1, 1+numIterations):
            
            print "Executing iteration #: %s for replacement=%s" % (i, replace)
            suffix = ".%s" % i
            migrate(sourceSolrUrl, targetSolrUrl, core, maxRecords=maxRecords, suffix=suffix, replace=replace)