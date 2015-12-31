'''
Script to index data into Solr for scaling studies.

@author: Luca Cinquini
'''

from esgfpy.migrate.solr2solr import migrate

if __name__ == '__main__':
    
    sourceSolrUrl = "http://localhost:8983/solr"
    targetSolrUrl = "http://localhost:7000/solr"
    core = "datasets"
    #replace = None
    #replace = "pcmdi9.llnl.gov:esgf-node.jpl.nasa.gov"
    #replace = "pcmdi9.llnl.gov:others"
    
    # total number of records indexed = maxRecords * numIterations * replacements
    maxRecords = 20000    # maximum number of records per migration
    numIterations = 250   # number of migrations
    
    replacements = ["pcmdi9.llnl.gov:esgf-node.jpl.nasa.gov",
                    "pcmdi9.llnl.gov:pcmdi9.llnl.gov",
                    "pcmdi9.llnl.gov:esgf-data.dkrz.de",
                    "pcmdi9.llnl.gov:esgf-node.ipsl.fr",
                    "pcmdi9.llnl.gov:esgf.nccs.nasa.gov",
                    "pcmdi9.llnl.gov:esg2.nci.org.au",
                    "pcmdi9.llnl.gov:esgf-index1.ceda.ac.uk",
                    "pcmdi9.llnl.gov:esgdata.gfdl.nooa.gov",
                    "pcmdi9.llnl.gov:hydra.fsl.noaa.gov",
                    "pcmdi9.llnl.gov:others"]
    
    for replace in replacements:
                
        for i in range(1, 1+numIterations):
            
            # only optimize index after the very last iteration (for each replacement)
            if i==numIterations:
                optimize = True
            else:
                optimize = False
            
            print "Executing iteration #: %s for replacement=%s" % (i, replace)
            suffix = ".%s" % i
            migrate(sourceSolrUrl, targetSolrUrl, core, maxRecords=maxRecords, suffix=suffix, replace=replace, 
                    query='index_node:pcmdi9.llnl.gov', optimize=optimize)