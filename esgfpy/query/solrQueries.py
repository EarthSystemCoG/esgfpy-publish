'''
Script for benchmarking queries to Solr servers
'''

import solr
#from random import randint

# Solr cores
CORES = ['datasets', 'files']

# Solr servers
SERVERS = ['esg-datanode.jpl.nasa.gov', 'esgf-cloud1.jpl.nasa.gov']

# Solr queries
FQS = [ ['*:*'], 
        ['project:CMIP5'], 
        ['checksum:2891ad8d750cef035304dcad5c598e84'],
        ['time_frequency:mon','product:observations','institute:NASA-JPL'],
        ["datetime_stop:[1992-10-16T12:00:00Z TO *]", "datetime_start:[* TO 2010-12-16T12:00:00Z]" ],
      ]

OUTPUT_FILE = './solr-queries.txt'
SEPARATOR = ","


if __name__ == '__main__':
    
    # open output file
    f = open(OUTPUT_FILE,"w")
    
    # write header line  
    headers = ['server', 'core', 'iq', 'query', 'QTime', 'numFound']
    f.write("# ")         
    for h in headers:
        f.write(h)
        f.write(SEPARATOR)
    f.write("\n")
    
    # loop over servers
    for iserver, server in enumerate(SERVERS):
        
        # loop over cores
        for core in CORES:
        
            # Solr URL: server + core
            s = solr.Solr('http://%s:8983/solr/%s' % (server, core))
            
            #solr_ports = [8983, 8985, 8986, 8987, 8988, 8989, 8990, 8991, 8992, 8993, 8994, 8995, 8996, 8997, 8998, 8999, 9000, 9001 ]
            solr_ports = [8983] + range(8985, 9002) 
            shards = ",".join( ["localhost:%s/solr/%s" % (p, core) for p in solr_ports])
            
            # loop over queries
            for ifq, fq in enumerate(FQS):
                            
                # append random number query to avoid caching of results
                #fq.append('text:%s' % randint(0,1000))
                # disable query filter cachning
                #fq = [ "{!cache=false}%s" % x for x in fq ]
                                                  
                if iserver==0:
                    # esg-datanode: distributed shards searcg
                    response = s.select('*:*', fq=fq, shards=shards)
                else:
                    # esgf-cloud1: internal cloud search
                    response = s.select('*:*', fq=fq)
                    
                print 'Qurying: %s for: %s' % ( s.url, fq)
                print "\ttime: %s numFound: %s" % (response.header['QTime'], response.numFound)
                
                # write entry
                f.write("%s" % server)
                f.write(SEPARATOR)
                f.write("%s" % core)
                f.write(SEPARATOR)
                f.write("%s" % (ifq+1))
                f.write(SEPARATOR)
                f.write("%s" % fq)
                f.write(SEPARATOR)
                f.write("%s" % response.header['QTime'])
                f.write(SEPARATOR)
                f.write("%s" % response.numFound)
                f.write(SEPARATOR)      
                f.write("\n")
                
            # close Solr connection
            s.close()
    
    # close output file
    f.close()
