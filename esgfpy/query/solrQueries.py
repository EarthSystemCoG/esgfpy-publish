'''
Script for benchmarking queries to Solr servers
'''

import solr

# Solr servers
SERVERS = ['esgf-node.jpl.nasa.gov', 'esgf-cloud1.jpl.nasa.gov']

# Solr queries
QUERIES = { 'datasets': [ ['*:*'], # all records
                          ['project:CMIP5', 'experiment:rcp60', 'replica:false', 'latest:true'], # 4-facet constraint
                          ['project:CMIP5', 'experiment:rcp60', 'replica:false', 'latest:true',  # 8-facet constraint
                           'cf_standard_name:mole_concentration_of_calcite_expressed_as_carbon_in_sea_water',
                           'data_node:aims3.llnl.gov', 'time_frequency:yr', 'institute:MIROC' ],
                          ["datetime_stop:[1992-10-16T12:00:00Z TO *]", "datetime_start:[* TO 2010-12-16T12:00:00Z]" ], # range query 
                          ['id:cordex.output.NAM-44.DMI.ICHEC-EC-EARTH.historical.r3i1p1.HIRHAM5.v1.day.tasmin.v20131108|cordexesg.dmi.dk'], # specific record
                          ['id:*MIROC*'], # regular expression
                        ],
            'files': [ ['checksum:4c67977c00d32c2b8341df21be5bce1261beb34f33b5e903edc166dd3e9ce346'], # checksum
                       ['dataset_id:cordex.output.NAM-44.DMI.ICHEC-EC-EARTH.historical.r3i1p1.HIRHAM5.v1.day.tasmin.v20131108|cordexesg.dmi.dk' ], # all files in a dataset
                     ]
      }

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
    
    # loop over queries
    for core, queries in QUERIES.items():
        for ifq, fq in enumerate(queries):
    
            # loop over servers
            for iserver, server in enumerate(SERVERS):
                
                # Solr URL: server + core
                s = solr.Solr('http://%s:8983/solr/%s' % (server, core))
                                                    
                # append random number query to avoid caching of results
                #fq.append('text:%s' % randint(0,1000))
                # disable query filter caching
                #fq = [ "{!cache=false}%s" % x for x in fq ]
                                                  
                if iserver==0:
                    
                    #solr_ports = [8983, 8985, 8986, 8987, 8988, 8989, 8990 ]
                    solr_ports = [8983] + range(8985, 8990+1) 
                    shards = ",".join( ["localhost:%s/solr/%s" % (p, core) for p in solr_ports])
                    
                    # esgf-node: distributed shards search
                    response = s.select('*:*', fq=fq, shards=shards)
                else:
                    # esgf-cloud1: internal cloud search
                    response = s.select('*:*', fq=fq)
                    
                print 'Querying: %s for: %s' % (s.url, fq)
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
