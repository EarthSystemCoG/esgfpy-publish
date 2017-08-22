'''
Python script that queries the Solr indexes and generates a text-file of obs4MIPs datasets.

@author: Luca Cinquini
'''
import solr
from constants import SOLR_HOST, FQ1, FQ2, NUMROWS, FIELDS, SOLR_PORTS, TEXT_FILE

def obs4mips_query():
    
    # dictionary of all obs4MIPs datasets
    datasets = {}
        
    # Solr server and shards
    s = solr.Solr('http://%s:8983/solr/datasets' % SOLR_HOST)
    shards = ",".join( ["localhost:%s/solr/datasets" % p for p in SOLR_PORTS])
    
    # 1) query for datasets WITH quality control flags
    response = s.select('*:*', fq=FQ1, shards=shards, fields=FIELDS, rows=NUMROWS)
    print "Query=%s number of results found=%s" % (FQ1, response.numFound)
    for result in response.results:
        print "Dataset id=%s qcf=%s" % (result["id"], result["quality_control_flags"])
        # convert [u'obs4mips_indicators:1:green', u'obs4mips_indicators:2:green', u'obs4mips_indicators:3:green', 
        #          u'obs4mips_indicators:4:green', u'obs4mips_indicators:5:yellow', u'obs4mips_indicators:6:light_gray']
        # to [0,1,2,3,4,5]
        qcflags = []
        for qcf in result["quality_control_flags"]:
            parts = qcf.split(":")
            qcflags.append(parts[2])
        datasets[ result["id"].lower() ] = [result["id"], result["index_node"], result["data_node"], [x for x in qcflags]]
        
    # 2) query for datasets WITHOUT quality control flags
    response = s.select('*:*', fq=FQ2, shards=shards, fields=FIELDS, rows=NUMROWS)
    print "Query=%s number of results found=%s" % (FQ2, response.numFound)
    for result in response.results:
        print "Dataset id=%s" % result["id"]
        qcflags = ["white", "white", "white", "white", "white", "white"]
        
        datasets[ result["id"].lower() ] = [result["id"], result["index_node"], result["data_node"], [x for x in qcflags]]

    return datasets
        
def obs4mips_write_text_file(datasets):
    
    with open(TEXT_FILE, 'w') as the_file: 
        
        # write header row
        the_file.write("# dataset_id\tindex_node\tdata_node\tindicator_1\tindicator_2\tindicator_3\tindicator_4\tindicator_5\tindicator_6\n")
        for key in sorted(datasets):
            values = datasets[key]
            the_file.write("\t".join( [values[0], values[1], values[2]] + [v for v in values[3]] ) + "\n")
    

if __name__ == '__main__':
    
    datasets = obs4mips_query()
    obs4mips_write_text_file(datasets)