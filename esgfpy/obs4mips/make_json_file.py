'''
Python script that parses the text file and creates the JSON file.

Example:
{
    "id:obs4mips.NASA-JPL.AIRS.ta.mon.v20110608|esgf-data.jpl.nasa.gov": {
        "quality_control_flags": ["obs4mips_indicators:1:green", 
                                  "obs4mips_indicators:2:green", 
                                  "obs4mips_indicators:3:green", 
                                  "obs4mips_indicators:4:green", 
                                  "obs4mips_indicators:5:yellow", 
                                  "obs4mips_indicators:6:light_gray"
                                  ]
    },
    "id:obs4mips.CNES.AVISO.zos.mon.v20110829|esgf-data.jpl.nasa.gov": {
        "quality_control_flags": ["obs4mips_indicators:1:green", 
                                  "obs4mips_indicators:2:green", 
                                  "obs4mips_indicators:3:green", 
                                  "obs4mips_indicators:4:green", 
                                  "obs4mips_indicators:5:yellow", 
                                  "obs4mips_indicators:6:light_gray"
                                  ]
    }
}

@author: cinquini
'''

from constants import TEXT_FILE, JSON_FILE
import json

# indexes of important fields inside text file (starting at 0)
iDatasetId = 8
iIndicator1 = 2
iIndicator6 = 7

def obs4mips_read_text_file(index_node):
    
    # dictionary of obs4MIPs datasets
    
    datasets = {}
    
    # read file into dictionary
    with open(TEXT_FILE, 'r') as the_file:
        lines = the_file.readlines()
        
    for line in lines:
        # variables     time_frequency  indicator_1     indicator_2     indicator_3     indicator_4     indicator_5     indicator_6     dataset_id      index_node      data_node
        if not line.startswith("#"): # skip header
            parts = line.rstrip('\n').split("\t")
            # write all, or match a specific index node
            if index_node is None or index_node == parts[-2]:
              quality_control_flags = []
              for i in range(iIndicator1,iIndicator6+1):
                quality_control_flags.append("obs4mips_indicators:%s:%s" % (i-iIndicator1+1, parts[i] ) )
              datasets[ "id:%s" % parts[iDatasetId] ] = {"quality_control_flags": quality_control_flags }
    
    return datasets

def obs4mips_write_json_file(datasets):
    
    jdata = json.dumps(datasets, encoding="UTF-8", indent=4, sort_keys=True)
    
    with open(JSON_FILE, 'w') as the_file:
        the_file.write(jdata)


if __name__ == '__main__':
    #index_node = None
    index_node = "esgf-node.jpl.nasa.gov"
    datasets = obs4mips_read_text_file(index_node)
    obs4mips_write_json_file(datasets)
