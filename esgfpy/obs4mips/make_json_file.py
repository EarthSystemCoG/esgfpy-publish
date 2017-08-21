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

from constants import TEXT_FILE, REVERSE_INDICATOR_MAP, JSON_FILE
import json

def obs4mips_read_text_file():
    
    # dictionary of obs4MIPs datasets
    
    datasets = {}
    
    # read file into dictionary
    with open(TEXT_FILE, 'r') as the_file:
        lines = the_file.readlines()
        
    for line in lines:
        if not line.startswith("#"): # skip header
            parts = line.rstrip('\n').split("\t")
            quality_control_flags = []
            for i in range(1,7):
                quality_control_flags.append("obs4mips_indicators:%s:%s" % (i, REVERSE_INDICATOR_MAP[ parts[i+2] ] ) )
            datasets[ "id:%s" % parts[0] ] = {"quality_control_flags": quality_control_flags }
    
    return datasets

def obs4mips_write_json_file(datasets):
    
    jdata = json.dumps(datasets, encoding="UTF-8", indent=4, sort_keys=True)
    
    with open(JSON_FILE, 'w') as the_file:
        the_file.write(jdata)


if __name__ == '__main__':
    datasets = obs4mips_read_text_file()
    obs4mips_write_json_file(datasets)