'''
Holds contsnats common to this package.

@author: Luca Cinquini
'''

SOLR_HOST = "esgf-node.jpl.nasa.gov"
FQ1 = ["project:obs4MIPs","quality_control_flags:*"]
FQ2 = ["project:obs4MIPs","-quality_control_flags:[* TO *]"]
NUMROWS=1000

FIELDS = ["id","index_node","data_node","variable","quality_control_flags"]
# [8983, 8985, 8986, 8987, 8988, 8989, 8990, 8991, 8992, 8993]
SOLR_PORTS = [8983] + range(8985, 8993+1) 
TEXT_FILE = "obs4mips_indicators.txt"
JSON_FILE = "obs4mips_indicators.json"

#INDICATOR_MAP = { "white":"0", "yellow":"1", "green":"2", "orange":"3", "red":"4", "light_gray":"5" }
#REVERSE_INDICATOR_MAP = { i[1]:i[0] for i in INDICATOR_MAP.items()}
