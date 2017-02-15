# sample script that reads a configuration file for ESGF quality control flags
# and publishes the information to the Solr index

import logging
from esgfpy.update.utils import updateSolr
import ConfigParser
import sys
import json

# input: configuration file example:
# [obs4MIPs.NASA-JPL.AIRS.hus.mon.v1|esgf-dev.jpl.nasa.gov]
# obs4mips_indicators_1=green
# obs4mips_indicators_2=green
# obs4mips_indicators_3=green
# obs4mips_indicators_4=green
# obs4mips_indicators_5=yellow
# obs4mips_indicators_6=light_gray

# intermediate output: python dictionary used by this script:
#myDict = {'id:obs4MIPs.NASA-JPL.AIRS.hus.mon.v1|esgf-dev.jpl.nasa.gov': 
#            {'quality_control_flags':['obs4mips_indicators:1:green', 
#                                      'obs4mips_indicators:2:green',
#                                      'obs4mips_indicators:3:green',
#                                      'obs4mips_indicators:4:green',
#                                      'obs4mips_indicators:5:yellow',
#                                      'obs4mips_indicators:6:light_gray']},
#         }

# final output: Solr metadata:
#"quality_control_flags": [
#          "obs4mips_indicators:1:green",
#          "obs4mips_indicators:2:green",
#          "obs4mips_indicators:3:green",
#          "obs4mips_indicators:4:green",
#          "obs4mips_indicators:5:yellow",
#          "obs4mips_indicators:6:light_gray"
#        ],

logging.basicConfig(level=logging.DEBUG)

SOLR_URL = 'http://esgf-dev.jpl.nasa.gov:8984/solr'

myDict = {'id:obs4MIPs.NASA-JPL.AIRS.hus.mon.v1|esgf-dev.jpl.nasa.gov': 
            {'quality_control_flags':['obs4mips_indicators:1:green', 
                                      'obs4mips_indicators:2:green',
                                      'obs4mips_indicators:3:green',
                                      'obs4mips_indicators:4:green',
                                      'obs4mips_indicators:5:yellow',
                                      'obs4mips_indicators:6:light_gray']},
         }

# read indicators from configuration file
config_file = 'obs4mips_indicators_jpl.cfg' # FIXME: use GitHub URL
config = ConfigParser.ConfigParser()
# must set following line explicitly to preserve the case of configuration keys
config.optionxform = str 
myDict = {}
try:
    config.read(config_file)
    for dataset_id in config.sections():
        print 'Dataset id=%s' % dataset_id
        qcflags_list = []
        for key, value in config.items(dataset_id):
            print '\t%s = %s' % (key, value)
            # split the key at the last '_' to extract the numer
            i = key.rfind("_")
            qcflag_name = key[:i]
            qcflag_number = key[i+1:]
            qcflags_list.append( "%s:%s:%s" % (qcflag_name, qcflag_number, value) )
        myDict['id:%s' % dataset_id] = { 'quality_control_flags' : qcflags_list }
except Exception as e:
    print "ERROR reading configuration:"
    print e
    sys.exit(-1)

# publish to Solr
print "Publishing metadata: %s" % myDict
jDict = json.dumps(myDict)
print jDict
updateSolr(myDict, update='set', solr_url=SOLR_URL, solr_core='datasets')

