#!/bin/bash

# Script to harvest all records from a remote Solr server to a local Solr server.
# Example invocation:
# ./solr_harvest.sh http://esgdata.gfdl.noaa.gov/solr http://localhost:8983/solr datasets files aggregations

set -e

# parse command line arguments
solr_source_url=$1
shift
solr_target_url=$1
shift
collections="$@"
echo "Harvesting from Solr: ${solr_source_url} to Solr: ${solr_target_url} collections: ${collections}"

# root directory of this source code repository
SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PARENT_DIR="$(dirname $SOURCE_DIR)"
cd $PARENT_DIR

# loop over collections
for collection in $collections
do
  url="${solr_source_url}/${collection}"'/select/?q=*:*&wt=json&rows=0'
  numTotal=`curl -s "$url" | python -c 'import json,sys;obj=json.load(sys.stdin);print obj["response"]["numFound"]'`
  echo ""
  echo "Harvesting collection=${collection} total number of records=$numTotal"
  
  startRecord=0
  maxRecords=100000
  while [ $startRecord -lt $numTotal ]; do
     echo "	Starting record=$startRecord max records=$maxRecords"
     python esgfpy/migrate/solr2solr.py ${solr_source_url} ${solr_target_url} --core ${collection} --start ${startRecord} --max ${maxRecords}
     startRecord=$((startRecord + maxRecords))
  done

done
