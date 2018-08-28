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
  #numTotal=`curl -s 'http://esgdata.gfdl.noaa.gov/solr/'${collection}'/select/?q=*:*&wt=json&rows=0' | python -c 'import json,sys;obj=json.load(sys.stdin);print obj["response"]["numFound"]'`
  url="${solr_source_url}/${collection}"'/select/?q=*:*&wt=json&rows=0'
  echo $url
  numTotal=`curl -s "$url" | python -c 'import json,sys;obj=json.load(sys.stdin);print obj["response"]["numFound"]'`
  echo "Harvesting collection=${collection} total number of records=$numTotal"
  
  start=0
  maxRecords=10
  while [ $start -lt $numTotal ]; do
     echo "\tStarting record=$start max records=$maxRecords"
     
     start=$((start + maxRecords))
  done

  #python esgfpy/migrate/solr2solr4all.py ${solr_source_url} ${solr_target_url} --core ${collection}
done
