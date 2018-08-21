#!/bin/bash

# Script to harvest a remote Solr server
# Exxample invocation from local host:
# ./solr_harvest.sh http://esgf-node.jpl.nasa.gov/solr http://localhost:8983/solr datasets files aggregations

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
  echo "Harvesting collection=${collection}"
  python esgfpy/migrate/solr2solr.py --core ${collection} ${solr_source_url} ${solr_target_url}
done
