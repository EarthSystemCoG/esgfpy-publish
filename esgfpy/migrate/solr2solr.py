'''
Created on Dec 19, 2014

@author: cinquini
'''

import solr
import logging
import datetime
import argparse

MAX_RECORDS_PER_REQUEST = 100
MAX_RECORDS_TOTAL = 9999999 # total maxRecords number of records to be migrated
DEFAULT_QUERY='*:*'
logging.basicConfig(level=logging.DEBUG)

def migrate(sourceSolrUrl, targetSolrUrl, core=None, query=DEFAULT_QUERY, fq=None,
            start=0, maxRecords=MAX_RECORDS_TOTAL, 
            replace=None, suffix='', commit=True, optimize=True):
    '''
    By default, it commits the changes and optimizes the index.
    '''
    
    surl = (sourceSolrUrl +"/" + core if core is not None else sourceSolrUrl)
    turl = (targetSolrUrl +"/" + core if core is not None else targetSolrUrl)
    
    replacements = {}
    if replace is not None and len(replace)>0:
        replaces = replace.split(":\s+")
        for _replace in replaces:
            (oldValue, newValue) = _replace.split(':')
            replacements[oldValue] = newValue
            logging.debug("Replacing metadata: %s --> %s" % (oldValue, newValue))
        
    t1 = datetime.datetime.now()
        
    s1 = solr.Solr(surl)
    s2 = solr.Solr(turl)

    numRecords = 0 # number of records migrated so far <= maxRecords
    numFound = start+1
    while start < numFound and numRecords < maxRecords:
                
        # try migrating MAX_RECORDS_PER_REQUEST records at once
        try:
            _maxRecords = min(maxRecords-numRecords, MAX_RECORDS_PER_REQUEST) # do NOT migrate more records than this number
            (_numFound, _numRecords) = _migrate(s1, s2, query, fq, core, start, _maxRecords, replacements, suffix)
            numFound = _numFound
            start += _numRecords
            numRecords += _numRecords
    
        # migrate 1 record at a time
        except:
            for i in range(MAX_RECORDS_PER_REQUEST):
                if start < numFound and numRecords < maxRecords:
                    try:
                        (_numFound, _numRecords) = _migrate(s1, s2, query, fq, core, start, 1, replacements, suffix)
                    except Exception as e:
                        logging.warn('ERROR: %s' % e)
                    start += 1
                    numRecords += 1
        
    # optimize full index (optimize=True implies commit=True)
    if optimize:
        logging.info("Optimizing the index...")
        s2.optimize()
        logging.info("...done")
    # just commit the changes but do not optimize
    elif commit:
        logging.info("Committing changes to the index...")
        s2.commit()
        logging.info("...done")
    
    # close connections
    s1.close()
    s2.close()
    
    t2 = datetime.datetime.now()
    logging.debug("Total number of records migrated: %s" % numRecords)
    logging.debug("Total elapsed time: %s" % (t2-t1))
    
    return numRecords
    
def _migrate(s1, s2, query, fq, core, start, howManyMax, replacements, suffix):
    '''
    Migrates 'howManyMax' records starting at 'start'.
    By default, it commits the changes after this howManyRecords have been migrated,
    but does NOT optimize the index.
    '''
    
    logging.debug("Request: start record=%s max records per request=%s" % (start, howManyMax) )
    
    fquery=[] # empty
    if fq is not None:
        fquery = [fq]
    logging.info("Querying: query=%s start=%s rows=%s fq=%s" % (query, start, howManyMax, fquery))
    response = s1.select(query, start=start, rows=howManyMax, fq=fquery)
    _numFound = response.numFound
    _numRecords = len(response.results)
    logging.info("Query returned numFound=%s" % _numFound)
    
    # process records
    for result in response.results:
        
        # remove "_version_" field otherwise Solr will return an HTTP 409 error (Conflict)
        # by design, "_version_" > 0 will only insert the document if it exists already with the same _version_
        if result.get("_version_", None):
            del result['_version_']
            
        # append suffix to all ID fields
        result['id'] = result['id'] + suffix
        result['master_id'] = result['master_id'] + suffix
        result['instance_id'] = result['instance_id'] + suffix
        if result.get("dataset_id", None):
            result['dataset_id'] = result['dataset_id'] + suffix
            
        # apply replacement patterns
        if len(replacements) > 0:
            for key, value in result.items():
                # multiple values
                if hasattr(value, "__iter__"):
                    result[key] = []
                    for _value in value:
                        result[key].append(_replaceValue(_value, replacements))
                # single value
                else:
                    result[key] = _replaceValue(value, replacements)
    
    # FIX broken dataset records
    if core=='datasets':
        for result in response.results:
            for field in ['height_bottom', 'height_top']:
                value = result.get(field, None)
                if value:
                    try:
                        result[field] = float(value)
                    except ValueError:
                        result[field] = 0.
                        
    logging.info("Adding results...")
    s2.add_many(response.results, commit=False) # will not commit these changes
    logging.info("...done adding")
    
    logging.debug("Response: current number of records=%s total number of records=%s" % (start+_numRecords, _numFound))
    return (_numFound, _numRecords)
    
def _replaceValue(value, replacements):
    '''Apply dictionary of 'replacements' patterns to the string 'value'.'''
    
    if isinstance(value, basestring): # 'basestring' is the superclass of both 'str' and 'unicode'
        for oldValue, newValue in replacements.items():
            value = value.replace(oldValue, newValue)
        
    return value    

if __name__ == '__main__':
    
    # parse command line arguments
    parser = argparse.ArgumentParser(description="Migration tool for Solr indexes")
    parser.add_argument('sourceSolrUrl', type=str, help="URL of source Solr (example: http://localhost:8983/solr)")
    parser.add_argument('targetSolrUrl', type=str, help="URL of target Solr (example: http://localhost:8984/solr)")
    parser.add_argument('--core', dest='core', type=str, help="URL of target Solr (example: --core datasets)", default=None)
    parser.add_argument('--query', dest='query', type=str, help="Optional query to sub-select records (example: --query project:xyz)", default=DEFAULT_QUERY)
    parser.add_argument('--start', dest='start', type=int, help="Optional first record to be migrated (example: --start 1000)", default=0)
    parser.add_argument('--replace', dest='replace', type=str, help="Optional string replacements for all field (example: --replace old_value_1:new_value_1,old_value_2:new_value_2)", default=None)
    parser.add_argument('--max', dest='max', type=int, help="Optional maxRecords number of records to be migrated (example: --max 1000)", default=MAX_RECORDS_TOTAL)
    parser.add_argument('--suffix', dest='suffix', type=str, help="Optional suffix string to append to all record ids (example: --suffix abc)", default='')
    args_dict = vars( parser.parse_args() )
    
    # execute migration
    migrate(args_dict['sourceSolrUrl'], args_dict['targetSolrUrl'], 
            core=args_dict['core'], query=args_dict['query'], start=args_dict['start'], replace=args_dict['replace'],
            maxRecords=args_dict['max'], suffix=args_dict['suffix'])