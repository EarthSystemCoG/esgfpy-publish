import urllib2
import solr
from xml.etree.ElementTree import Element, SubElement, tostring

import logging

# FIXME
MAX_ROWS = 10 # maximum number of records returned by a Solr query

def updateSolr(updateDict, update='set', solr_url='http://localhost:8984/solr', solr_core='datasets'):
    '''
    Method to bulk-update all matching records in a Solr index.
    
    updateDict: dictionary of Solr queries to map of field name and values to be updated for all matching results
    update='set' to override the previous values of that field, update'add' to add new values to that field

    Example of updateDict:
    setDict = { 'id:test.test.v1.testData.nc|esgf-dev.jpl.nasa.gov': 
                {'xlink':['http://esg-datanode.jpl.nasa.gov/.../zosTechNote_AVISO_L4_199210-201012.pdf|AVISO Sea Surface Height Technical Note|summary']}}
                
    Note: multiple query constraints can be combined with '&', for example: 'id:obs4MIPs.NASA-JPL.AIRS.mon.v1|esgf-node.jpl.nasa.gov&variable:hus*'
    
    Note: to remove a field, set its value to None or to an empty list, for example: 'xlink':None or 'xlink':[]

    Example od returned document:
    <?xml version="1.0" encoding="UTF-8" standalone="no"?>
    <add>
        <doc>
            <field name="id">test.test.v1.testData.nc|esgf-dev.jpl.nasa.gov</field>
            <field name="xlink" update="add">http://esg-datanode.jpl.nasa.gov/.../zosTechNote_AVISO_L4_199210-201012.pdf|AVISO Sea Surface Height Technical Note|summary</field>
        </doc>
    </add>
    '''
    
    solr_server = solr.SolrConnection(solr_url+"/"+solr_core)
    
    start = 0
    numFound = start+1
    while start < numFound:

        # query Solr, construct update document
        (xmlDoc, numFound, numRecords) = _buildSolrXml(solr_server, updateDict, update=update, solr_core=solr_core, start=start)
        
        print xmlDoc
        
        # send update document to Solr
        _sendSolrXml(xmlDoc, solr_url=solr_url, solr_core=solr_core)
        
        # increase starting record locator
        start += numRecords
            
            
    
def _buildSolrXml(solr_server, updateDict, update='set', solr_core='datasets', start=0):
    
    # root of global update document
    rootEl = Element("add")
    
    # loop over query expressions
    for query, fieldDict in updateDict.items():
        
        # execute query to Solr
        queries = query.split('&')
        response = solr_server.query('*:*', fq=queries, start=start, rows=MAX_ROWS, fl=['id'])
        numFound = response.numFound
        numRecords = len(response.results)
        logging.info("Executing query=%s start=%s total number of records found: %s number of records returned: %s" % (query, start, numFound, numRecords))
        
        # update all records matching the query
        # <add>
        
        for result in response.results:
            logging.debug("Updating record id=%s" % result['id'])
            
            # <doc>
            docEl = SubElement(rootEl, "doc")
            # <field name="id">obs4MIPs.NASA-JPL.AIRS.mon.v1.taStderr_AIRS_L3_RetStd-v5_200209-201105.nc|esgf-node.jpl.nasa.gov</field>
            el = SubElement(docEl, "field", attrib={ "name": 'id' })
            el.text = str(result['id'])
            
            # loop over fields to be updates
            for fieldName, fieldValues in fieldDict.items():
                
                if fieldValues is not None and len(fieldValues)>0:
                    for fieldValue in fieldValues:
                        #<field name="xlink" update="set">https://earthsystemcog.org/.../taTechNote_AIRS_L3_RetStd-v5_200209-201105.pdf|AIRS Air Temperature Technical Note|technote</field>
                        el = SubElement(docEl, "field", attrib={ "name": fieldName, 'update': update })
                        el.text = str(fieldValue)
                else:
                    # <field name="xlink" update="set" null="true"/>
                    el = SubElement(docEl, "field", attrib={ "name": fieldName, 'update': update, 'null':'true' })

    # serialize document from all queries            
    xmlstr = tostring(rootEl)
    logging.debug(xmlstr)
    return (xmlstr, numFound, numRecords)
    

def _sendSolrXml(xmlDoc, solr_url='http://localhost:8984/solr', solr_core='datasets'):
    '''Method to send a Solr/XML update document to a specific Solr server and core.'''
    
    #
    url = solr_url + "/" + solr_core + '/update?commit=true'
    
    # send XML document
    r = urllib2.Request(url, data=xmlDoc,
                         headers={'Content-Type': 'application/xml'})
    u = urllib2.urlopen(r)
    response = u.read()
    logging.info(response)
