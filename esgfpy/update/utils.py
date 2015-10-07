import urllib2
import solr
from xml.etree.ElementTree import Element, SubElement, tostring

import logging

def buildSolrXml(updateDict, update='set', solr_url='http://localhost:8984/solr', solr_core='datasets'):
    '''
    Method to build a Solr/XML update document.
    
    updateDict: dictionary of Solr queries to map of field name and values to be updated for all matching results
    update='set' to override the previous values of that field, update'add' to add new values to that field

    Example of updateDict:
    setDict = { 'id:test.test.v1.testData.nc|esgf-dev.jpl.nasa.gov': 
                {'xlink':['http://esg-datanode.jpl.nasa.gov/.../zosTechNote_AVISO_L4_199210-201012.pdf|AVISO Sea Surface Height Technical Note|summary']}}
                
    Note: multiple query constraints can be combined with '&', for example: 'id:obs4MIPs.NASA-JPL.AIRS.mon.v1|esgf-node.jpl.nasa.gov&variable:hus*'

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
    
    # root of global update document
    rootEl = Element("add")
    
    # loop over query expressions
    for query, fieldDict in updateDict.items():
        
        # execute query to Solr
        queries = query.split('&')
        response = solr_server.query('*:*', fq=queries, start=0)
        logging.info("Executing query=%s number of records found: %s" % (query, response.numFound))
        
        # update all records matching the query
        # <add>
        
        for result in response.results:
            logging.debug("Updating record id=%s" % result['id'])
            
            # <doc>
            docEl = SubElement(rootEl, "doc")
            el = SubElement(docEl, "field", attrib={ "name": 'id' })
            el.text = str(result['id'])
            
            # loop over fields to be updates
            for fieldName, fieldValues in fieldDict.items():
                for fieldValue in fieldValues:
                    # <field name="id">test.test.v1.testData.nc|esgf-dev.jpl.nasa.gov</field>
                    el = SubElement(docEl, "field", attrib={ "name": fieldName, 'update': update })
                    el.text = str(fieldValue)

    # serialize document from all queries            
    xmlstr = tostring(rootEl)
    logging.debug(xmlstr)
    return xmlstr
    

def sendSolrXml(xmlDoc, solr_url='http://localhost:8984/solr', solr_core='datasets'):
    '''Method to send a Solr/XML update document to a specific Solr server and core.'''
    
    #
    url = solr_url + "/" + solr_core + '/update?commit=true'
    
    # send XML document
    r = urllib2.Request(url, data=xmlDoc,
                         headers={'Content-Type': 'application/xml'})
    u = urllib2.urlopen(r)
    response = u.read()
    logging.info(response)
