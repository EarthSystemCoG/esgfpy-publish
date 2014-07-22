'''
Module :mod:`pyesgf.publish.services`
=====================================

Module containing service classes for publishing/unpublishing metadata records to ESGF.

@author: Luca Cinquini
'''

import urllib
import urllib2
import string
import os
import json
from xml.etree.ElementTree import Element, SubElement, tostring
import logging

from esgfpy.publish.consts import TYPE_DATASET, TYPE_FILE, SOLR_CORES

class PublishingClient(object):
    """
    Client to remote ESGF publishing service.
    This implementation publishes 10 records at a time, divided by type,
    then commits all records at once.
    """

    # number of max records for publishing/unpublishing operations
    MAX_RECORDS_PER_REQUEST = 10

    def __init__(self, indexer, publishing_service_url='http://localhost:8984/solr', maxRecords=-1):
        """
        :param indexer: Indexer instance responsible for generating the XML records.
        :param publishing_service_url: URL of publishing service where XML records are sent.
        """
        #! TODO: change from Solr to the ESGF publishing service.

        # class responsible for creating the Solr records
        self.indexer = indexer

        # URL or remote publishing service
        self.publishing_service_url = publishing_service_url
        
        # maximum number of records published/unpublished
        self.maxRecords = maxRecords

    def publish(self, uri):
        """
        Method to publish records from a metadata repository to the ESGF publishing service.

        :param uri: URI of metadata repository to be indexed
        """

        # generate records from repository
        records = self.indexer.index(uri, self.maxRecords)

        # publish records to remote publishing service, one type at a time
        # first Datasets
        self._post( records[TYPE_DATASET], TYPE_DATASET )

        # then Files
        self._post( records[TYPE_FILE], TYPE_FILE )
        
        # update datasets
        self._update( records[TYPE_DATASET] )
        
    def unpublish(self, uri):
        """
        Method to unpublish records from a metadata repository to the ESGF publishing service.

        :param uri: URI of metadata repository to be indexed
        """

        # generate records from repository
        records = self.indexer.index(uri, self.maxRecords)

        # unpublish Files first
        self._post( records[TYPE_FILE], TYPE_FILE, publish=False )

        # then unpublish Datasets
        self._post( records[TYPE_DATASET], TYPE_DATASET, publish=False )
        
    def _update(self, datasetRecords):
        ''' Updates the "number_of_files" field for each dataset (in one cumulative request).'''
        
        # <add>...</add>
        rootEl = Element("add")
        for datasetRecord in datasetRecords:
            
            solr_url = build_solr_query_url(self.publishing_service_url, TYPE_FILE)
            jobj = self._query_json(solr_url, {"q":"dataset_id:%s" % datasetRecord.id} )
            numberOfFiles = jobj["response"]["numFound"]
            
            # <doc>...</doc>
            docEl = SubElement(rootEl, "doc")
            
            # <field name="id>$dataset_id</field>
            fieldIdEl = SubElement(docEl, "field")
            fieldIdEl.attrib["name"] = "id"
            fieldIdEl.text = "%s" % datasetRecord.id
            
            # <field name="number_of_files" update="set">$number_of_files</update>
            fieldNumEl = SubElement(docEl, "field")
            fieldNumEl.attrib["name"] = "number_of_files"
            fieldNumEl.attrib["update"] = "set"
            fieldNumEl.text = "%s" % numberOfFiles
            print "Updating dataset=%s number of files=%s" % (datasetRecord.id, numberOfFiles)
            
        xml = tostring(rootEl, encoding="UTF-8")
        solr_url = build_solr_update_url(self.publishing_service_url, TYPE_DATASET)
        self._post_xml(solr_url, xml)
        self._commit(TYPE_DATASET)

    def _post(self, records, record_type, publish=True):
        """Method to publish/unpublish a list of records of the same type to the publishing service."""

        #! TODO: use the same publishing_service_url for all records
        solr_url = build_solr_update_url(self.publishing_service_url, record_type)

        # enclosing <add>/<delete> instruction
        if publish:
            rootEl = Element("add")
        else:
            rootEl = Element("delete")

        # loop over records, publish/unpublish "MAX_RECORDS_PER_REQUEST" records at once
        print "Number of %s records: %s" % (record_type, len(records))
        for i, record in enumerate(records):

            # add/delete this record
            if publish:
                print "Adding record: type=%s id=%s" % (record.type, record.id)
                #print tostring(record.toXML(), encoding="UTF-8")
                rootEl.append(record.toXML())
            else:
                print "Deleting record: type=%s id=%s" % (record.type, record.id)
                queryEl = SubElement(rootEl, "query")
                queryEl.text = "id:%s" % record.id

            # send every MAX_RECORDS_PER_REQUEST records
            if ( (i+1) % PublishingClient.MAX_RECORDS_PER_REQUEST) == 0:

                #logging.debug("Posting XML:\n%s" % tostring(rootEl, encoding="UTF-8") )

                # post these records
                self._post_xml(solr_url, tostring(rootEl, encoding="UTF-8") )

                # remove all children
                rootEl.clear()

        if len(list(rootEl)) > 0:
            #logging.debug("Posting XML:\n%s" % tostring(rootEl, encoding="UTF-8") )
            self._post_xml(solr_url, tostring(rootEl, encoding="UTF-8") )

        # commit all records of this type at once
        self._commit(record_type)
        
    def _post_xml(self, url, xml):
        """Method to post an XML document to the publishing service."""

        #print '\nPosting to URL=%s\nXML=\n%s' % (url, xml)
        request = urllib2.Request(url=url,
                                  data=xml,
                                  headers={'Content-Type': 'text/xml; charset=UTF-8'})
        response = urllib2.urlopen(request)
        response.read()
        
    def _query_json(self, url, params):
        '''Executes HTTP GET request and return results as json object.'''
        
        url = url+"?"+"wt=json&"+urllib.urlencode(params)
        #print 'Solr search URL=%s' % url
        fh = urllib2.urlopen( url )
        jdoc = fh.read().decode("UTF-8")
        jobj = json.loads(jdoc)
        return jobj

    def _commit(self, record_type):

        solr_url = build_solr_update_url(self.publishing_service_url, record_type)
        self._post_xml(solr_url, '<commit/>')

class Indexer(object):
    """API for generating ESGF metadata records by parsing a given URI location."""

    def index(self, uri, maxRecords=-1):
        """
        :param uri: reference to records storage
        :param maxRecords: if greater than 0, maximum number of records to be published
        :return: dictionary of record type, records list
        """
        raise NotImplementedError("Error: index() method must be implemented by subclasses.")


class FileSystemIndexer(Indexer):
    """
    Class that generates XML/Solr records by parsing a local directory tree.
    It uses the configured DatasetRecordFactory and FileRecordfactory to create the records.
    A dataset record is created whenever files are found in a directory, and associated with the corresponding file records.
    Optionally, selected File metadata can be copied into the containing Dataset metadata, and viceversa.
    """

    def __init__(self, datasetRecordFactory, fileRecordFactory, fileMetadataKeysToCopy=[], datasetMetadataKeysToCopy=[] ):
        """
        :param datasetRecordFactory: subclass of DatasetRecordFactory
        :param fileRecordFactory: subclass of FileRecordFactory
        :param fileMetadataKeysToCopy: list of metadata keys (strings)
        :param datasetMetadataKeysToCopy: list of metadata keys (strings)
        """

        # record factories
        self.datasetRecordFactory = datasetRecordFactory
        self.fileRecordFactory = fileRecordFactory

        # metadata fields to copy
        self.fileMetadataKeysToCopy = fileMetadataKeysToCopy
        self.datasetMetadataKeysToCopy = datasetMetadataKeysToCopy

    def index(self, startDirectory, maxRecords=-1):
        """
        This method implementation traverses the directory tree
        and creates records whenever it finds a non-empty sub-directory.
        The metadata file: /.../.../<subdir>/<subdir>.xml will be associated with all dataset records under /.../.../<subdir>
        The metadata file: /.../.../<subdir>/<filemname>.ext.xml will be associated with the single file <filemname>.ext
        """

        records = {TYPE_DATASET:[], TYPE_FILE:[]}
        if not os.path.isdir(startDirectory):
            raise Exception("Wrong starting directory: %s" % startDirectory)
        else:
            print 'Start directory=%s' % startDirectory
            
        for directory, subdirs, files in os.walk(startDirectory):
            
            # create list of one Dataset record
            datasetRecord = self.datasetRecordFactory.create(directory)


            # directory structure matches ones of the templates
            if datasetRecord is not None:
                
                # loop over files in this directory
                for file in files:
                    if maxRecords < 0 or len(records[TYPE_FILE]) < maxRecords-1:
                        # ignore hidden files and thumbnails
                        if not file[0] == '.' and not 'thumbnail' in file and not file.endswith('.xml'):
                            
                            filepath = os.path.join(directory, file)
                            fileRecord = self.fileRecordFactory.create(datasetRecord, filepath)
                            
                            # file matches one of the patterns
                            if fileRecord is not None:
                                
                                # copy metadata from File --> Dataset
                                self._copyMetadata(self.fileMetadataKeysToCopy, fileRecord, datasetRecord)
                                # copy metadata from Dataset --> File
                                self._copyMetadata(self.datasetMetadataKeysToCopy, datasetRecord, fileRecord)
                                
                                # add this File record
                                records[TYPE_FILE].append( fileRecord )

                # dataset has files
                if len(records[TYPE_FILE]) > 0 :

                    # add number of files
                    datasetRecord.fields['number_of_files'] = [str( len(records[TYPE_FILE]) )]

                    # add this Dataset record
                    records[TYPE_DATASET].append( datasetRecord )


        return records

    def _copyMetadata(self, keys, fromRecord, toRecord):
        '''Utility method to copy selected metadata fields from one record to another.
           For each key, if append=False the field is only copied if not existing already,
           if append=True the field is appended to the existing values.
           '''

        for key, append in keys.items():
            if key in fromRecord.fields:
                if append or key not in toRecord.fields:
                    if key not in toRecord.fields:
                        toRecord.fields[key] = []
                    for value in fromRecord.fields[key]:
                        toRecord.fields[key].append(value)


def build_solr_update_url(solr_base_url, record_type):
    #! TODO: remove this function as records will not be published to Solr directly."""

    try:
        core = SOLR_CORES[string.capitalize(record_type)]
    except KeyError:
        raise Exception('Record type: %s is not supported' % record_type )

    return "%s/%s/update" % (solr_base_url, core)

def build_solr_query_url(solr_base_url, record_type):

    try:
        core = SOLR_CORES[string.capitalize(record_type)]
    except KeyError:
        raise Exception('Record type: %s is not supported' % record_type )

    return "%s/%s/select" % (solr_base_url, core)