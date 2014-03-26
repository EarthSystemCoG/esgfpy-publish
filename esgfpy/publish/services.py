'''
Module :mod:`pyesgf.publish.services`
=====================================

Module containing service classes for publishing/unpublishing metadata records to ESGF.

@author: Luca Cinquini
'''

import urllib2
import string
import os
from xml.etree.ElementTree import Element, SubElement, tostring
import logging

from .consts import TYPE_DATASET, TYPE_FILE, SOLR_CORES

class PublishingClient(object):
    """
    Client to remote ESGF publishing service.
    This implementation publishes 10 records at a time, divided by type,
    then commits all records at once.
    """

    # number of max records for publishing/unpublishing operations
    MAX_RECORDS = 10

    def __init__(self, indexer, publishing_service_url='http://localhost:8984/solr'):
        """
        :param indexer: Indexer instance responsible for generating the XML records.
        :param publishing_service_url: URL of publishing service where XML records are sent.
        """
        #! TODO: change from Solr to the ESGF publishing service.

        # class responsible for creating the Solr records
        self.indexer = indexer

        # URL or remote publishing service
        self.publishing_service_url = publishing_service_url

    def publish(self, uri):
        """
        Method to publish records from a metadata repository to the ESGF publishing service.

        :param uri: URI of metadata repository to be indexed
        """

        # generate records from repository
        records = self.indexer.index(uri)

        # publish records to remote publishing service, one type at a time
        # first Datasets
        self._post( records[TYPE_DATASET], TYPE_DATASET )

        # then Files
        self._post( records[TYPE_FILE], TYPE_FILE )

    def unpublish(self, uri):
        """
        Method to unpublish records from a metadata repository to the ESGF publishing service.

        :param uri: URI of metadata repository to be indexed
        """

        # generate records from repository
        records = self.indexer.index(uri)

        # unpublish Files first
        self._post( records[TYPE_FILE], TYPE_FILE, publish=False )

        # then unpublish Datasets
        self._post( records[TYPE_DATASET], TYPE_DATASET, publish=False )

    def _post(self, records, record_type, publish=True):
        """Method to publish/unpublish a list of records of the same type to the publishing service."""

        #! TODO: use the same publishing_service_url for all records
        solr_url = build_solr_update_url(self.publishing_service_url, record_type)

        # enclosing <add>/<delete> instruction
        if publish:
            rootEl = Element("add")
        else:
            rootEl = Element("delete")

        # loop over records, publish/unpublish "MAX_RECORDS" records at once
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

            # send every MAX_RECORDS records
            if ( (i+1) % PublishingClient.MAX_RECORDS) == 0:

                #logging.debug("Posting XML:\n%s" % tostring(rootEl, encoding="UTF-8") )

                # post these records
                self._post_xml(solr_url, tostring(rootEl, encoding="UTF-8") )

                # remove all children
                rootEl.clear()

        # post remaining records
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

    def _commit(self, record_type):

        solr_url = build_solr_update_url(self.publishing_service_url, record_type)
        self._post_xml(solr_url, '<commit/>')

class Indexer(object):
    """API for generating ESGF metadata records by parsing a given URI location."""

    def index(self, uri):
        """
        :param uri: reference to records storage
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

    def index(self, startDirectory):
        """
        This method implementation traverses the directory tree
        and creates records whenever it finds a non-empty sub-directory.
        The metadata file: /.../.../<subdir>/<subdir>.xml will be associated with all dataset records under /.../.../<subdir>
        The metadata file: /.../.../<subdir>/<filemname>.ext.xml will be associated with the single file <filemname>.ext
        """

        records = {TYPE_DATASET:[], TYPE_FILE:[]}
        dMetadata = {} # empty additional dataset-level metadata dictionary
        fMetadata = {} # empty additional file-level metadata dictionary
        if not os.path.isdir(startDirectory):
            raise Exception("Wrong starting directory: %s" % startDirectory)
        else:
            print 'Start directory=%s' % startDirectory
        for dir, subdirs, files in os.walk(startDirectory):

            # distinguish data and metadata files
            datafiles = []
            if len(files)>0:

                for file in files:
                    # ignore hidden files and thumbnails
                    if not file[0] == '.' and not 'thumbnail' in file and not file.endswith('.xml'):
                        filepath = os.path.join(dir, file)
                        datafiles.append(filepath)

            # create dataset containing these data files
            if len(datafiles)>0:

                # create list of one Dataset record
                datasetRecord = self.datasetRecordFactory.create(dir)

                # directory structure matches template
                if datasetRecord is not None:

                    # add number of files
                    datasetRecord.fields['number_of_files'] = [str(len(datafiles))]

                    #print 'Walking dir=%s, subdirs=%s, files=%s' % (dir, subdirs, datafiles)

                    # create list of multiple File records
                    for datafile in datafiles:
                        fileRecord = self.fileRecordFactory.create(datasetRecord, datafile)

                        # copy metadata from File --> Dataset
                        self._copyMetadata(self.fileMetadataKeysToCopy, fileRecord, datasetRecord)
                        # copy metadata from Dataset --> File
                        self._copyMetadata(self.datasetMetadataKeysToCopy, datasetRecord, fileRecord)

                        # add this File record
                        records[TYPE_FILE].append( fileRecord )

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