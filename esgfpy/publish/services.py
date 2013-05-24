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

from .consts import TYPE_DATASET, TYPE_FILE, SOLR_CORES
from .metadata_parsers import OptOutMetadataFileParser

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
                rootEl.append(record.toXML())
            else:
                print "Deleting record: type=%s id=%s" % (record.type, record.id)
                queryEl = SubElement(rootEl, "query")
                queryEl.text = "id:%s" % record.id
             
            # send every MAX_RECORDS records
            if ( (i+1) % PublishingClient.MAX_RECORDS) == 0:  
                
                # post these records
                self._post_xml(solr_url, tostring(rootEl, encoding="UTF-8") )
                
                # remove all children
                rootEl.clear()
                
        # post remaining records
        if len(list(rootEl)) > 0:
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
    An optional MetadataFileParser can be used to parse additional metadata inside files within the directory tree
    - in this case, the metadata files are not counted towards the creation of a dataset record.
    """
    
    def __init__(self, datasetRecordFactory, fileRecordFactory, metadataFileParser=OptOutMetadataFileParser()):
        """
        :param datasetRecordFactory: subclass of DatasetRecordFactory
        :param fileRecordFactory: subclass of FileRecordFactory
        :param metadataFileParser: optional collaborator to augment the dataset and files metadata (defaults to no-opt)
        """
        self.datasetRecordFactory = datasetRecordFactory
        self.fileRecordFactory = fileRecordFactory
        self.metadataFileParser = metadataFileParser
                
    def index(self, startDirectory):
        """ 
        This method implementation traverses the directory tree
        and creates records whenever it finds a non-empty sub-directory.
        The metadata file: /.../.../<subdir>/<subdir>.xml will be associated with all dataset records under /.../.../<subdir>
        The metadata file: /.../.../<subdir>/<filemname>.ext.xml will be associated with the single file <filemname>.ext
        """
        
        records = { TYPE_DATASET:[], TYPE_FILE:[]}
        dMetadata = {} # empty additional dataset-level metadata dictionary
        fMetadata = {} # empty additional file-level metadata dictionary
        for dir, subdirs, files in os.walk(startDirectory):
            
            # distinguish data and metadata files
            datafiles = []
            if len(files)>0:
                
                for file in files:
                    # ignore hidden files and thumbnails
                    if not file[0] == '.' and not 'thumbnail' in file:
                        filepath = os.path.join(dir, file)
                        
                        # metadata file
                        if self.metadataFileParser.isMetadataFile(filepath):
                            print 'Parsing metadata file: %s' % filepath
                            
                            met = self.metadataFileParser.parseMetadata(filepath)
                            filename, ext = os.path.splitext(file)
                            if dir.endswith(filename):
                                # dataset-level metadata
                                # key example: /Users/.../Evaluation/Dataset/BCCA/Protocol1
                                dMetadata[dir] = met
                            else:
                                # file-level metadata
                                # key example: /Users/.../Evaluation/Dataset/BCCA/Protocol1/Group1/Temperature/Maurer_Tmax_Above_90_deg_NC_CSC_1950.jpg
                                fMetadata[os.path.join(dir,filename)] = met
                        
                        # data file
                        else:
                            datafiles.append(filepath)
                  
            # create dataset containing these data files
            if len(datafiles)>0:      
                        
                # create list of one Dataset record
                datasetRecord = self.datasetRecordFactory.create(dir, 
                                                                 metadata=self._subSelectMetadata(dMetadata,dir))
                # add number of files
                datasetRecord.fields['number_of_files'] = [str(len(datafiles))]
                
                # directory structure matches template
                if datasetRecord is not None:
                    #print 'Walking dir=%s, subdirs=%s, files=%s' % (dir, subdirs, datafiles)
                    records[TYPE_DATASET].append( datasetRecord )
                    # create list of multiple File records
                    for datafile in datafiles:
                        records[TYPE_FILE].append( self.fileRecordFactory.create(datasetRecord, datafile, 
                                                                                 metadata=self._subSelectMetadata(fMetadata, datafile)) )
                
        return records

    def _subSelectMetadata(self, metadata, path):
        """Utility method to subselect a metadata dictionart by matching keys to the given path."""
        subMetadata = {}
        for key, met in metadata.items():
            if key in path:
                # copy 'met' dictionary onto 'subMetadata' dictionary
                subMetadata.update(met)
        return subMetadata

def build_solr_update_url(solr_base_url, record_type):
    #! TODO: remove this function as records will not be published to Solr directly."""

    try:
        core = SOLR_CORES[string.capitalize(record_type)]
    except KeyError:
        raise Exception('Record type: %s is not supported' % record_type )
    
    return "%s/%s/update" % (solr_base_url, core)