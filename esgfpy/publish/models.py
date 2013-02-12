'''

Module :mod:`esgfpy.publish.models`
===================================

Module containing classes representing ESGF searchable metadata records.

@author: Luca Cinquini
'''

from xml.etree.ElementTree import Element, SubElement

from .consts import TYPE_DATASET, TYPE_FILE, TYPE_AGGREGATION, SOLR_CORES
from .utils import isNull

class Record(object):
    """
    Class representing a basic ESGF metadata record with the minimum required fields.
    This is the superclass of all Record classes.
    """
    
    def __init__(self, id, title, type, fields={}):
        """
        :param id: globally unique identifier for the record
        :param title: short record title to display in search results
        :param type: valid record type
        :param fields: dictionary of record metadata fields as (name, values) pairs
        """
        
        # arguments validation
        if isNull(id):
            raise Exception("Invalid record id: %s" % id)
        self.id = id
        if isNull(title):
            raise Exception("Invalid record title: %s" % title)       
        self.title = title
        if not type in SOLR_CORES:
            raise Exception("Unknown record type: %s" % type)
        self.type = type
        self.fields = fields
        
        #! TODO: remove these mandatory fields from core schema ?
        if not 'index_node' in self.fields:
            self.fields['index_node'] = ["localhost"]
        if not 'data_node' in self.fields:
            self.fields['data_node'] = ["localhost"]
        
        
    def toXML(self):
        """Method to serialize the record into Solr/XML. Returns the root XML element object."""
        
        # <doc>
        docEl = Element("doc")
        
        # <field name="id">obs4MIPs.NASA-JPL.AIRS.mon.v1|esg-datanode.jpl.nasa.gov</field>
        Record._add_field(docEl, 'id', self.id)
        
        # <field name="title">obs4MIPs NASA-JPL AIRS L3 Monthly Data</field>
        Record._add_field(docEl, 'title', self.title)

        # <field name="type">Dataset</field>
        Record._add_field(docEl, 'type', self.type)
                
        # <field name="model">mymodel</field>
        for name, values in self.fields.items():
            for value in values:
                Record._add_field(docEl, name, value)
        
        return docEl
    
    @staticmethod
    def _add_field(docEl, name, value):
        """Method to add a single-valued field to an XML element."""
        
        el = SubElement(docEl, "field", attrib={ "name": name })
        el.text = value
        
class DatasetRecord(Record):
    """Class representing an ESGF metadata record of type 'Dataset'."""
    
    def __init__(self, id, title, fields={}):
        """
        :param id: globally unique identifier for the record
        :param title: short record title to display in search results
        :param fields: dictionary of record metadata fields as (name, values) pairs
        """
        super(DatasetRecord,self).__init__(id, title, TYPE_DATASET, fields)
        
class FileRecord(Record):
    """Class representing an ESGF metadata record of type 'File'."""
    
    def __init__(self, datasetRecord, id, title, fields={}):
        """
        :param datasetRecord: the parent Dataset record
        :param id: globally unique identifier for the record
        :param title: short record title to display in search results
        :param fields: dictionary of record metadata fields as (name, values) pairs
        """
        super(FileRecord,self).__init__(id, title, TYPE_FILE, fields)
        
        # store parent dataset
        self.datasetId = datasetRecord.id
        
    def toXML(self):
        """Overridden superclass method to serialize File specific metadata."""
        
        docEl = super(FileRecord,self).toXML()
        
        # add "dataset_id" field
        # <field name="dataset_id">....</field>
        Record._add_field(docEl, 'dataset_id', self.datasetId)
        
        return docEl
    
class AggregationRecord(Record):
    """Class representing an ESGF metadata record of type 'Aggregation'."""
    
    def __init__(self, id, title, fields={}):
        """
        :param datasetRecord: the parent Dataset record
        :param id: globally unique identifier for the record
        :param title: short record title to display in search results
        :param fields: dictionary of record metadata fields as (name, values) pairs
        """
        super(DatasetRecord,self).__init__(id, title, TYPE_AGGREGATION, fields)
        
    def toXML(self):
        """Overridden superclass method to serialize Aggregation specific metadata."""
        
        docEl = super(FileRecord,self).toXML()
        
        # add "dataset_id" field
        # <field name="dataset_id">....</field>
        Record._add_field(docEl, 'dataset_id', self.datasetId)
        
        return docEl