from esgfpy.publish.parsers.abstract_parser import AbstractMetadataFileParser
from netCDF4 import Dataset
import logging
from dateutil.parser import *

class NetcdfMetadataFileParser(AbstractMetadataFileParser):
    '''Parses metadata from NetCDF files.'''
        
    def parseMetadata(self, filepath):
        
        metadata = {} # empty metadata dictionary
        
        if filepath.endswith("nc"):
            logging.debug('NetcdfMetadataFileParser: parsing filepath=%s' % filepath)
        
            try:
                # open file
                nc = Dataset(filepath, 'r')
                
                # loop over global attributes
                for attname in nc.ncattrs():
                    attvalue = getattr(nc, attname)
                    if 'date' in attname.lower():
                        # must format dates in Solr format
                        solrDateTime = parse(attvalue)
                        self._addMetadata(metadata, attname, solrDateTime.strftime('%Y-%m-%dT%H:%M:%SZ') )
                    else:
                        self._addMetadata(metadata, attname, attvalue )
                    
                # loop over dimensions
                for key, dim in nc.dimensions.items():
                    self._addMetadata(metadata, 'dimension', "%s:%s" % (key, len(dim)) )
                
                # loop over variable attributes
                for key, variable in nc.variables.items():
                    if not self._isCoordinate(key):
                        # IMPORTANT: the variable arrays must have the same number of entries
                        self._addMetadata(metadata, 'variable', key)
                        self._addMetadata(metadata, 'variable_long_name', getattr(variable, 'long_name', None) )
                        self._addMetadata(metadata, 'cf_standard_name', getattr(variable, 'stanadard_name', None) )
                        self._addMetadata(metadata, 'units', getattr(variable, 'units', None) )
    
            except Exception as e:
                logging.error(e)
            finally:
                try:
                    nc.close()
                except:
                    pass
            
        return metadata
    
    def _isCoordinate(self, key):
        return (   key.lower()=='longitude' or key.lower()=='lon' 
                or key.lower()=='latitude' or key.lower()=='lat' 
                or key.lower()=='altitude' or key.lower()=='time' 
                or key.lower()=='level' or key.lower()=='altitude' or key.lower()=='alt')