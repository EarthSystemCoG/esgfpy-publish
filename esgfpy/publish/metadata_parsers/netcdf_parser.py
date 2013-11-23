from esgfpy.publish.metadata_parsers.abstract_parser import AbstractMetadataFileParser
from netCDF4 import Dataset

class NetcdfMetadataFileParser(AbstractMetadataFileParser):
    '''Parses metadata from NetCDF files.'''
    
    def isMetadataFile(self, filepath):
        return filepath.lower().endswith(".nc")
    
    def parseMetadata(self, filepath):
        print 'NetcdfMetadataFileParser: parsing filepath=%s' % filepath
        
        metadata = {} # empty metadata dictionary
    
        try:
            # open file
            nc = Dataset(filepath, 'r')
            
            # loop over global attributes
            for attname in nc.ncattrs():
                self._addMetadata(metadata, attname, getattr(nc, attname) )
            
            # loop over variable attributes
            for key, variable in nc.variables.items():
                if key.lower()!='longitude' and key.lower()!='latitude' and key.lower()!='altitude' and key.lower()!='time' and key.lower()!='level':
                    # IMPORTANT: the variable arrays must have the same number of entries
                    self._addMetadata(metadata, 'variable', key)
                    self._addMetadata(metadata, 'variable_long_name', getattr(variable, 'long_name', None) )
                    self._addMetadata(metadata, 'cf_standard_name', getattr(variable, 'stanadard_name', None) )
                    self._addMetadata(metadata, 'units', getattr(variable, 'units', None) )

        except Exception as e:
            print e
        finally:
            nc.close()
            
        return metadata