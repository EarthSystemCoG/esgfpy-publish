import abc
import sys, os
import ConfigParser

class MetadataMapper(object):
    '''API for mapping metadata values to a controlled vocabulary.'''
    
    __metaclass__ = abc.ABCMeta
    
    @abc.abstractmethod
    def mappit(self, key, value):
        '''Maps a metadata value for a given key to another value.'''
        return value
    
class ConfigFileMetadataMapper(MetadataMapper):
    '''MetadataMapper based on a python configuration file.'''
    
    def __init__(self, configFilePath):
        
        self.map = {}
        self.config = ConfigParser.RawConfigParser()
        try:
            self.config.read( os.path.expanduser(configFilePath) )
            
            for section in self.config.sections():
                for option in self.config.options(section):
                    print "Mapping value:%s to: %s" % (option, self.config.get(section, option))
            
        except Exception as e:
            print e
            raise Exception("ERROR reading mapping configuration file: %s" % configFilePath)
        
    def mappit(self, key, value):
        '''Maps a metadata value for a given key to another value.'''
        
        if self.config.has_option(key, value):
            return self.config.get(key, value)
        else:
            return value
