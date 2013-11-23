'''
Module for parsing records metadata from ancillary files.
'''

from os.path import splitext, expanduser
from xml.etree.ElementTree import fromstring
from esgfpy.publish.consts import METADATA_MAPPING_FILE
import ConfigParser
import abc
from netCDF4 import Dataset
import os

    

    

    
