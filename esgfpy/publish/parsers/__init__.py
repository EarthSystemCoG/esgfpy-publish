'''
Package containing metadata parsers.
'''

from optout_parser import OptOutMetadataFileParser
from netcdf_parser import NetcdfMetadataFileParser
from xml_parser import XMLMetadataFileParser
from filename_parser import FilenameMetadataParser
from directory_parser import DirectoryMetadataParser
from tes_xml_parser import TesXmlMetadataFileParser
from hdf_parser import HdfMetadataFileParser
from acos_parser import AcosFileParser, AcosLiteFileParser_v34r02, AcosLiteFileParser_v34r03, AcosLiteFileParser_v35r02
from oco2_parser import Oco2L2StdFileParser, Oco2LtCO2FileParser, Oco2LtSIFFileParser
from tes_parser import TesFileParser
from airs_parser import AirsFileParser
