esgfpy-publish
==============

Python package containing client-side functionality to publish resources to ESGF.

Prerequisites:

o PIL - Python Image Library
	to process images
	sudo pip install pil
	
o netcdf4-python
    to parse metadata contained in NetCDF files
	download netCDF4-1.0.7.tar.gz
	tar xvfz netCDF4-1.0.7.tar.gz
	cd netCDF4-1.0.7
	python setup.py build
	sudo python setup.py install
	
o h5py to parse metadata from HDF files
	
o xml.etree
  to parse metadata within XML files
  
o python-dateutil
  to parse a large set of date formats from NetCDF global attribute values

Quick startup instructions:

o cd <INSTALLATION DIRECTORY>
o export PYTHONPATH=$PYTHONPATH:<INSTALLATION DIRECTORY>

o create a file /usr/local/esgf/config/esgfpy-publish.cfg: 
create a section with project specific parameters, see example provided with distribution

o edit the file esgfpy/publish/main.py, customize as needed
o python esgfpy/publish/main.py True