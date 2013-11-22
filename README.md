esgfpy-publish
==============

Python package containing client-side functionality to publish resources to ESGF.

Prerequisites:

o PIL - Python Image Library
	sudo pip install pil

Quick startup instructions:

o cd <INSTALLATION DIRECTORY>
o export PYTHONPATH=$PYTHONPATH:<INSTALLATION DIRECTORY>

o create a file /usr/local/esgf/config/esgfpy-publish.cfg: 
create a section with project specific parameters, see example provided with distribution

o edit the file esgfpy/publish/main.py, customize as needed
o python esgfpy/publish/main.py True