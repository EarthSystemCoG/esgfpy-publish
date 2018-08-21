# Docker image containing Python package
# to harvest records from a remote ESGF Solr server

FROM python:2.7

# apply latest security patches
RUN apt-get update && \
    apt-get install -y git

# install ESGF harvesting software
RUN cd /usr/local && \
    git clone https://github.com/EarthSystemCoG/esgfpy-publish.git && \
    cd esgfpy-publish && \
    pip install --no-cache-dir -r requirements.txt
    
ENV PYTHONPATH=/usr/local/esgfpy-publish

WORKDIR /usr/local/esgfpy-publish

