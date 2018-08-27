# Docker image containing Python package
# to harvest records from a remote ESGF Solr server

FROM python:2.7

# install ESGF harvesting software
COPY . /usr/local/esgfpy-publish
RUN cd /usr/local/esgfpy-publish && \
    pip install --no-cache-dir -r requirements.txt
    
ENV PYTHONPATH=/usr/local/esgfpy-publish

WORKDIR /usr/local/esgfpy-publish

