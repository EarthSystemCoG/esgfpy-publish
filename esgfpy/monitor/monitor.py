#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Script to monitor the consistency of the 'latest' datasets throughout the Earth System Grid Federation

# Usage: python esgfpy/monitor/monitor.py <optional space-separated list of search constraints>
# Example: python esgfpy/monitor/monitor.py  project=CMIP5
# Example: python esgfpy/monitor/monitor.py  project=CMIP5 from=2015-01-01T00:00:00Z to=2015-12-31T23:59:29Z

import json
import logging
import sys
import urllib
import urllib2
from multiprocessing.dummy import Pool as ThreadPool
from tqdm import tqdm

MAX_THREADS = 4
ESGF_SEARCH_URL = 'https://esgf-node.ipsl.upmc.fr/esg-search/search'
LIMIT = 100
FORMAT = 'application/solr+json'
OUTPUT_FILE = 'bad_datasets.out'

logging.basicConfig(format='%(message)s',
                    filename='bad_datasets.log',
                    level=logging.INFO)


def get_url(params):
    return '{0}?{1}'.format(ESGF_SEARCH_URL,
                            urllib.urlencode(params))


def get_result(params):
    url = get_url(params)
    logging.debug('URL = {0}'.format(url))
    query = urllib2.urlopen(url)
    response = query.read().decode("UTF-8")
    return json.loads(response)


def check_dataset(master_id):
    # Dictionary of detected inconsistent datasets
    bad_datasets = {}
    # Execute Solr query to retrieve all master and replica instances of a master_id whatever the version
    _params = [('master_id', master_id),
               ('distrib', 'true'),
               ('fields', 'id,version,latest,replica'),
               ('format', FORMAT),
               ('facets', 'version'),
               ('offset', 0),
               ('limit', LIMIT)]
    _result = get_result(_params)

    # Get the number of result
    _numFound = _result['response']['numFound']

    # If one instance found, check the latest flag is set to True and slide to the next one
    if _numFound == 1:
        _instance = _result['response']['docs'][0]
        if _instance['latest']:
            return None
        else:
            logging.warn('\tSet latest flag to TRUE: {0} (replica={1})'.format(_instance['id'],
                                                                               _instance['replica']))
            bad_datasets[_instance['id']] = 'Latest flag FALSE -> TRUE'
            return bad_datasets

    _instances = list()
    for _doc in _result['response']['docs']:
        # Execute Solr query to retrieve all file checksums of a give dataset instance
        __params = [('dataset_id', _doc['id']),
                    ('distrib', 'true'),
                    ('fields', 'id'),
                    ('facets', 'checksum'),
                    ('type', 'File'),
                    ('format', FORMAT),
                    ('offset', 0),
                    ('limit', LIMIT)]
        __result = get_result(__params)
        __checksums = [str(i) for i in __result['facet_counts']['facet_fields']['checksum'] if isinstance(i, unicode)]
        _hash_value = hash(tuple(__checksums))
        _instances.append((_doc['id'], _hash_value, _doc['version'], _doc['latest'], _doc['replica']))

    # Sort the dataset instances by versions
    _instances.sort(key=lambda x: x[2], reverse=True)

    # For each version, keep only the master (replica = false).
    # If it doesn’t exist, keep only one replica instance.
    # So we get one tuple per version with priority to the master instance.
    _instances_uniq = list()
    for version in list(set(zip(*_instances)[2])):
        _instances_subset = [i for i in _instances if i[2] == version]
        if len(_instances_subset) > 1:
            if not any(zip(*_instances_subset)[4]):
                for i in _instances_subset:
                    logging.warn('\tDuplicated master instance: {0} (replica={1})'.format(i[0], i[4]))
                    return None
            elif all(zip(*_instances_subset)[4]):
                _instances_uniq.append(_instances_subset[0])
            else:
                _instances_uniq.append([i for i in _instances_subset if i[4] == False][0])
        else:
            # Only one instance exists with this version
            _instances_uniq.append(_instances_subset[0])

    # Loop on the resulting list of instances
    tmp = [] ; _latest_version = 'Unfound'
    for instance in _instances_uniq:
        if not instance[4]:  # if relplica = False
            # This is the real latest version. In respect with the master-driven lastest rule.
            _latest_version = instance[2]
            break
        else:  # if relplica = True, i.e., the master doesn't exist for this version
            try:
                next_instance = _instances_uniq[_instances_uniq.index(instance) + 1]
                if instance[1] != next_instance[1]:
                    # It means the master have been published in the past, replicated then unpublished.
                    # This is the real latest version.
                    _latest_version = instance[2]
                    break
                else:
                    # The version is inconsistent(one hash = one version).
                    # It means the master never existed for this version which only reflects a publication date.
                    # This instance would need a republication with the proper real latest version.
                    tmp.append(instance)
            except:
                # Only one version exists
                _latest_version = instance[2]

    # Finally check the full instances matrix
    logging.debug('{0} (latest version = {1})'.format(master_id, _latest_version))
    if _latest_version != 'Unfound':
        # Just print instances for debug
        for instance in _instances:
            logging.debug('\t{0}'.format('|'.join(map(str, instance))))
        for instance in _instances:
            # Set latest flag to TRUE for all instance with version == latest_version
            if instance[2] == _latest_version and not instance[3]:
                logging.warn('\t\tSet latest flag to TRUE: {0} (replica={1})'.format(instance[0], instance[4]))
                bad_datasets[instance[0]] = 'Latest flag FALSE -> TRUE'
            # Set latest flag to FALSE for all instance with version != latest_version
            if instance[2] != _latest_version and instance[3]:
                logging.warn('\t\tSet latest flag to FALSE: {0} (replica={1})'.format(instance[0], instance[4]))
                bad_datasets[instance[0]] = 'Latest flag TRUE -> FALSE'
        if tmp:
            # Print instances to republish with latest version
            for instance in tmp:
                logging.warn(
                    '\t\tShould be republished with version={0}: {1} (replica={2})'.format(_latest_version, instance[0],
                                                                                           instance[4]))
                bad_datasets[instance[0]] = 'Republish with version = {0}'.format(_latest_version)

    if bad_datasets:
        return bad_datasets
    else:
        return None


# main program
if __name__ == '__main__':

    # Capture optional search constraints from command line arguments
    if len(sys.argv) > 1:
        constraints = sys.argv[1:]
    else:
        constraints = []
    cparams = []
    for constraint in constraints:
        cparams.append(tuple(constraint.split('=')))
    # Execute Solr query to retrieve all master and replica instances of a master_id whatever the version
    params = [('distrib', 'true'),
              ('fields', 'master_id'),
              ('facets', 'master_id'),
              ('format', FORMAT),
              ('offset', 0),
              ('limit', LIMIT)] + cparams
    result = get_result(params)
    master_ids = [str(i) for i in result['facet_counts']['facet_fields']['master_id'] if isinstance(i, unicode)]
    # Remove duplicates from results
    master_ids = list(set(master_ids))
    # Make pool of processes
    pool = ThreadPool(MAX_THREADS)
    bad_datasets = [x for x in tqdm(pool.imap(check_dataset, master_ids),
                                    desc='Master IDs checking',
                                    total=len(master_ids),
                                    bar_format='{desc}{percentage:3.0f}% |{bar}| {n_fmt}/{total_fmt} ids',
                                    ncols=100,
                                    unit='ids',
                                    file=sys.stdout)]
    # Close threads pool
    pool.close()
    pool.join()

    # Write out results
    with open(OUTPUT_FILE, 'w') as outfile:
        json.dump(bad_datasets, outfile,
                  sort_keys = True, indent = 4, ensure_ascii = False)