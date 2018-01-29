#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2018 Bitergia
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# Authors:
#   Alberto Pérez García-Plaza <alpgarcia@bitergia.com>
#
"""Sample code to query ES for author first and last commit dates.
"""

import certifi
import configparser
import json
import os
import sys

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

def create_conn():
    """Creates an ES connection from ''.settings' file.

    ''.settings' contents sample:
    [ElasticSearch]

    user=john_smith
    password=aDifficultOne
    host=my.es.host
    port=80
    path=es_path_if_any
    """

    parser = configparser.ConfigParser()
    parser.read('.settings')

    section = parser['ElasticSearch']
    user = section['user']
    password = section['password']
    host = section['host']
    port = section['port']
    path = section['path']

    connection = "https://" + user + ":" + password + "@" + host + ":" + port \
                + "/" + path

    es_read = Elasticsearch([connection], use_ssl=True,
                            verity_certs=True, ca_cert=certifi.where(),
                            scroll='300m', timeout=1000)

    return es_read

def main():
    """Query ES to get first and last commit of each author together with
    some extra info like .
    """
    es_conn = create_conn()

    # Create search object
    s = Search(using=es_conn, index='git')

    # FILTER: retrieve commits before given year
    s = s.filter('range', grimoire_creation_date={'lt': 'now/y'})

    # Bucketize by uuid and get first and last commit (commit date is stored in
    # author_date field)
    s.aggs.bucket('authors', 'terms', field='author_uuid', size=10000000) \
        .metric('first', 'top_hits',
                _source=['author_date', 'author_org_name', 'author_uuid', 'project'],
                size=1,
                sort=[{"author_date": {"order": "asc"}}]) \
        .metric('last_commit', 'max', field='author_date')

    # Sort by commit date
    s = s.sort("author_date")

    #print(s.to_dict())
    result = s.execute()

    # Print result
    print(json.dumps(result.to_dict()['aggregations'], indent=2, sort_keys=True))

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        s = "\n\nReceived Ctrl-C or other break signal. Exiting.\n"
        sys.stdout.write(s)
        sys.exit(0)
    except RuntimeError as e:
        s = "Error: %s\n" % str(e)
        sys.stderr.write(s)
        sys.exit(1)
