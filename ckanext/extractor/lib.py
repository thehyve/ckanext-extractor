#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) 2016 Stadt Karlsruhe (www.karlsruhe.de)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


from __future__ import absolute_import, print_function, unicode_literals

import datetime
import tempfile
import os

from pylons import config
import pysolr
import requests

from sas7bdat import SAS7BDAT as sas7bdat
import logging

convert_extensions = ['sas7bdat']

def download_and_extract(resource_url):
    """
    Download resource and extract metadata using Solr.

    The extracted metadata is cleaned and returned.
    """
    with tempfile.NamedTemporaryFile() as f:
        extension = resource_url.split('.')[-1]
        r = requests.get(resource_url, stream=True)
        r.raise_for_status()
        for chunk in r.iter_content(chunk_size=1024):
            f.write(chunk)
        f.flush()
        f.seek(0)
        
        # If we have a file we want to convert, convert it to a csv file
        # and extract that instead of the original file.
        if extension in convert_extensions: 
            csv_name = '{}.csv'.format(f.name)
            sas7bdat(f.name).convert_file(csv_name)
            f = open(csv_name)
        
        data = pysolr.Solr(config).extract(f, extractFormat='text')
        
        # If we created a csv file remove the file since we no longer need it.     
        if '.csv' == f.name[-4:]:
            f.close()
            os.remove(f.name)
    
    data['metadata']['fulltext'] = data['contents']
    return dict(clean_metadatum(*x) for x in data['metadata'].iteritems())


def clean_metadatum(key, value):
    """
    Clean an extracted metadatum.

    Takes a key/value pair and returns it in cleaned form.
    """
    if isinstance(value, list) and len(value) == 1:
        # Flatten 1-element lists
        value = value[0]
    key = key.lower().replace('_', '-')
    return key, value

