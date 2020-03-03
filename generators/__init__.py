#! python
# ===============LICENSE_START=======================================================
# metadata-flatten-extractor Apache-2.0
# ===================================================================================
# Copyright (C) 2017-2020 AT&T Intellectual Property. All rights reserved.
# ===================================================================================
# This software file is distributed by AT&T 
# under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# This file is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ===============LICENSE_END=========================================================
# -*- coding: utf-8 -*-

import pkgutil
from os import path
import json
import gzip

import logging
import warnings
from sys import stdout as STDOUT

import pandas as pd

modules = [name for _, name, _ in pkgutil.iter_modules(__path__)]

class Generate():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    PATH_DATA = path.join(path.dirname(path.dirname(__file__)), 'data')

    def __init__(self, path_destination, generator="unknown", format=".csv", universal=False):
        """Construct new generator instance

        :param path_destination: (str): Path (directory) for output file
        :param generator: (str): Name of the generator (from derived class)
        :param format: (str): File extension for output
        :param universal: (bool): Flag for universal (True, single file) output or independent (False) files
        """
        super().__init__()
        self._format = format
        self._generator = generator
        self._universal = universal
        self._path_destination = path_destination

    @property
    def is_universal(self):
        return self._universal

    def get_output_path(self, name_parser):
        if self._universal:
            return path.join(self._path_destination, self._generator + self._format)
        return path.join(self._path_destination, f"{self._generator}_{name_parser}{self._format}")

    def json_load(self, path_file):
        """Helper to read dict object from JSON

        :param path_file: (str): Path for source file (can be gzipped)
        :return: dict.  The loaded dict or an empty dict (`{}`) on error
        """
        if path.exists(path_file):
            if path_file.endswith(".gz"):
                infile = gzip.open(path_file, 'rt')
            else:
                infile = open(path_file, 'rt')
            try:
                return json.load(infile)
            except json.decoder.JSONDecodeError as e:
                return {}
            except UnicodeDecodeError as e:
                return {}
            if infile:
                infile.close()
        return {}

    def json_save(self, path_file, dict_source=None, pretty_print=False):
        """Helper to write dict object to json

        :param path_file: (str): Path for destination file
        :param dict_source: (dict): The dictionary to write to JSON
        :param pretty_print: (bool): Write out in more human-readable format
        :return: bool.  Sueccess of operation and non-empty dictionary.
        """
        if dict_source is not None:
            outfile = gzip.open(path_file, 'wt') if path_file.endswith(".gz") else open(path_file, 'wt')
            json.dump(dict_source, outfile, indent=4 if pretty_print else None)
            if outfile:
                outfile.close()
            return True
        return False