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
import importlib

import json
import re
import math
import gzip
from os import path

from pathlib import Path

import logging
import warnings
from sys import stdout as STDOUT

import pandas as pd

import contentaiextractor as contentai

class Flatten():
    # https://cloud.google.com/video-intelligence/docs/reference/reast/Shared.Types/Likelihood
    GCP_LIKELIHOOD_MAP = { "LIKELIHOOD_UNSPECIFIED": 0.0, "VERY_UNLIKELY": 0.1, "UNLIKELY": 0.25,
                           "POSSIBLE": 0.5, "LIKELY": 0.75, "VERY_LIKELY": 0.9 }
    TAG_TRANSCRIPT = "_transcript_"
    ROUND_DIGITS = 5
    SCORE_DEFAULT = 0.5


    def __init__(self, path_content, logger=None):
        super().__init__()
        self.extractor_keys = []
        self.extractor_name = None
        self.path_content = path_content
        if logger is None:
            logger = logging.getLogger()
            logger = logging.getLogger()
            logger.setLevel(logging.DEBUG)
            handler = logging.StreamHandler(STDOUT)
            handler.setLevel(logging.DEBUG)
            logger.addHandler(handler)
        self.logger = logger

    @staticmethod
    def known_types():
        """Return the output types for this generator
        :return: list.  List of output types (file types) for this generator
        """
        return None

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
        return {}

    def text_load(self, path_file):
        """Helper to read text object

        :param path_file: (str): Path for source file (can be gzipped)
        :return: dict.  The loaded dict or an empty dict (`{}`) on error
        """
        if path.exists(path_file):
            if path_file.endswith(".gz"):
                infile = gzip.open(path_file, 'rt')
            else:
                infile = open(path_file, 'rt')
            try:
                return infile.read()
            except UnicodeDecodeError as e:
                return ""
        return ""

    def get_extractor_results(self, extractor_name, path, force_retrieve=False, is_json=True):
        """Get results from remote or local location.  Return a dictionary or string (depending on is_json), empty if not found"""
        result_data = {} if is_json else ""
        if force_retrieve or (len(self.extractor_keys) < 1 or self.extractor_name != extractor_name):   # safe way to request without 404/500 error
            self.extractor_name = extractor_name
            try:
                self.extractor_keys = self.get_extractor_keys(extractor_name)
                self.logger.info(f"Retrieved available keys {self.extractor_keys} for extractor {self.extractor_name} ")
                if self.extractor_keys is None:
                    self.extractor_keys = []
            except Exception as e:
                self.logger.info(f"Failed to get extractor keys for extractor {self.extractor_name} (error: '{e}')")
        if self.extractor_keys is not None and path in self.extractor_keys:   # have the keys, check for presence
            try:
                if is_json:
                    _local_data = contentai.get_json(extractor_name, path)
                else:
                    _local_data = contentai.get(extractor_name, path)
                result_data = _local_data
            except Exception as e:
                self.logger.warning(f"Failed to get key data '{path}' for extractor '{extractor_name}'")

        if not result_data:  # do we need to load it locally?
            for dir_search in self.recursive_search(self.path_content, extractor_name):
                path_file = dir_search.joinpath(path)
                if is_json:
                    result_data = self.json_load(str(path_file))
                    if not result_data:
                        result_data = self.json_load(str(path_file)+".gz")
                else:  # not JSON, just return string?
                    result_data = self.text_load(str(path_file))
                    if not result_data:
                        result_data = self.text_load(str(path_file)+".gz")
        return result_data


    def get_extractor_keys(self, extractor_name):
        return contentai.keys(extractor_name)

    def recursive_search(self, path_root, extractor_name):
        """Attempt to find a specific extractor directory under the desired path"""
        list_dirs = []
        for path_search in Path(path_root).rglob(extractor_name):
            if path_search.is_dir():
                list_dirs.append(path_search)
        return list_dirs

# import other modules

_modules = []
for module_finder, extractor_name, _ in pkgutil.iter_modules(__path__):
    parser_module = module_finder.find_module(extractor_name).load_module()
    parser_obj = getattr(parser_module, "Parser")   # get class template
    if parser_obj is not None:
        _modules.append({'obj':parser_obj, 'types':parser_obj.known_types(), 'name':extractor_name})

def get_by_type(type_list=None):
    """Get parsers with a specific filter for type.

    :param local_list: (list) list of tag type required in output (e.g. ['shot', 'tag']) (default=None or all available)
    :return list: list of raw "Parser()" classes that are instantiated with input file paths
    """
    local_list = []
    if type_list is None:
        local_list = [local_obj for local_obj in _modules]
    else:
        if type(type_list) != list:
            type_list = [type_list]
        type_list = set(type_list)  # convert to set
        local_list = [local_obj for local_obj in _modules if local_obj['types'] is None or len(type_list.intersection(set(local_obj['types']))) > 0]
    return local_list


def get_by_name(name_limit=None):
    """Get parsers with a specific filter for name.
    :param name_limit: (str) list of tag type required in output (e.g. 'dsai_metadata', 'azure') (default=None or all available)
    :return list: list of raw "Parser()" classes that are instantiated with input file paths
    """
    local_list = []
    if name_limit is None:
        local_list = [local_obj for local_obj in _modules]
    else:
        local_list = [local_obj for local_obj in _modules if name_limit in local_obj['name']]
    return local_list

def empty_dataframe():
    return pd.DataFrame([], columns=["time_begin", "time_end", "source_event", "tag_type", 
                                        "time_event", "tag", "score", "details", "extractor"])
