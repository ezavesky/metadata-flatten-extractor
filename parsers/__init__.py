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
import json
import re
import math
import gzip
from os import path

import logging
import warnings
from sys import stdout as STDOUT

import pandas as pd

import contentai

modules = [name for _, name, _ in pkgutil.iter_modules(__path__)]

class Flatten():
    # https://cloud.google.com/video-intelligence/docs/reference/reast/Shared.Types/Likelihood
    GCP_LIKELIHOOD_MAP = { "LIKELIHOOD_UNSPECIFIED": 0.0, "VERY_UNLIKELY": 0.1, "UNLIKELY": 0.25,
                           "POSSIBLE": 0.5, "LIKELY": 0.75, "VERY_LIKELY": 0.9 }
    TAG_TRANSCRIPT = "_transcript_"

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(STDOUT)
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    def __init__(self, path_content):
        super().__init__()
        self.extractor_keys = []
        self.extractor = None
        self.path_content = path_content

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

    def get_extractor_results(self, extractor_name, path, force_retrieve=False):
        if not force_retrieve:  # safe way to request without 404/500 error
            if len(self.extractor_keys) < 1 or self.extractor != extractor_name:  
                self.extractor_keys = []
                self.extractor_name = extractor_name
                dict_raw = self.get_extractor_keys(extractor_name)
                # except urllib.error.HTTPError as e:
                if dict_raw is not None and 'keys' in dict_raw:
                    self.extractor_keys = dict_raw['keys']
                    self.logger.info(f"Retrieved available keys {self.extractor_keys} for extractor {self.extractor_name} ")
            if path not in self.extractor_keys:   # have the keys, check for presence
                return None
        return contentai.get_extractor_results(extractor_name, path)   # checked or brute force request

    def get_extractor_keys(self, extractor_name):
        return contentai.get_extractor_result_keys(extractor_name)

