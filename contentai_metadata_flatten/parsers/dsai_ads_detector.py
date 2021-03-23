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

from os import path
from pandas import DataFrame
import json
import re

from pytimeparse import parse as pt_parse

from contentai_metadata_flatten.parsers import Flatten

class Parser(Flatten):
    def __init__(self, path_content, logger=None):
        super().__init__(path_content, logger=logger)
        self.EXTRACTOR = "dsai_ads_detector"


    @staticmethod
    def known_types():
        """Return the output types for this generator
        :return: list.  List of output types (file types) for this generator
        """
        return ['ad_scenes']



    def parse(self, run_options):
        """dsai_ads_detector

        :param: run_options (dict): specific runtime information
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """
        dict_data = self.get_extractor_results(self.EXTRACTOR, "data.json")
        re_time_clean = re.compile(r"s$")
        list_items = []

        # "ad_scenes": [
        #     {
        #         "time_begin": 0,
        #         "time_end": 1.5,
        #         "score": 0.6567,
        #     },

        if "ad_scenes" in dict_data:  # overall validation
            scenes = dict_data["ad_scenes"] or {}

        if len(scenes) > 0:   # return the whole thing as dataframe
            return DataFrame(scenes)

        if run_options["verbose"]:
            self.logger.critical(f"Missing ad_scenes from source dsai_ads_detector")
        return None
