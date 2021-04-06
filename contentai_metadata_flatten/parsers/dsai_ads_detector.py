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
        self.TAG_TYPE = "commercial_lead"

    @staticmethod
    def known_types():
        """Return the output types for this generator
        :return: list.  List of output types (file types) for this generator
        """
        return ['scene']

    def parse(self, run_options):
        """dsai_ads_detector

        :param: run_options (dict): specific runtime information
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """
        dict_data = self.get_extractor_results(self.EXTRACTOR, "data.json")

        # "ads_scenes": [
        #     {
        #         "time_begin": 0,
        #         "time_end": 1.5,
        #         "score": 0.6567,
        #     },


        if "ads_scenes" in dict_data:  # overall validation
            list_items = []
            base_obj = {"source_event": "video", "tag_type": self.TAG_TYPE, "details": "",
                        "tag": "scene", "extractor": self.EXTRACTOR}

            for scene_obj in dict_data["ads_scenes"]:
                scene_obj.update(base_obj)
                scene_obj['time_event'] = scene_obj['time_end']   # indicating an ad should go here!
                scene_obj['score'] = round(scene_obj['score'], self.ROUND_DIGITS)
                list_items.append(scene_obj)

            if len(list_items) > 0:   # return the whole thing as dataframe
                return DataFrame(list_items)

        if run_options["verbose"]:
            self.logger.critical(f"Missing ad_scenes from source {self.EXTRACTOR}")
        return None
