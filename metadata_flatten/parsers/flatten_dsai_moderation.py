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

from pytimeparse import parse as pt_parse

from metadata_flatten.parsers import Flatten

class Parser(Flatten):
    def __init__(self, path_content):
        super().__init__(path_content)
        self.EXTRACTOR = "dsai_moderation"

    @staticmethod
    def known_types():
        """Return the output types for this generator
        :return: list.  List of output types (file types) for this generator
        """
        return ['moderation']

    def parse(self, run_options):
        """Flatten moderation score results

        :param: run_options (dict): specific runtime information
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """
        dict_data = self.get_extractor_results(self.EXTRACTOR, "data.json", is_json=True)
        if not dict_data:  # do we need to load it locally?
            if 'extractor' in run_options:
                path_content = path.join(self.path_content, "data.json")
            else:
                path_content = path.join(self.path_content, self.EXTRACTOR, "data.json")
            if not path.exists(path_content):
                path_content += ".gz"
            dict_data = self.json_load(path_content)
        if len(dict_data) < 1:
            if run_options["verbose"]:
                self.logger.critical(f"Empty result string for extractor '{self.EXTRACTOR}', aborting")
            return None

        # "results": [ ...
        # {
        #     "sexy": "0.033553965",    -> "racy"
        #     "drawings": "0.03447094", -> "cartoon"
        #     "hentai": "0.038157757",  -> "explicit drawing"
        #     "neutral": "0.15495029",  -> "neutral"
        #     "porn": "0.7388671",      -> "pornography"
        #     "time_event": 0.9592916666666667,
        #     "time_frame": 24
        # },

        score_mapping = {"sexy": "racy", "drawings": "drawing", "hentai": "explicit drawing", 
                         "neutral": "neutral", "porn": "pornography"}
        list_items = []

        if dict_data is None or 'results' not in dict_data or 'config' not in dict_data:
            self.logger.critical(f"Missing nested 'results' from source '{self.EXTRACTOR}'")
            return None
        for local_obj in dict_data["results"]:
            if "time_event" in local_obj and "neutral" in local_obj:  # validate object
                time_event = float(local_obj['time_event'])
                for score_original in score_mapping:
                    if score_original in local_obj:
                        list_items.append({"time_begin": time_event, "source_event": "image", "tag_type": "moderation",
                            "time_end": time_event, "time_event": time_event, "tag": score_mapping[score_original],
                            "score":  local_obj[score_original], "details": "", "extractor": self.EXTRACTOR})

        if len(list_items) > 0:   # return the whole thing as dataframe
            return DataFrame(list_items)

        if run_options["verbose"]:
            self.logger.critical(f"No valid events detected for '{self.EXTRACTOR}'")
        return None
