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
from pandas import DataFrame, read_csv
from io import StringIO
import json

from pytimeparse import parse as pt_parse

from contentai_metadata_flatten.parsers import Flatten


class Parser(Flatten):
    def __init__(self, path_content, logger=None):
        super().__init__(path_content, logger=logger)
        self.EXTRACTOR = "comskip_json"

    @staticmethod
    def known_types():
        """Return the output types for this generator
        :return: list.  List of output types (file types) for this generator
        """
        return ['scene']

    def parse(self, run_options):
        """Flatten SlowFast actions results

        :param: run_options (dict): specific runtime information
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """
        dict_data = self.get_extractor_results(self.EXTRACTOR, "data.json")
        if "commercials" not in dict_data:
            if run_options["verbose"]:
                self.logger.critical(f"Missing nested 'commercials' from source 'comskip_json' VERBOSE: {dict_data}")
            return None

        # (parsed JSON format)
        # {"commercials": [{"start": 0.03336666666666667, "end": 22.355666666666668}, 
        # {"start": 109.54276666666667, "end": 295.92896666666667}, {"start": 809.6421666666666, "end": 1011.5772333333333}, 
        # {"start": 1257.9900666666667, "end": 1484.3161666666667}, {"start": 1799.9982, "end": 1800.0315666666668}]}

        base_obj = {"source_event": "video", "tag_type": "scene", "tag": "commercial",
                    "extractor": self.EXTRACTOR, "score": self.SCORE_DEFAULT}

        list_items = []
        for annotation_obj in dict_data["commercials"]:  # traverse items
            if "start" in annotation_obj and "end" in annotation_obj:  # validate object
                item_new = {"time_begin": round(annotation_obj["start"], self.ROUND_DIGITS),
                            "time_end": round(annotation_obj["end"], self.ROUND_DIGITS),
                            "time_event": round(annotation_obj["start"], self.ROUND_DIGITS), "details": ""}
                item_new.update(base_obj)
                list_items.append(item_new)

        if len(list_items) > 0:   # return the whole thing as dataframe
            return DataFrame(list_items)

        if run_options["verbose"]:
            self.logger.critical(f"No valid events detected for '{self.EXTRACTOR}'")
        return None

