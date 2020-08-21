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
import json

from pytimeparse import parse as pt_parse

# NOTE: we reuse the parser (also CSV source) for this type as well
from contentai_metadata_flatten.parsers.dsai_activity_slowfast import Parser as ParserBase
# NOTE: non-CSV parser (JSON) will use core flattener
from contentai_metadata_flatten.parsers import Flatten

class ParserLegacy(ParserBase):
    def __init__(self, path_content, logger=None):
        super().__init__(path_content, logger=logger)
        self.EXTRACTOR = "dsai_places"

    def get_source_types(self, column_clean):
        # (places)
        # file,Time_begin,Time_end,Time_event,label_id0,label0,probability0,label_id1,label1,probability1,label_id2,label2,probability2,label_id3,label3,probability3,label_id4,label4,probability4
        # output000001.png,2,2,2,231,motel,0.158193097,122,discotheque,0.070194781,158,gas_station,0.062356248,129,elevator/door,0.059626624,177,home_theater,0.055273291

        if "file" in column_clean:  # suspect it's scene images
            return {'type': "image", 'column_prefix':['label', 'probability']}
        return None


class Parser(Flatten):
    def __init__(self, path_content, logger=None):
        super().__init__(path_content, logger=logger)
        self.EXTRACTOR = "dsai_places"
        self.parser_legacy = ParserLegacy(path_content)

    @staticmethod
    def known_types():
        """Return the output types for this generator
        :return: list.  List of output types (file types) for this generator
        """
        return ['tag']

    def parse(self, run_options):
        """Flatten SlowFast actions results

        :param: run_options (dict): specific runtime information
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """
        dict_data = self.get_extractor_results(self.EXTRACTOR, "data.json")
        if not dict_data:
            self.logger.info("Attempting legacy 'dsai_places' analysis")
            return self.parser_legacy.parse(run_options)
       
        # {
        #     "config": {
        #         "version": "1.0.0",
        #         "extractor": "places-extractor",
        #         "input": "/var/contentai/content/HBOMax-Launch.mp4",
        #         "timestamp": "2020-06-03 19:56:32.162248"
        #     },
        #     "results": [
        #         {
        #             "time_event": 0.95929,
        #             "index_frame": 24,
        #             "scores": {
        #                    "discotheque": 0.54629,
        #                    "beauty_salon": 0.1958,
        #                    "elevator/door": 0.04798,
        #                    "elevator_lobby": 0.03297,
        #                    "stage/indoor": 0.03135
        #             }
        #         },

        list_items = []

        if dict_data is None or 'results' not in dict_data or 'config' not in dict_data:
            self.logger.critical(f"Missing nested 'results' from source '{self.EXTRACTOR}'")
            return None
        for local_obj in dict_data["results"]:
            score_obj = local_obj
            if "scores" in score_obj:  # validate object
                score_obj = local_obj["scores"]
            if "time_event" in local_obj:  # validate object
                time_event = float(local_obj['time_event'])
                for score_original in score_obj:
                    local_score = float(score_obj[score_original])
                    list_items.append({"time_begin": time_event, "source_event": "image", "tag_type": "tag",
                        "time_end": time_event, "time_event": time_event, "tag": score_original,
                        "score": local_score, "details": "", "extractor": self.EXTRACTOR})

        if len(list_items) > 0:   # return the whole thing as dataframe
            return DataFrame(list_items)

        if run_options["verbose"]:
            self.logger.critical(f"No valid events detected for '{self.EXTRACTOR}'")
        return None
