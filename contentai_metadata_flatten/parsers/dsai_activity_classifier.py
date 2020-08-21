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
        self.EXTRACTOR = "dsai_activity_classifier"

    @staticmethod
    def known_types():
        """Return the output types for this generator
        :return: list.  List of output types (file types) for this generator
        """
        return ['tag']

    def parse(self, run_options):
        """Flatten activity classifier score results

        :param: run_options (dict): specific runtime information
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """
        dict_data = self.get_extractor_results(self.EXTRACTOR, "data.json", is_json=True)
        if not dict_data:
            if run_options["verbose"]:
                self.logger.critical(f"Empty result string for extractor '{self.EXTRACTOR}', aborting")
            return None

        # "results": [
        #     {
        #         "time_begin": 0,
        #         "time_end": 1.5,
        #         "type_audio": "",
        #         "type_video": "dsai_videocnn",
        #         "score": 0.28173,
        #         "class": "BuildingExplode"
        #     },

        list_items = []

        if dict_data is None or 'results' not in dict_data or 'config' not in dict_data:
            self.logger.critical(f"Missing nested 'results' from source '{self.EXTRACTOR}'")
            return None
        for local_obj in dict_data["results"]:
            if "class" in local_obj and "score" in local_obj:  # validate object
                time_begin = float(local_obj['time_begin'])
                time_end = float(local_obj['time_end'])
                details_obj = {}
                source_type = 'video'
                if 'type_video' in local_obj and len(local_obj['type_video']) > 0:
                    details_obj['video'] = local_obj['type_video']
                if 'type_audio' in local_obj and len(local_obj['type_audio']) > 0:
                    details_obj['audio'] = local_obj['type_audio']
                    if "video" not in details_obj:
                        source_type = 'audio'
                list_items.append({"time_begin": time_begin, "source_event": source_type, "tag_type": "tag",
                    "time_end": time_end, "time_event": time_begin, "tag": local_obj["class"],
                    "score":  local_obj['score'], "details": json.dumps(details_obj),
                    "extractor": self.EXTRACTOR})

        if len(list_items) > 0:   # return the whole thing as dataframe
            return DataFrame(list_items)

        if run_options["verbose"]:
            self.logger.critical(f"No valid events detected for '{self.EXTRACTOR}'")
        return None
