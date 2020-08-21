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

from contentai_metadata_flatten.parsers import Flatten

class Parser(Flatten):
    def __init__(self, path_content, logger=None):
        super().__init__(path_content, logger=logger)
        self.EXTRACTOR = "dsai_sceneboundary"

    @staticmethod
    def known_types():
        """Return the output types for this generator
        :return: list.  List of output types (file types) for this generator
        """
        return ['scene']

    def parse(self, run_options):
        """Flatten DSAI Sceneboundary results

        :param: run_options (dict): specific runtime information
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """
        dict_data = self.get_extractor_results(self.EXTRACTOR, "data.json")

        list_timing = {}
        if "shots" in dict_data:  # look-up timing items first 
            for local_obj in dict_data['shots']:
                list_timing[local_obj['id']] = {"time_begin": local_obj['time_begin'], "time_end": local_obj['time_end']}
        if not dict_data:  # valid load?
            if run_options["verbose"]:
                self.logger.critical(f"Missing timing array for extractor '{self.EXTRACTOR}', aborting")
            return None

        list_items = []
        if "annotations" in dict_data and len(dict_data["annotations"]):  # validate known format 
            for local_obj in dict_data['annotations']:
                if "annotator" in local_obj and local_obj["annotator"]["name"] == "sceneboundary":
                    detail_obj = {"version":local_obj["annotator"]["name"], 
                                  "timestamp": local_obj["annotator"]["timestamp"],
                                  "threshold": local_obj["classifier"]["threshold"],
                                  "frame_position": local_obj["classifier"]["frame_position"]}

                    for insight_obj in local_obj['segments']:  # walk through the segments/scenes
                        detail_local = {"id": insight_obj["id"], "shots": len(insight_obj["shots"])}
                        detail_local.update(detail_obj)
                        if (insight_obj["shots"][0] not in list_timing) or (insight_obj["shots"][-1] not in list_timing):
                            self.logger.critical(f"Missing timing array for extractor '{self.EXTRACTOR}', aborting")
                            return None

                        list_items.append( {"time_begin": list_timing[insight_obj["shots"][0]]["time_begin"], 
                            "time_end": list_timing[insight_obj["shots"][-1]]["time_end"], 
                            "source_event": "video", "tag_type": "scene", "tag": "scene",
                            "score": insight_obj["score"], "details": json.dumps(detail_local),
                            "extractor": self.EXTRACTOR})

        if len(list_items) > 0:   # return the whole thing as dataframe
            return DataFrame(list_items)

        if run_options["verbose"]:
            self.logger.critical(f"Missing nested sections from source '{self.EXTRACTOR}'")
        return None
