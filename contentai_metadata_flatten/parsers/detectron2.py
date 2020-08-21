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
import json
from pandas import DataFrame

from contentai_metadata_flatten.parsers import Flatten


class Parser(Flatten):
    def __init__(self, path_content, logger=None):
        super().__init__(path_content, logger=logger)
        self.EXTRACTOR = "detectron2"

    @staticmethod
    def known_types():
        """Return the output types for this generator
        :return: list.  List of output types (file types) for this generator
        """
        return ['tag']

    def parse(self, run_options):
        """Flatten Detectron Classifier
            - https://github.com/facebookresearch/detectron2/

        :param: run_options (dict): specific runtime information
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """
        list_items = []

        dict_data = self.get_extractor_results(self.EXTRACTOR, "data.json")

        for local_obj in dict_data:  # traverse items
            if "results" in local_obj or "milliseconds" in local_obj:
                # [ { "milliseconds": 208.54166666666666, "frameNumber": 6,
                #     "results": { "boxes": [ {
                #         "name": "person", "confidence": 0.9644513726234436,
                #         "boundingBox": { "left": 0.3009820302327474,
                #               "top": 0.4641285649052373, "width": 0.02381299336751302,
                #               "height": 0.1438453956886574 } 
                #         } , ... ] }, ... ]

                time_frame = round(float(local_obj["milliseconds"]) / 1000.0, self.ROUND_DIGITS)
                base_obj = { "time_begin": time_frame, "time_event": time_frame, "time_end": time_frame,
                             "tag_type": "tag", "source_event": "image", "extractor": self.EXTRACTOR }
                if "boxes" in local_obj["results"]:
                    for obj_result in local_obj["results"]:   
                        for instance_obj in local_obj["results"]["boxes"]:   # iterate through objects
                            details_obj = { 'box': {'w': round(instance_obj['boundingBox']['width'], self.ROUND_DIGITS), 
                                'h': round(instance_obj['boundingBox']['height'], self.ROUND_DIGITS),
                                'l': round(instance_obj['boundingBox']['left'], self.ROUND_DIGITS), 
                                't': round(instance_obj['boundingBox']['top'], self.ROUND_DIGITS) } }
                            score_frame = round(float(instance_obj["confidence"]), self.ROUND_DIGITS)
                            obj_insert = { "tag": instance_obj["name"], "score": score_frame, 
                                "details": json.dumps(details_obj) }
                            obj_insert.update(base_obj)
                            list_items.append(obj_insert)

        if len(list_items) > 0:   # return the whole thing as dataframe
            return DataFrame(list_items)

        if run_options["verbose"]:
            self.logger.critical(f"No tag entries found in source '{self.EXTRACTOR}'")
        return None
