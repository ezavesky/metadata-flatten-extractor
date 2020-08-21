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
        self.EXTRACTOR = "aws_rekognition_video_labels"

    @staticmethod
    def known_types():
        """Return the output types for this generator
        :return: list.  List of output types (file types) for this generator
        """
        return ['tag']

    def parse(self, run_options):
        """Flatten AWS Video Labels
            - https://docs.aws.amazon.com/rekognition/latest/dg/labels-detecting-labels-video.html

        :param: run_options (dict): specific runtime information
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """
        list_items = []
        last_load_idx = 0
        while last_load_idx >= 0:
            file_search = f"result{last_load_idx}.json"
            dict_data = self.get_extractor_results(self.EXTRACTOR, file_search)
            if not dict_data:  # couldn't load anything else...
                if list_items:
                    return DataFrame(list_items)
                else:
                    last_load_idx = -1
                    break

            if run_options["verbose"]:
                self.logger.info(f"... parsing aws_rekognition_video_labels/{file_search} ")

            if "Labels" not in dict_data:
                self.logger.critical(f"Missing nested 'Labels' from source 'aws_rekognition_video_labels' ({file_search})")
                return None

            for labeled_obj in dict_data["Labels"]:  # traverse items
                if "Label" in labeled_obj:  # validate object
                    # " { "Timestamp": 0, "Label": { "Name": "Train", "Confidence": 62.60573959350586, "Instances": [
                    # { "BoundingBox": { "Width": 0.224, "Height": 0.2151, "Left": 0.722, "Top": 0.350 },
                    # "Confidence": 62.73824691772461 } ], "Parents": [{ "Name": "Vehicle" }, { "Name": "Transportation" } ] }
                    time_frame = float(labeled_obj["Timestamp"])/1000
                    details_obj = {}
                    local_obj = labeled_obj["Label"]
                    if "Parents" in local_obj and len(local_obj["Parents"]):   # skip over those without parent name
                        details_obj = {'category': [p["Name"] for p in local_obj["Parents"]]}
                    if "Instances" in local_obj and len(local_obj["Instances"]):
                        for instance_obj in local_obj["Instances"]:  # treat each box independently
                            details_obj['box'] = {'w': round(instance_obj['BoundingBox']['Width'], self.ROUND_DIGITS), 
                                'h': round(instance_obj['BoundingBox']['Height'], self.ROUND_DIGITS),
                                'l': round(instance_obj['BoundingBox']['Left'], self.ROUND_DIGITS), 
                                't': round(instance_obj['BoundingBox']['Top'], self.ROUND_DIGITS) }

                            score_frame = round(float(instance_obj["Confidence"])/100, self.ROUND_DIGITS)
                            list_items.append({"time_begin": time_frame, "source_event": "image",  "tag_type": "tag",
                                "time_end": time_frame, "time_event": time_frame, "tag": local_obj["Name"],
                                "score": score_frame, "details": json.dumps(details_obj),
                                "extractor": self.EXTRACTOR})
            last_load_idx += 1

        if run_options["verbose"]:
            self.logger.critical(f"No moderation enties found in source 'aws_rekognition_video_labels' ({file_search})")
        return None
