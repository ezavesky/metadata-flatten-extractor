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
        self.EXTRACTOR = "aws_rekognition_video_content_moderation"

    @staticmethod
    def known_types():
        """Return the output types for this generator
        :return: list.  List of output types (file types) for this generator
        """
        return ['moderation']

    def parse(self, run_options):
        """Flatten AWS Rekognition Moderatioon
            - https://docs.aws.amazon.com/rekognition/latest/dg/API_GetContentModeration.html

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
                self.logger.info(f"... parsing aws_rekognition_video_content_moderation/{file_search} ")

            if "ModerationLabels" not in dict_data:
                self.logger.critical(f"Missing nested 'ModerationLabels' from source 'aws_rekognition_video_content_moderation' ({file_search})")
                return None

            for celebrity_obj in dict_data["ModerationLabels"]:  # traverse items
                if "ModerationLabel" in celebrity_obj:  # validate object
                    # "ModerationLabels": [ {  "Timestamp": 29662,   "ModerationLabel": 
                    #   {"Confidence": 71.34247589111328, "Name": "Explicit Nudity", "ParentName": ""  } },
                    local_obj = celebrity_obj["ModerationLabel"]
                    if "ParentName" in local_obj and len(local_obj["ParentName"]):   # skip over those without parent name
                        time_frame = float(celebrity_obj["Timestamp"])/1000
                        details_obj = {'category': local_obj["ParentName"]}
                        score_frame = round(float(local_obj["Confidence"])/100, self.ROUND_DIGITS)
                        list_items.append({"time_begin": time_frame, "source_event": "image",  "tag_type": "moderation",
                            "time_end": time_frame, "time_event": time_frame, "tag": local_obj["Name"],
                            "score": score_frame, "details": json.dumps(details_obj),
                            "extractor": self.EXTRACTOR})
            last_load_idx += 1

        if run_options["verbose"]:
            self.logger.critical(f"No moderation enties found in source 'aws_rekognition_video_content_moderation' ({file_search})")
        return None
