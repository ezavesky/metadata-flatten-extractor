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

from pandas import DataFrame
from os import path
import json

from contentai_metadata_flatten.parsers import Flatten

class Parser(Flatten):
    def __init__(self, path_content, logger=None):
        super().__init__(path_content, logger=logger)
        self.EXTRACTOR = "aws_rekognition_video_celebs"

    @staticmethod
    def known_types():
        """Return the output types for this generator
        :return: list.  List of output types (file types) for this generator
        """
        return ['identity']

    def parse(self, run_options):
        """Flatten AWS Rekognition celebrities
            - https://docs.aws.amazon.com/rekognition/latest/dg/celebrities-procedure-image.html

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
                self.logger.info(f"... parsing aws_rekognition_video_celebs/{file_search} ")

            if "Celebrities" not in dict_data:
                self.logger.critical(f"Missing nested 'Celebrities' from source 'aws_rekognition_video_celebs' ({file_search})")
                return None

            for celebrity_obj in dict_data["Celebrities"]:  # traverse items
                if "Celebrity" in celebrity_obj:  # validate object
                    local_obj = celebrity_obj["Celebrity"]
                    time_frame = float(celebrity_obj["Timestamp"])/1000
                    details_obj = {}
                    if "BoundingBox" in local_obj:
                        details_obj['box'] = {'w': round(local_obj['BoundingBox']['Width'], self.ROUND_DIGITS), 
                            'h': round(local_obj['BoundingBox']['Height'], self.ROUND_DIGITS),
                            'l': round(local_obj['BoundingBox']['Left'], self.ROUND_DIGITS), 
                            't': round(local_obj['BoundingBox']['Top'], self.ROUND_DIGITS) }
                    if "Urls" in local_obj and local_obj["Urls"]:
                        details_obj['urls'] = ",".join(local_obj["Urls"])
                    score_frame = round(float(local_obj["Confidence"])/100, self.ROUND_DIGITS                                                                                  )

                    list_items.append({"time_begin": time_frame, "source_event": "face", "tag_type": "identity",
                        "time_end": time_frame, "time_event": time_frame, "tag": local_obj["Name"],
                        "score": score_frame, "details": json.dumps(details_obj),
                        "extractor": self.EXTRACTOR})
            last_load_idx += 1

        if run_options["verbose"]:
            self.logger.critical(f"No celebrity enties found in source 'aws_rekognition_video_celebs' ({file_search})")
        return None
        
