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

from contentai_metadata_flatten.parsers import Flatten


class Parser(Flatten):
    def __init__(self, path_content, logger=None):
        super().__init__(path_content, logger=logger)
        self.EXTRACTOR = "aws_rekognition_video_text_detect"

    @staticmethod
    def known_types():
        """Return the output types for this generator
        :return: list.  List of output types (file types) for this generator
        """
        return ['tag', 'transcript']

    def parse(self, run_options):
        """Flatten Video Text detection
        https://docs.aws.amazon.com/rekognition/latest/dg/text-detection.html

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
                self.logger.info(f"... parsing {self.EXTRACTOR}/{file_search} ")

            if "TextDetections" not in dict_data:
                self.logger.critical(f"Missing nested 'TextDetections' from source '{self.EXTRACTOR}' ({file_search})")
                return None

            for local_obj in dict_data['TextDetections']:
                if "Timestamp" in local_obj and "TextDetection" in local_obj:  # validate object
                    time_begin = round(float(local_obj['Timestamp']) / 1000.0, self.ROUND_DIGITS)
                    instance_obj = local_obj['TextDetection']
                    score_detect = round(float(instance_obj["Confidence"]) / 100, self.ROUND_DIGITS)
                    details_obj = { }
                    if "Geometry" in instance_obj and instance_obj["Geometry"]["BoundingBox"]:   # make sure geometry is valid
                        details_obj['box'] = {'w': round(instance_obj["Geometry"]['BoundingBox']['Width'], self.ROUND_DIGITS), 
                            'h': round(instance_obj["Geometry"]['BoundingBox']['Height'], self.ROUND_DIGITS),
                            'l': round(instance_obj["Geometry"]['BoundingBox']['Left'], self.ROUND_DIGITS), 
                            't': round(instance_obj["Geometry"]['BoundingBox']['Top'], self.ROUND_DIGITS) }
                    text_type = instance_obj['Type'].lower()
                    if text_type == "line":   # either line (transcript)
                        details_obj['transcript'] = instance_obj['DetectedText']
                        list_items.append( {"time_begin": time_begin, "source_event": "ocr", "tag_type": "transcript",
                            "time_end": time_begin, "time_event": time_begin, "tag": Flatten.TAG_TRANSCRIPT,
                            "score": score_detect, "details": json.dumps(details_obj), "extractor": self.EXTRACTOR})
                    elif text_type == "word":   # or word
                        list_items.append( {"time_begin": time_begin, "source_event": "ocr", "tag_type": "word", 
                            "time_end": time_begin, "time_event": time_begin, "tag": instance_obj['DetectedText'],
                            "score": score_detect, "details": json.dumps(details_obj), "extractor": self.EXTRACTOR})

            last_load_idx += 1

        if len(list_items) > 0:   # return the whole thing as dataframe
            return DataFrame(list_items)

        if run_options["verbose"]:
            self.logger.critical(f"Missing nested 'TextDetections' or 'videos' from source '{self.EXTRACTOR}'")
        return None
