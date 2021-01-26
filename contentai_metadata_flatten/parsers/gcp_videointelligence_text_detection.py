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
import re
import json
from pandas import DataFrame

from contentai_metadata_flatten.parsers import Flatten

class Parser(Flatten):
    def __init__(self, path_content, logger=None):
        super().__init__(path_content, logger=logger)
        self.EXTRACTOR = "gcp_videointelligence_text_detection"

    @staticmethod
    def known_types():
        """Return the output types for this generator
        :return: list.  List of output types (file types) for this generator
        """
        return ['transcript']

    def parse(self, run_options):
        """Flatten GCP Text Detection 
            - https://cloud.google.com/video-intelligence/docs/feature-text-detection

        :param: run_options (dict): specific runtime information 
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """
        # read data.json
        # ...
        #   "textAnnotations": [
        #     { "text": "w", "segments": [
        #     { "segment": { "startTimeOffset": "55.555500s", "endTimeOffset": "55.805750s" },
        #       "confidence": 0.75872964,
        #       "frames": [ { "rotatedBoundingBox": { "vertices": [
        #               { "x": 0.4375, "y": 0.6875 }, { "x": 0.45546794,"y": 0.68780065 },
        #               { "x": 0.455365, "y": 0.7072443  }, {  "x": 0.43739706, "y": 0.7069436 } ] },
        #                 "timeOffset": "55.555500s" }, ] } ] ....
                         
        dict_data = self.get_extractor_results(self.EXTRACTOR, "data.json")
        if "annotationResults" not in dict_data:
            if run_options["verbose"]:
                self.logger.critical(f"Missing nested 'annotationResults' from source 'gcp_videointelligence_text_detection'")
            return None

        list_items = []
        re_time_clean = re.compile(r"s$")
        for annotation_obj in dict_data["annotationResults"]:  # traverse items
            for local_obj in annotation_obj['textAnnotations']:
                if "segments" in local_obj and "text" in local_obj:  # validate object
                    instance_obj = local_obj['segments'][0]  # TODO: are there multiple ones?
                    time_begin_clean = float(re_time_clean.sub('', instance_obj['segment']['startTimeOffset']))
                    time_end_clean = float(re_time_clean.sub('', instance_obj['segment']['endTimeOffset']))
                    score_detect = round(float(instance_obj["confidence"]), self.ROUND_DIGITS)
                    details_obj = { }
                    if "frames" in instance_obj:
                        x_min = y_min = 1
                        x_max = y_max = 0
                        for letter_obj in instance_obj["frames"]:   # we're crunching down to a bounding box over all frames, not outline
                            if "rotatedBoundingBox" in letter_obj and "vertices" in letter_obj["rotatedBoundingBox"]:
                                for vertex_obj in letter_obj["rotatedBoundingBox"]["vertices"]:
                                    if "x" in vertex_obj and "y" in vertex_obj:  # bug where "y" was not defined!?
                                        x_max = max(x_max, vertex_obj["x"])
                                        x_min = min(x_min, vertex_obj["x"])
                                        y_max = max(y_max, vertex_obj["y"])
                                        y_min = min(y_min, vertex_obj["y"])
                        details_obj['box'] = {'w': round(x_max - x_min, self.ROUND_DIGITS), 
                            'h': round(y_max - y_min, self.ROUND_DIGITS),
                            'l': round(x_min, self.ROUND_DIGITS), 't': round(y_min, self.ROUND_DIGITS) }
                    list_items.append( {"time_begin": time_begin_clean, "source_event": "ocr", "tag_type": "transcript", 
                        "time_end": time_end_clean, "time_event": time_begin_clean, "tag": local_obj['text'],
                        "score": score_detect, "details": json.dumps(details_obj), "extractor": self.EXTRACTOR})

        if len(list_items) > 0:   # return the whole thing as dataframe
            return DataFrame(list_items)

        if run_options["verbose"]:
            self.logger.critical(f"Missing nested 'textAnnotations' in annotationResults chunks from source '{self.EXTRACTOR}'")
        return None
        
