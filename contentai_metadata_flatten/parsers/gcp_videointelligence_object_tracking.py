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
import math
from pandas import DataFrame

from contentai_metadata_flatten.parsers import Flatten

class Parser(Flatten):
    def __init__(self, path_content, logger=None):
        super().__init__(path_content, logger=logger)
        self.EXTRACTOR = "gcp_videointelligence_object_tracking"

    @staticmethod
    def known_types():
        """Return the output types for this generator
        :return: list.  List of output types (file types) for this generator
        """
        return ['tag']

    def parse(self, run_options):
        """Flatten GCP Object Tracking - https://cloud.google.com/video-intelligence/docs/object-tracking?

        :param: run_options (dict): specific runtime information ('all_frames'=True/False for all logo mapping)
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """
        # read data.json
        #   {  "annotationResults": [ { "inputUri": "/contentai-content/vmlr-contentai-ro/public/HBOMax-Launch.mp4",
        #       "segment": { "startTimeOffset": "0s", "endTimeOffset": "70.028291s" },
        #       "objectAnnotations": [ { 
        #           "entity": { "entityId": "/m/01g317", "description": "person", "languageCode": "en-US" },
        #           "frames": [ 
        #               { "normalizedBoundingBox": { "left": 0.44509378, "top": 0.6993372, "right": 0.4970483,  "bottom": 0.9154036 }, "timeOffset": "0s"},
        #               {  "normalizedBoundingBox": {  "left": 0.44609225, "top": 0.69999903, "right": 0.49843538, "bottom": 0.9166648  }, "timeOffset": "0.125125s"},
        #               ... ],
        #           "segment": { "startTimeOffset": "0s",  "endTimeOffset": "0.750750s" },
        #           "confidence": 0.870625
        #       } ]
        #       ... }

        dict_data = self.get_extractor_results(self.EXTRACTOR, "data.json")
        if "annotationResults" not in dict_data:
            if run_options["verbose"]:
                self.logger.critical(f"Missing nested 'annotationResults' from source '{self.EXTRACTOR}'")
            return None

        list_items = []
        re_time_clean = re.compile(r"s$")
        for annotation_obj in dict_data["annotationResults"]:  # traverse items
            if "objectAnnotations" in annotation_obj:  # validate object
                for object_item in annotation_obj["objectAnnotations"]:
                    details_obj = {}
                    if "entity" not in object_item:
                        self.logger.critical(f"Missing nested 'entity' in object chunk '{object_item}'")
                        return None
                    details_obj["entity"] = object_item["entity"]["entityId"]
                    if "frames" in object_item:   # validate data 
                        details_obj['box'] = []
                        for frame_item in object_item["frames"]:
                            if "normalizedBoundingBox" in frame_item and \
                                    'left' in frame_item['normalizedBoundingBox'] and \
                                    'top' in frame_item['normalizedBoundingBox']:   # pull box for one item
                                local_box = {'w': round(frame_item['normalizedBoundingBox']['right'], self.ROUND_DIGITS), 
                                    'h': round(frame_item['normalizedBoundingBox']['bottom'], self.ROUND_DIGITS),
                                    'l': round(frame_item['normalizedBoundingBox']['left'], self.ROUND_DIGITS), 
                                    't': round(frame_item['normalizedBoundingBox']['top'], self.ROUND_DIGITS) }
                                local_box['w'] -= local_box['l']
                                local_box['h'] -= local_box['t']
                                local_box["o"] = round(float(re_time_clean.sub('', frame_item["timeOffset"])), self.ROUND_DIGITS)
                                details_obj['box'].append(local_box)
                    if "confidence" in object_item:
                        time_begin = round(float(re_time_clean.sub('', object_item["segment"]["startTimeOffset"])), self.ROUND_DIGITS)
                        list_items.append( {
                            "time_begin": time_begin, "time_event": time_begin,
                            "time_end": round(float(re_time_clean.sub('', object_item["segment"]["endTimeOffset"])), self.ROUND_DIGITS), 
                            "source_event": "video", "tag": object_item["entity"]["description"], "tag_type": "tag",
                            "score": round(object_item["confidence"], self.ROUND_DIGITS), "details": json.dumps(details_obj), 
                            "extractor": self.EXTRACTOR})
        if list_items:
            return DataFrame(list_items)

        if run_options["verbose"]:
            self.logger.critical(f"Missing nested 'objectAnnotations' from source '{self.EXTRACTOR}'")
        return None      
