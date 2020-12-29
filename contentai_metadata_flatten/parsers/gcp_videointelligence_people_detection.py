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
import numpy as np

from contentai_metadata_flatten.parsers import Flatten

class Parser(Flatten):
    def __init__(self, path_content, logger=None):
        super().__init__(path_content, logger=logger)
        self.EXTRACTOR = "gcp_videointelligence_people_detection"

    @staticmethod
    def known_types():
        """Return the output types for this generator
        :return: list.  List of output types (file types) for this generator
        """
        return ['tag', 'person']

    def parse(self, run_options):
        """Flatten GCP People Detection - https://cloud.google.com/video-intelligence/docs/people-detection

        :param: run_options (dict): specific runtime information ('all_frames'=True/False for all logo mapping)
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """
        # read data.json
        #   {  "annotationResults": [ { "inputUri": "/contentai-content/vmlr-contentai-ro/public/HBOMax-Launch.mp4",
        #       "segment": { "startTimeOffset": "0s", "endTimeOffset": "70.028291s" },
        #       "personDetectionAnnotations": [
        #           "tracks": [ { "segment": { "startTimeOffset": "0s", "endTimeOffset": "0.750750s"  },
        #           "timestampedObjects": [ { 
        #               "normalizedBoundingBox": { "left": 0.44453126, "top": 0.69861114, "right": 0.49687502, "bottom": 0.91527784 },
        #               "timeOffset": "0s",
        #               "attributes": [ {  "name": "UpperCloth",  "confidence": 0.6872854, "value": "Plain" }, ... ]
        #               "landmarks": [  { "name": "left_ear", "point": { "x": 0.463835, "y": 0.7129844 }, "confidence": 0.7135438 } ... ]
        #           } ] ] }

        dict_data = self.get_extractor_results(self.EXTRACTOR, "data.json")
        if "annotationResults" not in dict_data:
            if run_options["verbose"]:
                self.logger.critical(f"Missing nested 'annotationResults' from source '{self.EXTRACTOR}'")
            return None

        list_items = []
        re_time_clean = re.compile(r"s$")
        for annotation_obj in dict_data["annotationResults"]:  # traverse items
            if "personDetectionAnnotations" in annotation_obj:  # validate object
                time_begin = round(float(re_time_clean.sub('', annotation_obj["segment"]["startTimeOffset"])), self.ROUND_DIGITS)
                time_end = round(float(re_time_clean.sub('', annotation_obj["segment"]["endTimeOffset"])), self.ROUND_DIGITS)

                for object_item in annotation_obj["personDetectionAnnotations"]:
                    if "tracks" in object_item:   # validate data 
                        for track_item in object_item["tracks"]:
                            for timed_item in track_item["timestampedObjects"]:
                                # extract box
                                if "normalizedBoundingBox" in timed_item and \
                                        'left' in timed_item['normalizedBoundingBox'] and \
                                        'top' in timed_item['normalizedBoundingBox']:   # pull box for one item
                                    local_box = {'w': round(timed_item['normalizedBoundingBox']['right'], self.ROUND_DIGITS), 
                                        'h': round(timed_item['normalizedBoundingBox']['bottom'], self.ROUND_DIGITS),
                                        'l': round(timed_item['normalizedBoundingBox']['left'], self.ROUND_DIGITS), 
                                        't': round(timed_item['normalizedBoundingBox']['top'], self.ROUND_DIGITS)}
                                    local_box['w'] -= local_box['l']
                                    local_box['h'] -= local_box['t']
                                    time_event = round(float(re_time_clean.sub('', timed_item["timeOffset"])), self.ROUND_DIGITS)

                                    # extract attributes as regular tag, but person-sourced
                                    if "attributes" in timed_item:
                                        for attr_item in timed_item["attributes"]:
                                            details_obj = {'box':local_box, 'category': attr_item['name']}
                                            list_items.append({
                                                "time_begin": time_begin, "time_event": time_event, "time_end": time_end,
                                                "source_event": "video", "tag_type": "tag",
                                                "tag": attr_item["value"], 
                                                "score": round(attr_item["confidence"], self.ROUND_DIGITS), 
                                                "details": json.dumps(details_obj), 
                                                "extractor": self.EXTRACTOR})
                                    # end attributes

                                    # extract skeleton information for people
                                    if "landmarks" in timed_item:
                                        local_skeleton = {}
                                        avg_score = []
                                        for landmark_item in timed_item["landmarks"]:
                                            avg_score.append(round(landmark_item["confidence"], self.ROUND_DIGITS))
                                            local_skeleton[landmark_item["name"]] = {
                                                'l': round(landmark_item['point']['x'], self.ROUND_DIGITS), 
                                                't': round(landmark_item['point']['y'], self.ROUND_DIGITS)}
                                        if avg_score:
                                            avg_score = round(np.average(avg_score), self.ROUND_DIGITS)
                                            details_obj = {'box':local_box, 'category': attr_item['name']}
                                            list_items.append({
                                                "time_begin": time_begin, "time_event": time_event, "time_end": time_end,
                                                "source_event": "image", "tag_type": "person",
                                                "tag": "Skeleton", 
                                                "score": round(attr_item["confidence"], self.ROUND_DIGITS), 
                                                "details": json.dumps(local_skeleton), 
                                                "extractor": self.EXTRACTOR})
                                    # end landmark parse
                            # end "timestampedObjects" parsing
                # end "personDetectionAnnotations" parsing
        # end "annotationResults" parsing                    

        if list_items:
            return DataFrame(list_items)

        if run_options["verbose"]:
            self.logger.critical(f"Missing nested 'personDetectionAnnotations' from source '{self.EXTRACTOR}'")
        return None      
