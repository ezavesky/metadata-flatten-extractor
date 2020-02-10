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

from . import Flatten


class Parser(Flatten):
    def __init__(self, path_content):
        super().__init__(path_content)

    def parse(self, run_options):
        """Flatten GCP Logo Recognition - https://cloud.google.com/video-intelligence/docs/logo-recognition?

        :param: run_options (dict): specific runtime information ('all_frames'=True/False for all logo mapping)
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """
        # read data.json
        #   "annotationResults": [ "logoRecognitionAnnotations": [ {
        #    "entity": { "entityId": "/m/01_8w2", "description": "CBS News",  "languageCode": "en-US" },
        #    "tracks": [ { "segment": { "startTimeOffset": "168s",  "endTimeOffset": "171.600s" },
        #     "timestampedObjects": [ { "normalizedBoundingBox": { "left": 0.439,
        #     "top": 0.717, "right": 0.482, "bottom": 0.822  },  "timeOffset": "168s"}, ...
        dict_data = self.get_extractor_results("gcp_videointelligence_logo_recognition", "data.json")
        if not dict_data:  # do we need to load it locally?
            path_content = path.join(self.path_content, "gcp_videointelligence_logo_recognition", "data.json")
            dict_data = self.json_load(path_content)
            if not dict_data:
                path_content += ".gz"
                dict_data = self.json_load(path_content)

        if "annotationResults" not in dict_data:
            self.logger.critical(f"Missing nested 'annotationResults' from source 'gcp_videointelligence_logo_recognition'")
            return None

        re_time_clean = re.compile(r"s$")
        for annotation_obj in dict_data["annotationResults"]:  # traverse items
            if "logoRecognitionAnnotations" in annotation_obj:  # validate object
                list_items = []
                for logo_item in annotation_obj["logoRecognitionAnnotations"]:
                    details_obj = {}
                    if "entity" not in logo_item:
                        self.logger.critical(f"Missing nested 'entity' in logo chunk '{logo_item}'")
                        return None
                    details_obj["entity"] = logo_item["entity"]["entityId"]
                    if "tracks" in logo_item:   # validate data 
                        for track_item in logo_item["tracks"]:
                            idx_boxes = [math.floor(len(track_item["timestampedObjects"]) // 2)]   # roughly grab center item
                            if 'all_frames' in run_options and run_options['all_frames']:  # save all instead of single frame?
                                idx_boxes = range(len(track_item["timestampedObjects"]))
                            details_obj['box'] = []
                            for timestamp_idx in idx_boxes:
                                timestamped_item = track_item["timestampedObjects"][timestamp_idx] 
                                if "normalizedBoundingBox" in timestamped_item and \
                                        'left' in timestamped_item['normalizedBoundingBox'] and \
                                        'top' in timestamped_item['normalizedBoundingBox']:   # pull box for one item
                                    local_box = {'w': round(timestamped_item['normalizedBoundingBox']['right'], 4), 
                                        'h': round(timestamped_item['normalizedBoundingBox']['bottom'], 4),
                                        'l': round(timestamped_item['normalizedBoundingBox']['left'], 4), 
                                        't': round(timestamped_item['normalizedBoundingBox']['top'], 4) }
                                    local_box['w'] -= local_box['l']
                                    local_box['h'] -= local_box['t']
                                    details_obj['box'].append(local_box)
                            if "confidence" in track_item:
                                list_items.append( {"time_begin": float(re_time_clean.sub('', track_item["segment"]["startTimeOffset"])), 
                                    "time_end": float(re_time_clean.sub('', track_item["segment"]["endTimeOffset"])), 
                                    "time_event": float(re_time_clean.sub('', timestamped_item["timeOffset"])), 
                                    "source_event": "video", "tag": logo_item["entity"]["description"], "tag_type": "brand",
                                    "score": round(track_item["confidence"], 4), "details": json.dumps(details_obj), 
                                    "extractor": "gcp_videointelligence_logo_recognition"})
                return DataFrame(list_items)

        self.logger.critical(f"Missing nested 'logoRecognitionAnnotations' from source 'gcp_videointelligence_logo_recognition'")
        return None      
