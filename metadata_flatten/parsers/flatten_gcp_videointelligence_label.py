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
import re
from pandas import DataFrame

from . import Flatten


class Parser(Flatten):
    def __init__(self, path_content):
        super().__init__(path_content)
        
    def parse(self, run_options):
        """Flatten GCP Content Labels 
            - https://cloud.google.com/video-intelligence/docs/analyze-labels

        :param: run_options (dict): specific runtime information
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """

        # read data.json
        dict_data = self.get_extractor_results("gcp_videointelligence_label", "data.json")
        if not dict_data:  # do we need to load it locally?
            if 'extractor' in run_options:
                path_content = path.join(self.path_content, "data.json")
            else:
                path_content = path.join(self.path_content, "gcp_videointelligence_label", "data.json")
            dict_data = self.json_load(path_content)
            if not dict_data:
                path_content += ".gz"
                dict_data = self.json_load(path_content)
        if "annotationResults" not in dict_data:
            if run_options["verbose"]:
                self.logger.critical(f"Missing nested 'annotationResults' from source 'gcp_videointelligence_label'")
            return None

        # return details from a local entity
        def extract_entities(entity_item, as_str=True):
            details_obj = {}
            tag_name = "unknown"
            if "entity" in entity_item:
                # "entity": { "entityId": "/m/0bmgjqz", "description": "sport venue", "languageCode": "en-US"},
                tag_name = entity_item["entity"]["description"]
                details_obj["entity"] = entity_item["entity"]["entityId"]
            if "categoryEntities" in entity_item:
                # "categoryEntities": [{ "entityId": "/m/078x4m", "description": "location", "languageCode": "en-US" },
                details_obj["categories"] = {}
                for cat_entity in entity_item["categoryEntities"]:
                    details_obj["categories"][cat_entity["description"]] = cat_entity["entityId"]
            if as_str:
                return tag_name, json.dumps(details_obj)
            return tag_name, details_obj

        re_time_clean = re.compile(r"s$")
        list_items = []
        for annotation_obj in dict_data["annotationResults"]:  # traverse items
            # "segments": [{ "segment": { "startTimeOffset": "0s", "endTimeOffset": "13189.109266s" }, 
            #               "confidence": 0.5998325347900391 }
            if "segmentLabelAnnotations" in annotation_obj:  # validate object
                for segment_item in annotation_obj["segmentLabelAnnotations"]:   # segments
                    tag_name, str_json = extract_entities(segment_item, True)
                    if "segments" in segment_item:   # parsing segments
                        for local_seg in segment_item["segments"]:
                            list_items.append({"source_event": "video", "score": float(local_seg["confidence"]),
                                "time_begin": float(re_time_clean.sub('', local_seg["segment"]["startTimeOffset"])),
                                "time_end": float(re_time_clean.sub('', local_seg["segment"]["endTimeOffset"])),
                                "time_event": float(re_time_clean.sub('', local_seg["segment"]["startTimeOffset"])),
                                "details": str_json, "extractor": "gcp_videointelligence_label", "tag_type": "tag",
                                "tag": tag_name})
            if "shotLabelAnnotations" in annotation_obj:  # validate object
                for segment_item in annotation_obj["shotLabelAnnotations"]:  # shots
                    tag_name, str_json = extract_entities(segment_item, True)
                    if "segments" in segment_item:  # parsing segments
                        for local_seg in segment_item["segments"]:
                            list_items.append({"source_event": "image", "score": float(local_seg["confidence"]),
                                "time_begin": float(re_time_clean.sub('', local_seg["segment"]["startTimeOffset"])),
                                "time_end": float(re_time_clean.sub('', local_seg["segment"]["endTimeOffset"])),
                                "time_event": float(re_time_clean.sub('', local_seg["segment"]["startTimeOffset"])),
                                "details": str_json, "extractor": "gcp_videointelligence_label", "tag_type": "tag",
                                "tag": tag_name})
            # convert list to a dataframe
            return DataFrame(list_items)

        if run_options["verbose"]:
            self.logger.critical(f"Missing nested knowns 'segmentLabelAnnotations' and 'shotLabelAnnotations' from source 'gcp_videointelligence_label'")
        return None