#! python
# ===============LICENSE_START=======================================================
# vinyl-tools Apache-2.0
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
from . import Flatten

class Parser(Flatten):
    def __init__(self, path_content):
        super().__init__(path_content)

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
            dict_data = self.get_extractor_results("aws_rekognition_video_labels", file_search)
            if not dict_data:  # do we need to load it locally?
                path_content = path.join(self.path_content, "aws_rekognition_video_labels", file_search)
                dict_data = self.json_load(path_content)
                if not dict_data:
                    path_content += ".gz"
                    dict_data = self.json_load(path_content)
            if not dict_data:  # couldn't load anything else...
                if list_items:
                    return pd.DataFrame(list_items)
                else:
                    last_load_idx = -1
                    break

            self.logger.info(f"... parsing aws_rekognition_video_labels/{file_search} ")

            if "Labels" not in dict_data:
                self.logger.critical(f"Missing nested 'Labels' from source 'aws_rekognition_video_labels' ({file_search})")
                return None

            for celebrity_obj in dict_data["Labels"]:  # traverse items
                if "Label" in celebrity_obj:  # validate object
                    # " { "Timestamp": 0, "Label": { "Name": "Train", "Confidence": 62.60573959350586, "Instances": [
                    # { "BoundingBox": { "Width": 0.224, "Height": 0.2151, "Left": 0.722, "Top": 0.350 },
                    # "Confidence": 62.73824691772461 } ], "Parents": [{ "Name": "Vehicle" }, { "Name": "Transportation" } ] }
                    time_frame = float(celebrity_obj["Timestamp"])/1000
                    details_obj = {}
                    local_obj = celebrity_obj["Label"]
                    if "Parents" in local_obj and len(local_obj["Parents"]):   # skip over those without parent name
                        details_obj = {'category': [p["Name"] for p in local_obj["Parents"]]}
                    if "Instances" in local_obj and len(local_obj["Instances"]):
                        details_obj['count'] = len(local_obj["Instances"])
                        details_obj['box'] = []
                        for box in local_obj["Instances"]:
                            details_obj['box'].append({'w': round(box['BoundingBox']['Width'], 4), 
                            'h': round(box['BoundingBox']['Height'], 4),
                            'l': round(box['BoundingBox']['Left'], 4), 
                            't': round(box['BoundingBox']['Top'], 4) })

                    score_frame = round(float(local_obj["Confidence"])/100, 4)
                    list_items.append({"time_begin": time_frame, "source_event": "image",  "tag_type": "tag",
                        "time_end": time_frame, "time_event": time_frame, "tag": local_obj["Name"],
                        "score": score_frame, "details": json.dumps(details_obj),
                        "extractor": "aws_rekognition_video_labels"})
            last_load_idx += 1

        self.logger.critical(f"No moderation enties found in source 'aws_rekognition_video_labels' ({file_search})")
        return None
