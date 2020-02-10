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

from . import Flatten

class Parser(Flatten):
    def __init__(self, path_content):
        super().__init__(path_content)

    def parse(self, run_options):
        """Flatten AWS Rekognition Person Tracking
            - https://docs.aws.amazon.com/rekognition/latest/dg/persons.html

        :param: run_options (dict): specific runtime information
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """
        list_items = []
        
        last_load_idx = 0
        while last_load_idx >= 0:
            file_search = f"result{last_load_idx}.json"
            dict_data = self.get_extractor_results("aws_rekognition_video_person_tracking", file_search)
            if not dict_data:  # do we need to load it locally?
                path_content = path.join(self.path_content, "aws_rekognition_video_person_tracking", file_search)
                dict_data = self.json_load(path_content)
                if not dict_data:
                    path_content += ".gz"
                    dict_data = self.json_load(path_content)
            if not dict_data:  # couldn't load anything else...
                if list_items:
                    return DataFrame(list_items)
                else:
                    last_load_idx = -1
                    break

            self.logger.info(f"... parsing aws_rekognition_video_person_tracking/{file_search} ")

            for face_obj in dict_data["Persons"]:  # traverse items
                if "Person" in face_obj:  # validate object
                    local_obj = face_obj["Person"]
                    time_frame = float(face_obj["Timestamp"])/1000
                    details_obj = {}
                    if "BoundingBox" in local_obj:
                        details_obj['box'] = {'w': round(local_obj['BoundingBox']['Width'], 4), 
                            'h': round(local_obj['BoundingBox']['Height'], 4),
                            'l': round(local_obj['BoundingBox']['Left'], 4), 
                            't': round(local_obj['BoundingBox']['Top'], 4) }
                    person_idx = "person_" + str(local_obj["Index"])

                    if "Face" in local_obj and local_obj["Face"]:
                        face_obj = local_obj["Face"]   # skip Pose, Quality, Landmarks
                        if "BoundingBox" in face_obj:
                            details_obj['Face'] = {'w': round(face_obj['BoundingBox']['Width'], 4), 
                                'h': round(face_obj['BoundingBox']['Height'], 4),
                                'l': round(face_obj['BoundingBox']['Left'], 4), 
                                't': round(face_obj['BoundingBox']['Top'], 4) }

                    list_items.append({"time_begin": time_frame, "source_event": "image",
                        "time_end": time_frame, "time_event": time_frame,  "tag_type": "person",
                        "tag": person_idx, "score": 1.0, "details": json.dumps(details_obj),
                        "extractor": "aws_rekognition_video_person_tracking"})

            last_load_idx += 1

        self.logger.critical(f"No people found in source 'aws_rekognition_video_person_tracking' ({file_search})")
        return None
