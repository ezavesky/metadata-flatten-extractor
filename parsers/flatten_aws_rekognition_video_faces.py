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
        """Flatten AWS Rekognition Faces
            - https://docs.aws.amazon.com/rekognition/latest/dg/faces.html

        :param: run_options (dict): specific runtime information 
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """
        list_items = []
        face_feats = ['AgeRange', 'Smile', 'Eyeglasses', 'Sunglasses', 'Gender', 'Beard', 'Mustache', 
                     'EyesOpen', 'MouthOpen', 'Pose']  # , 'Landmarks', 'Quality']  -- propose we skip these (emz 1/30
        
        last_load_idx = 0
        while last_load_idx >= 0:
            file_search = f"result{last_load_idx}.json"
            dict_data = self.get_extractor_results("aws_rekognition_video_faces", file_search)
            if not dict_data:  # do we need to load it locally?
                path_content = path.join(self.path_content, "aws_rekognition_video_faces", file_search)
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

            if run_options["verbose"]:
                self.logger.info(f"... parsing aws_rekognition_video_faces/{file_search} ")

            for face_obj in dict_data["Faces"]:  # traverse items
                if "Face" in face_obj:  # validate object
                    local_obj = face_obj["Face"]
                    time_frame = float(face_obj["Timestamp"])/1000
                    details_obj = {}
                    if "BoundingBox" in local_obj:
                        details_obj['box'] = {'w': round(local_obj['BoundingBox']['Width'], 4), 
                            'h': round(local_obj['BoundingBox']['Height'], 4),
                            'l': round(local_obj['BoundingBox']['Left'], 4), 
                            't': round(local_obj['BoundingBox']['Top'], 4) }
                    for f in face_feats:   # go through all face features
                        if f in local_obj and local_obj[f]:
                            if "Value" in local_obj[f]:
                                score_feat = round(float(local_obj[f]["Confidence"])/100, 4)
                                if local_obj[f]["Value"] == "Male":  # special case for 'male' gender
                                    details_obj["Male"] = score_feat
                                elif local_obj[f]["Value"] == "Female":  # special case for 'female' gender
                                    details_obj["Female"] = score_feat
                                else:  # normal valued item, use here
                                    if local_obj[f]["Value"] == False:
                                        score_feat = 1 - score_feat
                                    details_obj[f] = score_feat
                            else:  # don't match a condition above
                                details_obj[f] = local_obj[f]
                    score_frame = round(float(local_obj["Confidence"])/100, 4)

                    list_items.append({"time_begin": time_frame, "source_event": "face", 
                        "time_end": time_frame, "time_event": time_frame, "tag_type": "face",
                        "tag": "Face", "score": score_frame, "details": json.dumps(details_obj),
                        "extractor": "aws_rekognition_video_faces"})

                    # update 0.5.2 - break out emotion to other tag type
                    if "Emotions" in local_obj and local_obj["Emotions"]:
                        for emo_obj in local_obj["Emotions"]:
                            # if score_emo > 0.05   # consider a threshold?
                            score_emo = round(float(emo_obj["Confidence"])/100, 4)
                            list_items.append({"time_begin": time_frame, "source_event": "face", 
                                "time_end": time_frame, "time_event": time_frame, "tag_type": "emotion",
                                "tag": emo_obj["Type"].capitalize(), "score": score_emo, 
                                "details": json.dumps(details_obj), "extractor": "aws_rekognition_video_faces"})

            last_load_idx += 1

        if run_options["verbose"]:
            self.logger.critical(f"No faces found in source 'aws_rekognition_video_faces' ({file_search})")
        return None
