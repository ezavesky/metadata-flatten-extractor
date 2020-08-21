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

from contentai_metadata_flatten.parsers import Flatten

class Parser(Flatten):
    def __init__(self, path_content, logger=None):
        super().__init__(path_content, logger=logger)
        self.EXTRACTOR = "aws_rekognition_video_faces"

    @staticmethod
    def known_types():
        """Return the output types for this generator
        :return: list.  List of output types (file types) for this generator
        """
        return ['face', 'emotion']

    def parse(self, run_options):
        """Flatten AWS Rekognition Faces
            - https://docs.aws.amazon.com/rekognition/latest/dg/faces.html

        :param: run_options (dict): specific runtime information 
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """
        list_items = []
        face_feats = {'Smile':'NoSmile', 'Eyeglasses':'NoGlasses', 'Sunglasses':'NoGlasses', 
                      'Gender':None, 'Beard':'NoBeard', 'Mustache':'NoMustache', 
                      'EyesOpen':'EyesClosed', 'MouthOpen':'MouthClosed'} # 'Pose', 'Landmarks', 'Quality']  -- propose we skip these (emz 1/30
        
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
                self.logger.info(f"... parsing aws_rekognition_video_faces/{file_search} ")

            for face_obj in dict_data["Faces"]:  # traverse items
                if "Face" in face_obj:  # validate object
                    local_obj = face_obj["Face"]
                    time_frame = float(face_obj["Timestamp"])/1000
                    score_frame = round(float(local_obj["Confidence"])/100, self.ROUND_DIGITS)
                    if "BoundingBox" in local_obj:
                        details_obj = {}
                        details_obj['box'] = {'w': round(local_obj['BoundingBox']['Width'], self.ROUND_DIGITS), 
                            'h': round(local_obj['BoundingBox']['Height'], self.ROUND_DIGITS),
                            'l': round(local_obj['BoundingBox']['Left'], self.ROUND_DIGITS), 
                            't': round(local_obj['BoundingBox']['Top'], self.ROUND_DIGITS) }
                        list_items.append({"time_begin": time_frame, "source_event": "face", 
                            "time_end": time_frame, "time_event": time_frame, "tag_type": "face",
                            "tag": "Face", "score": score_frame, "details": json.dumps(details_obj),
                            "extractor": self.EXTRACTOR})
                    if "Pose" in local_obj:
                        details_obj['pose'] = local_obj["Pose"]
                        list_items.append({"time_begin": time_frame, "source_event": "face", 
                            "time_end": time_frame, "time_event": time_frame, "tag_type": "face",
                            "tag": "Face", "score": score_frame, "details": json.dumps(details_obj),
                            "extractor": self.EXTRACTOR})

                    # go through all face features (modified 0.5.4, split face attributes)
                    for f in local_obj:
                        score_feat = None
                        if f in face_feats:   
                            details_obj = {}
                            if "Value" in local_obj[f]:
                                score_feat = round(float(local_obj[f]["Confidence"])/100, self.ROUND_DIGITS)
                                if face_feats[f] is not None:   # normal valued item, use here
                                    if local_obj[f]["Value"] == False:   # get the right name for a negative value
                                        f = face_feats[f]
                                else:     # special match for gender
                                    f = local_obj[f]["Value"]
                        elif f == "AgeRange":   # special condition for age
                            details_obj[f] = local_obj[f]
                            score_feat = self.SCORE_DEFAULT
                            f = "Age"
                        if score_feat is not None:
                            list_items.append({"time_begin": time_frame, "source_event": "face", 
                                "time_end": time_frame, "time_event": time_frame, "tag_type": "face",
                                "tag": f, "score": score_feat, "details": json.dumps(details_obj),
                                "extractor": self.EXTRACTOR})

                    # update 0.5.2 - break out emotion to other tag type
                    if "Emotions" in local_obj and local_obj["Emotions"]:
                        for emo_obj in local_obj["Emotions"]:
                            # if score_emo > 0.05   # consider a threshold?
                            score_emo = round(float(emo_obj["Confidence"])/100, self.ROUND_DIGITS)
                            list_items.append({"time_begin": time_frame, "source_event": "face", 
                                "time_end": time_frame, "time_event": time_frame, "tag_type": "emotion",
                                "tag": emo_obj["Type"].capitalize(), "score": score_emo, 
                                "details": json.dumps(details_obj), "extractor": self.EXTRACTOR})

            last_load_idx += 1

        if run_options["verbose"]:
            self.logger.critical(f"No faces found in source 'aws_rekognition_video_faces' ({file_search})")
        return None
