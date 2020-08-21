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

from contentai_metadata_flatten.parsers import Flatten

class Parser(Flatten):
    def __init__(self, path_content, logger=None):
        super().__init__(path_content, logger=logger)
        self.EXTRACTOR = "aws_rekognition_face_collection"

    @staticmethod
    def known_types():
        """Return the output types for this generator
        :return: list.  List of output types (file types) for this generator
        """
        return ['identity']

    def parse(self, run_options):
        """Flatten AWS Results from Face Collections
        https://docs.aws.amazon.com/rekognition/latest/dg/API_SearchFaces.html

        :param: run_options (dict): specific runtime information 
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """
        list_items = []
        re_clean = re.compile(r"((faces*|result|data)|([0-9]+$))+")
        re_split = re.compile(r"_+")

        last_load_idx = 0
        suppressed_matches = 0
        while last_load_idx >= 0:
            file_search = f"result{last_load_idx}.json"
            dict_data = self.get_extractor_results(self.EXTRACTOR, file_search)
            if not dict_data:  # couldn't load anything else...
                if list_items:
                    self.logger.info(f"... suppressed {suppressed_matches} duplicate identities on a timestamp...")
                    return DataFrame(list_items)
                else:
                    last_load_idx = -1
                    break

            if run_options["verbose"]:
                self.logger.info(f"... parsing rekognition_face_collection/{file_search} ")

            for face_obj in dict_data["Persons"]:  # traverse items
                if "FaceMatches" in face_obj:  # validate object; for now, we skip unnamed faces
                    time_frame = float(face_obj["Timestamp"])/1000
                    seen_faces = {}
                    for local_obj in face_obj["FaceMatches"]:
                        # score is product of 'similarity' and 'confidence'
                        match_obj = local_obj["Face"]
                        score_frame = round(float(local_obj["Similarity"])/100 * float(match_obj["Confidence"])/100, self.ROUND_DIGITS)
                        details_obj = {}
                        if "BoundingBox" in match_obj:
                            details_obj['box'] = {'w': round(match_obj['BoundingBox']['Width'], self.ROUND_DIGITS), 
                                'h': round(match_obj['BoundingBox']['Height'], self.ROUND_DIGITS),
                                'l': round(match_obj['BoundingBox']['Left'], self.ROUND_DIGITS),
                                't': round(match_obj['BoundingBox']['Top'], self.ROUND_DIGITS) }

                        # this is a user-specified field, so we have to be creative... (see some examples)
                        #   "faces_Tech_N9Ne_Tech_N9Ne29.jpg",
                        #   "Tech_N9Ne_Tech_N9Ne29.jpg",
                        #   Deforest_Buckner36.jpg
                        face_name_raw = match_obj['ExternalImageId']
                        face_name = re_split.sub(' ', re_clean.sub("_", path.splitext(face_name_raw)[0])).strip().split(' ')
                        face_name = ' '.join(face_name) if len(face_name) < 2 else ' '.join(face_name[:2])
                        
                        # this module has a tendency to over-fire, so only allow one face-name per timestamp
                        if face_name not in seen_faces or seen_faces[face_name]['score'] < score_frame:
                            if face_name in seen_faces:
                                suppressed_matches += 1
                            seen_faces[face_name] = {"time_begin": time_frame, "source_event": "image", 
                                "time_end": time_frame, "time_event": time_frame, "tag_type": "identity",
                                "tag": face_name, "score": score_frame, "details": json.dumps(details_obj),
                                "extractor": self.EXTRACTOR}
                        else:
                            suppressed_matches += 1

                    # finally, append those highest scoring faces
                    list_items += list(seen_faces.values())
            last_load_idx += 1

        if run_options["verbose"]:
            self.logger.critical(f"No faces found in source 'rekognition_face_collection' ({file_search})")
        return None
