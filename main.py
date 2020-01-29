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

import contentai
from os import path
import sys
import gzip
import json
import re

import pandas as pd
from os import path, system, makedirs

import logging
import warnings
from sys import stdout as STDOUT

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(STDOUT)
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)

def json_load(path_file):
    """Helper to read dict object from JSON

    :param path_file: (str): Path for source file (can be gzipped)
    :return: dict.  The loaded dict or an empty dict (`{}`) on error
    """
    if path.exists(path_file):
        if path_file.endswith(".gz"):
            infile = gzip.open(path_file, 'rt')
        else:
            infile = open(path_file, 'rt')
        with infile:
            try:
                return json.load(infile)
            except json.decoder.JSONDecodeError as e:
                return {}
            except UnicodeDecodeError as e:
                return {}
    return {}

class Flatten():
    def __init__(self, path_content):
        super().__init__()
        self.path_content = path_content

    def flatten_gcp_videointelligence_shot_change(self, run_options):
        # read data.json
        #   "annotationResults": [ "shotAnnotations": [ {
        #     "startTimeOffset": "0s",
        #     "endTimeOffset": "19.285933s"
        #   }, ...
        if not path.exists(self.path_content):  # do we need to retrieve it?
            dict_data = contentai.get_extractor_results("gcp_videointelligence_shot_change", "data.json")
        else:
            dict_data = json_load(self.path_content)

        if "annotationResults" not in dict_data:
            logger.critical(f"Missing nested 'annotationResults' from metadata file '{self.path_content}'")
            return False
        path_result = run_options['path_result']

        reClean = re.compile(r"s$")
        for annotation_obj in dict_data["annotationResults"]:  # traverse items
            if "shotAnnotations" in annotation_obj:  # validate object
                list_items = []
                for shot_item in annotation_obj["shotAnnotations"]:
                    if "startTimeOffset" not in shot_item:
                        logger.critical(f"Missing nested 'startTimeOffset' in shot chunk '{shot_item}'")
                        return False
                    list_items.append( {"time_start": float(reClean.sub('', shot_item["startTimeOffset"])), 
                        "time_end": float(reClean.sub('', shot_item["endTimeOffset"])), 
                        "time_event": float(reClean.sub('', shot_item["startTimeOffset"])), 
                        "source_event": "video", "tag": "shot", "score": 1.0, "details": "",
                        "extractor": "gcp_videointelligence_shot_change"})
                df = pd.DataFrame(list_items)
                df.to_csv(path_result, index=False)
                logger.info(f"Wrote {len(df)} items to result file '{path_result}'")
                return True

        logger.critical(f"Missing nested 'shotAnnotations' from metadata file '{self.path_content}'")
        return False


def main():
    # check for a single argument as input for the path as an override
    if len(sys.argv) > 1:
        logger.info(f"Detected command line input: {sys.argv}")
        contentai.content_path = sys.argv[-1]
        contentai.result_path = path.dirname(contentai.content_path)
    
    # extract data from contentai.content_url
    # or if needed locally use contentai.content_path
    # after calling contentai.download_content()
    logger.info("Skipping raw content download from ContentAI")
    # contentai.download_content()   # critical, do not download content, we post-process!

    if not path.exists(contentai.result_path):
        makedirs(contentai.result_path)

    flatten = Flatten(contentai.content_path)
    for extractor_name in ["gcp_videointelligence_shot_change", 
                            "aws_rekognition_video_celebs", 
                            "aws_rekognition_video_content_moderation", 
                            "aws_rekognition_video_faces", 
                            "aws_rekognition_video_labels", 
                            "aws_rekognition_video_person_tracking", 
                            "azure_videoindexer", 
                            "gcp_videointelligence_explicit_content", 
                            "gcp_videointelligence_label", 
                            "gcp_videointelligence_shot_change", 
                            "gcp_videointelligence_speech_transcription"]:
        # attempt to get the flatten function
        try:
            func = getattr(flatten, f"flatten_{extractor_name}")
            # call process with i/o specified
            path_output = path.join(contentai.result_path, extractor_name + ".csv")

            # allow injection of parameters from environment
            input_vars = {'path_result': path_output}
            if contentai.metadata is not None:  # see README.md for more info
                input_vars.update(contentai.metadata)
            logger.info(f"ContentAI argments: {input_vars}")
            func(input_vars)
        except:
            logger.info(f"Flatten function for '{extractor_name}' not found, skipping")
        pass


if __name__ == "__main__":
    main()
