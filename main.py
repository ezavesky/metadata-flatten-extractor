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
    # https://cloud.google.com/video-intelligence/docs/reference/reast/Shared.Types/Likelihood
    GCP_LIKELIHOOD_MAP = { "LIKELIHOOD_UNSPECIFIED": 0.0, "VERY_UNLIKELY": 0.1, "UNLIKELY": 0.25,
                           "POSSIBLE": 0.5, "LIKELY": 0.75, "VERY_LIKELY": 0.9 }
    def __init__(self, path_content):
        super().__init__()
        self.path_content = path_content

    def flatten_gcp_videointelligence_label(self, run_options):
        """Flatten GCP Content Labels 
            - https://cloud.google.com/video-intelligence/docs/analyze-labels

        :param: run_options (dict): specific runtime information ('path_result' for directory output, 'force_overwrite' True/False)
        :returns: (bool): True on successful decoding and export, False (or exception) otherwise
        """
        path_result = run_options['path_result']
        if path.exists(path_result) and ('force_overwrite' not in run_options or not run_options['force_overwrite']):
            return True

        # read data.json
        dict_data = contentai.get_extractor_results("gcp_videointelligence_label", "data.json")
        if not dict_data:  # do we need to load it locally?
            path_content = path.join(self.path_content, "gcp_videointelligence_label", "data.json")
            dict_data = json_load(path_content)
            if not dict_data:
                path_content += ".gz"
                dict_data = json_load(path_content)
        if "annotationResults" not in dict_data:
            logger.critical(f"Missing nested 'annotationResults' from metadata file '{path_content}'")
            return False
        path_result = run_options['path_result']

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
                                "time_start": float(re_time_clean.sub('', local_seg["segment"]["startTimeOffset"])),
                                "time_end": float(re_time_clean.sub('', local_seg["segment"]["endTimeOffset"])),
                                "time_event": float(re_time_clean.sub('', local_seg["segment"]["startTimeOffset"])),
                                "details": str_json, "extractor": "gcp_videointelligence_label",
                                "tag": tag_name})
            if "shotLabelAnnotations" in annotation_obj:  # validate object
                for segment_item in annotation_obj["shotLabelAnnotations"]:  # shots
                    tag_name, str_json = extract_entities(segment_item, True)
                    if "segments" in segment_item:  # parsing segments
                        for local_seg in segment_item["segments"]:
                            list_items.append({"source_event": "image", "score": float(local_seg["confidence"]),
                                "time_start": float(re_time_clean.sub('', local_seg["segment"]["startTimeOffset"])),
                                "time_end": float(re_time_clean.sub('', local_seg["segment"]["endTimeOffset"])),
                                "time_event": float(re_time_clean.sub('', local_seg["segment"]["startTimeOffset"])),
                                "details": str_json, "extractor": "gcp_videointelligence_label",
                                "tag": tag_name})
            # convert list to a dataframe
            df = pd.DataFrame(list_items).sort_values("time_start")
            df.to_csv(path_result, index=False)
            logger.info(f"Wrote {len(df)} items to result file '{path_result}'")
            return True

        logger.critical(f"Missing nested knowns 'segmentLabelAnnotations' and 'shotLabelAnnotations' from metadata file '{path_content}'")
        return False

    def flatten_gcp_videointelligence_shot_change(self, run_options):
        """Flatten GCP Shot Change Detection - https://cloud.google.com/video-intelligence/docs/analyze-shots

        :param: run_options (dict): specific runtime information ('path_result' for directory output, 'force_overwrite' True/False)
        :returns: (bool): True on successful decoding and export, False (or exception) otherwise
        """
        # read data.json
        #   "annotationResults": [ "shotAnnotations": [ {
        #     "startTimeOffset": "0s",
        #     "endTimeOffset": "19.285933s"
        #   }, ...
        path_result = run_options['path_result']
        if path.exists(path_result) and ('force_overwrite' not in run_options or not run_options['force_overwrite']):
            return True

        dict_data = contentai.get_extractor_results("c", "data.json")
        if not dict_data:  # do we need to load it locally?
            path_content = path.join(self.path_content, "gcp_videointelligence_shot_change", "data.json")
            dict_data = json_load(path_content)
            if not dict_data:
                path_content += ".gz"
                dict_data = json_load(path_content)

        if "annotationResults" not in dict_data:
            logger.critical(f"Missing nested 'annotationResults' from metadata file '{path_content}'")
            return False

        re_time_clean = re.compile(r"s$")
        for annotation_obj in dict_data["annotationResults"]:  # traverse items
            if "shotAnnotations" in annotation_obj:  # validate object
                list_items = []
                for shot_item in annotation_obj["shotAnnotations"]:
                    if "startTimeOffset" not in shot_item:
                        logger.critical(f"Missing nested 'startTimeOffset' in shot chunk '{shot_item}'")
                        return False
                    list_items.append( {"time_start": float(re_time_clean.sub('', shot_item["startTimeOffset"])), 
                        "time_end": float(re_time_clean.sub('', shot_item["endTimeOffset"])), 
                        "time_event": float(re_time_clean.sub('', shot_item["startTimeOffset"])), 
                        "source_event": "video", "tag": "shot", "score": 1.0, "details": "",
                        "extractor": "gcp_videointelligence_shot_change"})
                df = pd.DataFrame(list_items).sort_values("time_start")
                df.to_csv(path_result, index=False)
                logger.info(f"Wrote {len(df)} items to result file '{path_result}'")
                return True

        logger.critical(f"Missing nested 'shotAnnotations' from metadata file '{path_content}'")
        return False

    def flatten_gcp_videointelligence_explicit_content(self, run_options):
        """Flatten GCP Explicit Annotation 
            - https://cloud.google.com/video-intelligence/docs/analyze-safesearch
            - https://cloud.google.com/video-intelligence/docs/reference/rest/Shared.Types/Likelihood

        :param: run_options (dict): specific runtime information ('path_result' for directory output, 'force_overwrite' True/False)
        :returns: (bool): True on successful decoding and export, False (or exception) otherwise
        """
        # read data.json
        #   "annotationResults": [ "explicitAnnotation": [ { "frames": [ {
        #     "timeOffset": "0s",
        #     "pornographyLikelihood": "VERY_UNLIKELY"
        #   }, ...
        path_result = run_options['path_result']
        if path.exists(path_result) and ('force_overwrite' not in run_options or not run_options['force_overwrite']):
            return True

        dict_data = contentai.get_extractor_results("gcp_videointelligence_explicit_content", "data.json")
        if not dict_data:  # do we need to load it locally?
            path_content = path.join(self.path_content, "gcp_videointelligence_explicit_content", "data.json")
            dict_data = json_load(path_content)
            if not dict_data:
                path_content += ".gz"
                dict_data = json_load(path_content)
        if "annotationResults" not in dict_data:
            logger.critical(f"Missing nested 'annotationResults' from metadata file '{path_content}'")
            return False

        re_time_clean = re.compile(r"s$")
        for annotation_obj in dict_data["annotationResults"]:  # traverse items
            if "explicitAnnotation" in annotation_obj:  # validate object
                if "frames" not in annotation_obj["explicitAnnotation"]:  # validate object
                    logger.critical(f"Missing nested 'frames' in shot chunk '{explicit_item}'")
                    return False
                list_items = []
                for frame_item in annotation_obj["explicitAnnotation"]["frames"]:
                    if "timeOffset" in frame_item:
                        time_clean = float(re_time_clean.sub('', frame_item["timeOffset"]))
                        dict_scores = {n:n.split("Likelihood")[0] for n in frame_item.keys() if not n.startswith("time") }
                        for n in dict_scores:  # a little bit of a dance, but flexiblity for future explicit types
                            list_items.append( {"time_start": time_clean, "source_event": "image",
                                "time_end": time_clean, "time_event": time_clean, "tag": dict_scores[n],                   
                                "score": Flatten.GCP_LIKELIHOOD_MAP[frame_item[n]], "details": "",
                                "extractor": "gcp_videointelligence_explicit_content"})
                df = pd.DataFrame(list_items).sort_values("time_start")
                df.to_csv(path_result, index=False)
                logger.info(f"Wrote {len(df)} items to result file '{path_result}'")
                return True

        logger.critical(f"Missing nested 'explicitAnnotation' from metadata file '{path_content}'")
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
    for extractor_name in [ "aws_rekognition_video_celebs", 
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
            input_vars = {'path_result': path_output, "force_overwrite": True}
            if contentai.metadata is not None:  # see README.md for more info
                input_vars.update(contentai.metadata)
            logger.info(f"ContentAI argments: {input_vars}")
            func(input_vars)
        except AttributeError as e:
            logger.info(f"Flatten function for '{extractor_name}' not found, skipping")
        except Exception as e:
            raise e
        pass


if __name__ == "__main__":
    main()
