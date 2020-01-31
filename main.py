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
    TAG_TRANSCRIPT = "_transcript_"
    def __init__(self, path_content):
        super().__init__()
        self.path_content = path_content

    def flatten_gcp_videointelligence_label(self, run_options):
        """Flatten GCP Content Labels 
            - https://cloud.google.com/video-intelligence/docs/analyze-labels

        :param: run_options (dict): specific runtime information
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """

        # read data.json
        dict_data = contentai.get_extractor_results("gcp_videointelligence_label", "data.json")
        if not dict_data:  # do we need to load it locally?
            path_content = path.join(self.path_content, "gcp_videointelligence_label", "data.json")
            dict_data = json_load(path_content)
            if not dict_data:
                path_content += ".gz"
                dict_data = json_load(path_content)
        if "annotationResults" not in dict_data:
            logger.critical(f"Missing nested 'annotationResults' from source 'gcp_videointelligence_label'")
            return None
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
            return pd.DataFrame(list_items)

        logger.critical(f"Missing nested knowns 'segmentLabelAnnotations' and 'shotLabelAnnotations' from source 'gcp_videointelligence_label'")
        return None

    def flatten_gcp_videointelligence_shot_change(self, run_options):
        """Flatten GCP Shot Change Detection - https://cloud.google.com/video-intelligence/docs/analyze-shots

        :param: run_options (dict): specific runtime information 
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """
        # read data.json
        #   "annotationResults": [ "shotAnnotations": [ {
        #     "startTimeOffset": "0s",
        #     "endTimeOffset": "19.285933s"
        #   }, ...
        dict_data = contentai.get_extractor_results("gcp_videointelligence_shot_change", "data.json")
        if not dict_data:  # do we need to load it locally?
            path_content = path.join(self.path_content, "gcp_videointelligence_shot_change", "data.json")
            dict_data = json_load(path_content)
            if not dict_data:
                path_content += ".gz"
                dict_data = json_load(path_content)

        if "annotationResults" not in dict_data:
            logger.critical(f"Missing nested 'annotationResults' from source 'gcp_videointelligence_shot_change'")
            return None

        re_time_clean = re.compile(r"s$")
        for annotation_obj in dict_data["annotationResults"]:  # traverse items
            if "shotAnnotations" in annotation_obj:  # validate object
                list_items = []
                for shot_item in annotation_obj["shotAnnotations"]:
                    if "startTimeOffset" not in shot_item:
                        logger.critical(f"Missing nested 'startTimeOffset' in shot chunk '{shot_item}'")
                        return None
                    list_items.append( {"time_begin": float(re_time_clean.sub('', shot_item["startTimeOffset"])), 
                        "time_end": float(re_time_clean.sub('', shot_item["endTimeOffset"])), 
                        "time_event": float(re_time_clean.sub('', shot_item["startTimeOffset"])), 
                        "source_event": "video", "tag": "shot", "score": 1.0, "details": "", "tag_type": "shot",
                        "extractor": "gcp_videointelligence_shot_change"})
                return pd.DataFrame(list_items)

        logger.critical(f"Missing nested 'shotAnnotations' from source 'gcp_videointelligence_shot_change'")
        return None

    def flatten_gcp_videointelligence_explicit_content(self, run_options):
        """Flatten GCP Explicit Annotation 
            - https://cloud.google.com/video-intelligence/docs/analyze-safesearch
            - https://cloud.google.com/video-intelligence/docs/reference/rest/Shared.Types/Likelihood

        :param: run_options (dict): specific runtime information
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """
        # read data.json
        #   "annotationResults": [ "explicitAnnotation": [ { "frames": [ {
        #     "timeOffset": "0s",
        #     "pornographyLikelihood": "VERY_UNLIKELY"
        #   }, ...
        dict_data = contentai.get_extractor_results("gcp_videointelligence_explicit_content", "data.json")
        if not dict_data:  # do we need to load it locally?
            path_content = path.join(self.path_content, "gcp_videointelligence_explicit_content", "data.json")
            dict_data = json_load(path_content)
            if not dict_data:
                path_content += ".gz"
                dict_data = json_load(path_content)

        if "annotationResults" not in dict_data:
            logger.critical(f"Missing nested 'annotationResults' from source 'gcp_videointelligence_explicit_content'")
            return None

        re_time_clean = re.compile(r"s$")
        for annotation_obj in dict_data["annotationResults"]:  # traverse items
            if "explicitAnnotation" in annotation_obj:  # validate object
                if "frames" not in annotation_obj["explicitAnnotation"]:  # validate object
                    logger.critical(f"Missing nested 'frames' in shot chunk '{explicit_item}'")
                    return None
                list_items = []
                for frame_item in annotation_obj["explicitAnnotation"]["frames"]:
                    if "timeOffset" in frame_item:
                        time_clean = float(re_time_clean.sub('', frame_item["timeOffset"]))
                        dict_scores = {n:n.split("Likelihood")[0] for n in frame_item.keys() if not n.startswith("time") }
                        for n in dict_scores:  # a little bit of a dance, but flexiblity for future explicit types
                            list_items.append( {"time_begin": time_clean, "source_event": "image",  "tag_type": "explicit",
                                "time_end": time_clean, "time_event": time_clean, "tag": dict_scores[n],                   
                                "score": Flatten.GCP_LIKELIHOOD_MAP[frame_item[n]], "details": "",
                                "extractor": "gcp_videointelligence_explicit_content"})
                return pd.DataFrame(list_items)

        logger.critical(f"Missing nested 'explicitAnnotation' from source 'gcp_videointelligence_explicit_content'")
        return None

    def flatten_gcp_videointelligence_speech_transcription(self, run_options):
        """Flatten GCP Speech Recognition 
            - https://cloud.google.com/video-intelligence/docs/transcription

        :param: run_options (dict): specific runtime information 
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """
        # read data.json
        #   "annotationResults": [ {  "speechTranscriptions": [ {
        #       "alternatives": [ { "transcript": "Play Super Bowl 50 for here tonight. ", "confidence": 0.8140063881874084,
        #       "words": [ { "startTime": "0s", "endTime": "0.400s", "word": "Play", "confidence": 0.9128385782241821 },
        dict_data = contentai.get_extractor_results("gcp_videointelligence_speech_transcription", "data.json")
        if not dict_data:  # do we need to load it locally?
            path_content = path.join(self.path_content, "gcp_videointelligence_speech_transcription", "data.json")
            dict_data = json_load(path_content)
            if not dict_data:
                path_content += ".gz"
                dict_data = json_load(path_content)

        if "annotationResults" not in dict_data:
            logger.critical(f"Missing nested 'annotationResults' from source 'gcp_videointelligence_speech_transcription'")
            return None

        re_time_clean = re.compile(r"s$")
        for annotation_obj in dict_data["annotationResults"]:  # traverse items
            if "speechTranscriptions" not in annotation_obj:  # validate object
                logger.critical(f"Missing nested 'speechTranscriptions' in annotation chunk '{annotation_obj}'")
                return None
            list_items = []
            for speech_obj in annotation_obj["speechTranscriptions"]:  # walk through all parts
                if "alternatives" not in speech_obj:
                    logger.critical(f"Missing nested 'alternatives' in speechTranscriptions chunk '{speech_obj}'")
                    return None
                for alt_obj in speech_obj["alternatives"]:   # walk through speech parts
                    time_end = 0
                    time_begin = 1e200
                    num_words = 0
                    if "words" in alt_obj and num_words==0:  # parse all words for this transcript chunk
                        for word_obj in alt_obj["words"]:  # walk through all words
                            # {  "startTime": "0.400s",  "endTime": "0.700s",  "word": "Super", "confidence": 0.9128385782241821 }
                            time_begin_clean = float(re_time_clean.sub('', word_obj["startTime"]))
                            time_end_clean = float(re_time_clean.sub('', word_obj["endTime"]))
                            time_begin = min(time_begin, time_begin_clean)
                            time_end = max(time_end, time_end_clean)
                            # add new item for this word
                            list_items.append( {"time_begin": time_begin_clean, "source_event": "speech", "tag_type": "word",
                                "time_end": time_end_clean, "time_event": time_begin_clean, "tag": word_obj["word"],
                                "score": float(word_obj["confidence"]), "details": "",
                                "extractor": "gcp_videointelligence_speech_transcription"})
                            num_words += 1

                        if "transcript" in alt_obj:  # generate top-level transcript item, after going through all words
                            list_items.append( {"time_begin": time_begin, "source_event": "speech", "tag_type": "transcript",
                                "time_end": time_end, "time_event": time_begin, "tag": Flatten.TAG_TRANSCRIPT,
                                "score": float(alt_obj["confidence"]), 
                                "details": json.dumps({"words": num_words, "transcript": alt_obj["transcript"]}),
                                "extractor": "gcp_videointelligence_speech_transcription"})
            return pd.DataFrame(list_items)

        logger.critical(f"Missing nested 'alternatives' in speechTranscriptions chunks from source 'gcp_videointelligence_speech_transcription'")
        return None

    def flatten_aws_rekognition_video_celebs(self, run_options):
        """Flatten AWS Rekognition celebrities
            - https://docs.aws.amazon.com/rekognition/latest/dg/celebrities-procedure-image.html

        :param: run_options (dict): specific runtime information
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """
        list_items = []
        last_load_idx = 0
        while last_load_idx >= 0:
            file_search = f"result{last_load_idx}.json"
            dict_data = contentai.get_extractor_results("aws_rekognition_video_celebs", file_search)
            if not dict_data:  # do we need to load it locally?
                path_content = path.join(self.path_content, "aws_rekognition_video_celebs", file_search)
                dict_data = json_load(path_content)
                if not dict_data:
                    path_content += ".gz"
                    dict_data = json_load(path_content)
            if not dict_data:  # couldn't load anything else...
                if list_items:
                    return pd.DataFrame(list_items)
                else:
                    last_load_idx = -1
                    break

            logger.info(f"... parsing aws_rekognition_video_celebs/{file_search} ")

            if "Celebrities" not in dict_data:
                logger.critical(f"Missing nested 'Celebrities' from source 'aws_rekognition_video_celebs' ({file_search})")
                return None

            for celebrity_obj in dict_data["Celebrities"]:  # traverse items
                if "Celebrity" in celebrity_obj:  # validate object
                    local_obj = celebrity_obj["Celebrity"]
                    time_frame = float(celebrity_obj["Timestamp"])/1000
                    details_obj = {}
                    if "BoundingBox" in local_obj:
                        details_obj['box'] = {'w': round(local_obj['BoundingBox']['Width'], 4), 
                            'h': round(local_obj['BoundingBox']['Height'], 4),
                            'l': round(local_obj['BoundingBox']['Left'], 4), 
                            't': round(local_obj['BoundingBox']['Top'], 4) }
                    if "Urls" in local_obj and local_obj["Urls"]:
                        details_obj['urls'] = ",".join(local_obj["Urls"])
                    score_frame = round(float(local_obj["Confidence"])/100, 4)

                    list_items.append({"time_begin": time_frame, "source_event": "image", "tag_type": "identity",
                        "time_end": time_frame, "time_event": time_frame, "tag": local_obj["Name"],
                        "score": score_frame, "details": json.dumps(details_obj),
                        "extractor": "aws_rekognition_video_celebs"})
            last_load_idx += 1

        logger.critical(f"No celebrity enties found in source 'aws_rekognition_video_celebs' ({file_search}'")
        return None

    def flatten_aws_rekognition_video_content_moderation(self, run_options):
        """Flatten AWS Rekognition Moderatioon
            - https://docs.aws.amazon.com/rekognition/latest/dg/API_GetContentModeration.html

        :param: run_options (dict): specific runtime information
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """
        list_items = []
        last_load_idx = 0
        while last_load_idx >= 0:
            file_search = f"result{last_load_idx}.json"
            dict_data = contentai.get_extractor_results("aws_rekognition_video_content_moderation", file_search)
            if not dict_data:  # do we need to load it locally?
                path_content = path.join(self.path_content, "aws_rekognition_video_content_moderation", file_search)
                dict_data = json_load(path_content)
                if not dict_data:
                    path_content += ".gz"
                    dict_data = json_load(path_content)
            if not dict_data:  # couldn't load anything else...
                if list_items:
                    return pd.DataFrame(list_items)
                else:
                    last_load_idx = -1
                    break

            logger.info(f"... parsing aws_rekognition_video_content_moderation/{file_search} ")

            if "ModerationLabels" not in dict_data:
                logger.critical(f"Missing nested 'ModerationLabels' from source 'aws_rekognition_video_content_moderation' ({file_search})")
                return None

            for celebrity_obj in dict_data["ModerationLabels"]:  # traverse items
                if "ModerationLabel" in celebrity_obj:  # validate object
                    # "ModerationLabels": [ {  "Timestamp": 29662,   "ModerationLabel": 
                    #   {"Confidence": 71.34247589111328, "Name": "Explicit Nudity", "ParentName": ""  } },
                    local_obj = celebrity_obj["ModerationLabel"]
                    if "ParentName" in local_obj and len(local_obj["ParentName"]):   # skip over those without parent name
                        time_frame = float(celebrity_obj["Timestamp"])/1000
                        details_obj = {'category': local_obj["ParentName"]}
                        score_frame = round(float(local_obj["Confidence"])/100, 4)
                        list_items.append({"time_begin": time_frame, "source_event": "image",  "tag_type": "explicit",
                            "time_end": time_frame, "time_event": time_frame, "tag": local_obj["Name"],
                            "score": score_frame, "details": json.dumps(details_obj),
                            "extractor": "aws_rekognition_video_content_moderation"})
            last_load_idx += 1

        logger.critical(f"No moderation enties found in source 'aws_rekognition_video_content_moderation' ({file_search}'")
        return None

    def flatten_aws_rekognition_video_labels(self, run_options):
        """Flatten AWS Video Labels
            - https://docs.aws.amazon.com/rekognition/latest/dg/labels-detecting-labels-video.html

        :param: run_options (dict): specific runtime information
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """
        path_result = run_options['path_result']
        if path.exists(path_result) and ('force_overwrite' not in run_options or not run_options['force_overwrite']):
            return True

        list_items = []
        last_load_idx = 0
        while last_load_idx >= 0:
            file_search = f"result{last_load_idx}.json"
            dict_data = contentai.get_extractor_results("aws_rekognition_video_labels", file_search)
            if not dict_data:  # do we need to load it locally?
                path_content = path.join(self.path_content, "aws_rekognition_video_labels", file_search)
                dict_data = json_load(path_content)
                if not dict_data:
                    path_content += ".gz"
                    dict_data = json_load(path_content)
            if not dict_data:  # couldn't load anything else...
                if list_items:
                    return pd.DataFrame(list_items)
                else:
                    last_load_idx = -1
                    break

            logger.info(f"... parsing aws_rekognition_video_labels/{file_search} ")

            if "Labels" not in dict_data:
                logger.critical(f"Missing nested 'Labels' from source 'aws_rekognition_video_labels' ({file_search})")
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

        logger.critical(f"No moderation enties found in source 'aws_rekognition_video_labels' ({file_search}'")
        return None

    def flatten_aws_rekognition_video_faces(self, run_options):
        """Flatten AWS Rekognition Faces
            - https://docs.aws.amazon.com/rekognition/latest/dg/faces.html

        :param: run_options (dict): specific runtime information 
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """
        path_result = run_options['path_result']
        if path.exists(path_result) and ('force_overwrite' not in run_options or not run_options['force_overwrite']):
            return True
        list_items = []
        face_feats = ['AgeRange', 'Smile', 'Eyeglasses', 'Sunglasses', 'Gender', 'Beard', 'Mustache', 
                     'EyesOpen', 'MouthOpen', 'Pose']  # , 'Landmarks', 'Quality']  -- propose we skip these (emz 1/30
        
        last_load_idx = 0
        while last_load_idx >= 0:
            file_search = f"result{last_load_idx}.json"
            dict_data = contentai.get_extractor_results("aws_rekognition_video_faces", file_search)
            if not dict_data:  # do we need to load it locally?
                path_content = path.join(self.path_content, "aws_rekognition_video_faces", file_search)
                dict_data = json_load(path_content)
                if not dict_data:
                    path_content += ".gz"
                    dict_data = json_load(path_content)
            if not dict_data:  # couldn't load anything else...
                if list_items:
                    return pd.DataFrame(list_items)
                else:
                    last_load_idx = -1
                    break

            logger.info(f"... parsing aws_rekognition_video_faces/{file_search} ")

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
                    if "Emotions" in local_obj and local_obj["Emotions"]:
                        emotion_obj = {}
                        for emo_obj in local_obj["Emotions"]:
                            score_emo = round(float(emo_obj["Confidence"])/100, 4)
                            # if score_emo > 0.05   # consider a threshold?
                            emotion_obj[emo_obj["Type"]] = score_emo
                        details_obj['Emotions'] = emotion_obj
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

                    list_items.append({"time_begin": time_frame, "source_event": "image", "tag_type": "face",
                        "time_end": time_frame, "time_event": time_frame, "type_event": "face",
                        "tag": "Face", "score": score_frame, "details": json.dumps(details_obj),
                        "extractor": "aws_rekognition_video_faces"})

            last_load_idx += 1

        logger.critical(f"No faces found in source 'aws_rekognition_video_faces' ({file_search}'")
        return None    

    def flatten_aws_rekognition_video_person_tracking(self, run_options):
        """Flatten AWS Rekognition Person Tracking
            - https://docs.aws.amazon.com/rekognition/latest/dg/persons.html

        :param: run_options (dict): specific runtime information
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """
        list_items = []
        
        last_load_idx = 0
        while last_load_idx >= 0:
            file_search = f"result{last_load_idx}.json"
            dict_data = contentai.get_extractor_results("aws_rekognition_video_person_tracking", file_search)
            if not dict_data:  # do we need to load it locally?
                path_content = path.join(self.path_content, "aws_rekognition_video_person_tracking", file_search)
                dict_data = json_load(path_content)
                if not dict_data:
                    path_content += ".gz"
                    dict_data = json_load(path_content)
            if not dict_data:  # couldn't load anything else...
                if list_items:
                    return pd.DataFrame(list_items)
                else:
                    last_load_idx = -1
                    break

            logger.info(f"... parsing aws_rekognition_video_person_tracking/{file_search} ")

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

        logger.critical(f"No people found in source 'aws_rekognition_video_person_tracking' ({file_search}'")
        return None

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
            input_vars = {'path_result': path_output, "force_overwrite": True, "compressed": True}
            if contentai.metadata is not None:  # see README.md for more info
                input_vars.update(contentai.metadata)

            if "compressed" in input_vars and input_vars["compressed"]:  # allow compressed version
                input_vars["path_result"] += ".gz"

            if path.exists(input_vars['path_result']) and input_vars['force_overwrite']:
                logger.info(f"Skipping re-process of {input_vars['path_result']}...")
            else:
                logger.info(f"ContentAI argments: {input_vars}")
                df = func(input_vars)  # attempt to process
                df.sort_values("time_begin").to_csv(input_vars['path_result'], index=False)
                logger.info(f"Wrote {len(df)} items to result file '{input_vars['path_result']}'")

        except AttributeError as e:
            logger.info(f"Flatten function for '{extractor_name}' not found, skipping")
        except Exception as e:
            raise e
        pass


if __name__ == "__main__":
    main()
