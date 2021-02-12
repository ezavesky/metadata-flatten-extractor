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
from pandas import DataFrame
import json
import re

from pytimeparse import parse as pt_parse

from contentai_metadata_flatten.parsers import Flatten

class Parser(Flatten):
    def __init__(self, path_content, logger=None):
        super().__init__(path_content, logger=logger)
        self.EXTRACTOR = "azure_videoindexer"
        self.SCORE_DEFAULT = 0.75

    @staticmethod
    def known_types():
        """Return the output types for this generator
        :return: list.  List of output types (file types) for this generator
        """
        return ['topic', 'keyword', 'identity', 'sentiment', 'emotion', 'tag', 'scene', 'brand', 'entity', 'shot', 'transcript']

    def parse(self, run_options):
        """Flatten Azure Indexing

        :param: run_options (dict): specific runtime information
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """
        dict_data = self.get_extractor_results(self.EXTRACTOR, "data.json")
        re_time_clean = re.compile(r"s$")
        list_items = []

        if "summarizedInsights" in dict_data:  # overall validation
            insight_obj = dict_data["summarizedInsights"]
            # TODO: consider alternate name for this instead of 'topic'
            if "topics" in insight_obj:  # loop over topics
                detail_map = {"iabName": 'iab', "iptcName": "iptc", "referenceUrl": "url"}
                for local_obj in insight_obj['topics']:
                    if "name" in local_obj and "appearances" in local_obj:  # validate object
                        details_obj = {}
                        for detail_name in detail_map:
                            if detail_name in local_obj and local_obj[detail_name] is not None:  # only if valid
                                details_obj[detail_map[detail_name]] = local_obj[detail_name]
                        for time_obj in local_obj["appearances"]:  # walk through all appearances
                            list_items.append({"time_begin": time_obj['startSeconds'], "source_event": "video", "tag_type": "topic",
                                "time_end": time_obj['endSeconds'], "time_event": time_obj['startSeconds'], "tag": local_obj["name"],
                                "score":  local_obj['confidence'], "details": json.dumps(details_obj),
                                "extractor": self.EXTRACTOR})
            # end of processing 'summarized insights'

        if "videos" in dict_data:  # overall validation
            for video_obj in dict_data["videos"]:  # overall validation
                insight_obj = video_obj["insights"]
                if "faces" in insight_obj:  # loop over faces
                    for local_obj in insight_obj['faces']:
                        if "name" in local_obj and "instances" in local_obj:  # validate object
                            if not local_obj["name"].startswith("Unknown"):   # full-fledged celebrity
                                details_obj = {"title": local_obj["title"], "description": local_obj['description'], 'url': local_obj['imageUrl']}
                                for time_obj in local_obj["instances"]:  # walk through all appearances
                                    time_begin = pt_parse(time_obj['start'])
                                    time_end = pt_parse(time_obj['end'])
                                    list_items.append({"time_begin": time_begin, "source_event": "face", "tag_type": "identity",
                                        "time_end": time_end, "time_event": time_begin, "tag": local_obj["name"],
                                        "score": local_obj['confidence'], "details": json.dumps(details_obj),
                                        "extractor": self.EXTRACTOR})
                            # TODO: handle others that ar emarked as 'unknown'?  (maybe not because no boundign rect)

                if "keywords" in insight_obj:  # loop over keywords
                    for local_obj in insight_obj['keywords']:
                        # TODO: enable raw keywords?  (note this is not transcript/ASR)
                        if False and "name" in local_obj and "instances" in local_obj:  # validate object
                            for time_obj in local_obj["instances"]:  # walk through all appearances
                                time_begin = pt_parse(time_obj['start'])
                                time_end = pt_parse(time_obj['end'])
                                list_items.append({"time_begin": time_begin, "source_event": "speech", "tag_type": "keyword",
                                    "time_end": time_end, "time_event": time_begin, "tag": local_obj["name"],
                                    "score": self.SCORE_DEFAULT, "details": "",
                                    "extractor": self.EXTRACTOR})

                if "sentiments" in insight_obj:  # loop over sentiment
                    for local_obj in insight_obj['sentiments']:
                        if "sentimentType" in local_obj and "instances" in local_obj:  # validate object
                            for time_obj in local_obj["instances"]:  # walk through all appearances
                                time_begin = pt_parse(time_obj['start'])
                                time_end = pt_parse(time_obj['end'])
                                list_items.append({"time_begin": time_begin, "source_event": "video", "tag_type": "sentiment",
                                    "time_end": time_end, "time_event": time_begin, "tag": local_obj["sentimentType"],
                                    "score": local_obj["averageScore"], "details": "",
                                    "extractor": self.EXTRACTOR})

                if "emotions" in insight_obj:  # loop over emotions
                    for local_obj in insight_obj['emotions']:
                        if "type" in local_obj and "instances" in local_obj:  # validate object
                            for time_obj in local_obj["instances"]:  # walk through all appearances
                                time_begin = pt_parse(time_obj['start'])
                                time_end = pt_parse(time_obj['end'])
                                # update to audio-only indicator for azure emotion
                                list_items.append({"time_begin": time_begin, "source_event": "audio", "tag_type": "emotion",
                                    "time_end": time_end, "time_event": time_begin, "tag": local_obj["type"],
                                    "score": time_obj["confidence"], "details": "",
                                    "extractor": self.EXTRACTOR})

                if "audioEffects" in insight_obj:  # loop over audio
                    for local_obj in insight_obj['audioEffects']:
                        if "type" in local_obj and "instances" in local_obj:  # validate object
                            for time_obj in local_obj["instances"]:  # walk through all appearances
                                time_begin = pt_parse(time_obj['start'])
                                time_end = pt_parse(time_obj['end'])
                                list_items.append({"time_begin": time_begin, "source_event": "audio", "tag_type": "tag",
                                    "time_end": time_end, "time_event": time_begin, "tag": local_obj["type"],
                                    "score": self.SCORE_DEFAULT, "details": "",
                                    "extractor": self.EXTRACTOR})

                if "labels" in insight_obj:  # loop over labels
                    for local_obj in insight_obj['labels']:
                        if "name" in local_obj and "instances" in local_obj:  # validate object
                            details_obj = {}
                            if "referenceId" in local_obj:
                                details_obj["category"] = local_obj["referenceId"]
                            for time_obj in local_obj["instances"]:  # walk through all appearances
                                time_begin = pt_parse(time_obj['start'])
                                time_end = pt_parse(time_obj['end'])
                                list_items.append({"time_begin": time_begin, "source_event": "video", "tag_type": "tag",
                                    "time_end": time_end, "time_event": time_begin, "tag": local_obj["name"],
                                    "score": time_obj["confidence"], "details": json.dumps(details_obj),
                                    "extractor": self.EXTRACTOR})

                if "framePatterns" in insight_obj:  # loop over frame; update 0.7.0, move to scene type
                    for local_obj in insight_obj['framePatterns']:
                        if "patternType" in local_obj and "instances" in local_obj:  # validate object
                            for time_obj in local_obj["instances"]:  # walk through all appearances
                                time_begin = pt_parse(time_obj['start'])
                                time_end = pt_parse(time_obj['end'])
                                list_items.append({"time_begin": time_begin, "source_event": "video", "tag_type": "scene",
                                    "time_end": time_end, "time_event": time_begin, "tag": local_obj["patternType"],
                                    "score": local_obj['confidence'], "details": "",
                                    "extractor": self.EXTRACTOR})

                if "brands" in insight_obj:  # loop over frame
                    for local_obj in insight_obj['brands']:
                        if "name" in local_obj and "instances" in local_obj:  # validate object
                            details_obj = {"url": local_obj["referenceUrl"], "description": local_obj['description']}
                            for time_obj in local_obj["instances"]:  # walk through all appearances
                                time_begin = pt_parse(time_obj['start'])
                                time_end = pt_parse(time_obj['end'])
                                list_items.append({"time_begin": time_begin, "source_event": "video", "tag_type": "brand",
                                    "time_end": time_end, "time_event": time_begin, "tag": local_obj["name"],
                                    "score":  local_obj['confidence'], "details": json.dumps(details_obj),
                                    "extractor": self.EXTRACTOR})

                if "namedLocations" in insight_obj:  # loop over named entities
                    for local_obj in insight_obj['namedLocations']:
                        if "name" in local_obj and "instances" in local_obj:  # validate object
                            details_obj = {"url": local_obj["referenceUrl"], "description": local_obj['description']}
                            for time_obj in local_obj["instances"]:  # walk through all appearances
                                time_begin = pt_parse(time_obj['start'])
                                time_end = pt_parse(time_obj['end'])
                                source_type = "image" if time_obj['instanceSource'] == "Ocr" else "speech"
                                list_items.append({"time_begin": time_begin, "source_event": source_type, "tag_type": "entity",
                                    "time_end": time_end, "time_event": time_begin, "tag": local_obj["name"],
                                    "score":  local_obj['confidence'], "details": json.dumps(details_obj),
                                    "extractor": self.EXTRACTOR})

                if "namedPeople" in insight_obj:  # loop over named entities
                    for local_obj in insight_obj['namedPeople']:
                        if "instances" in local_obj:  # validate object
                            details_obj = {"url": local_obj["referenceUrl"], "description": local_obj['description']}
                            for time_obj in local_obj["instances"]:  # walk through all appearances
                                time_begin = pt_parse(time_obj['start'])
                                time_end = pt_parse(time_obj['end'])
                                source_type = "image" if time_obj['instanceSource'] == "Ocr" else "speech"
                                list_items.append({"time_begin": time_begin, "source_event": source_type, "tag_type": "entity",
                                    "time_end": time_end, "time_event": time_begin, "tag": local_obj["name"],
                                    "score":  local_obj['confidence'], "details": json.dumps(details_obj),
                                    "extractor": self.EXTRACTOR})

                # TODO: consider adding 'textualContentModeration'

                if "visualContentModeration" in insight_obj:  # loop over named moderation
                    score_map = {'adultScore': 'adult', 'racyScore': 'racy'}
                    for local_obj in insight_obj['visualContentModeration']:
                        if "instances" in local_obj:  # validate object
                            for time_obj in local_obj["instances"]:  # walk through all appearances
                                time_begin = pt_parse(time_obj['start'])
                                time_end = pt_parse(time_obj['end'])
                                for type_moderation in score_map:
                                    if local_obj[type_moderation] > 0.01:
                                        list_items.append({"time_begin": time_begin, "source_event": "image",  "tag_type": "moderation",
                                            "time_end": time_end, "time_event": time_begin, "tag": score_map[type_moderation],
                                            "score": local_obj[type_moderation], "details": "",
                                            "extractor": self.EXTRACTOR})

                if "transcript" in insight_obj:  # loop over transcripts
                    for local_obj in insight_obj['transcript']:
                        if "text" in local_obj and "instances" in local_obj and len(local_obj["text"]) > 0:  # validate object
                            for time_obj in local_obj["instances"]:  # walk through all appearances
                                time_begin = pt_parse(time_obj['start'])
                                time_end = pt_parse(time_obj['end'])
                                list_items.append( {"time_begin": time_begin, "source_event": "speech", "tag_type": "transcript",
                                    "time_end": time_end, "time_event": time_begin, "tag": Flatten.TAG_TRANSCRIPT,
                                    "score": float(local_obj["confidence"]), 
                                    "details": json.dumps({ "transcript": local_obj["text"]}),
                                    "extractor": self.EXTRACTOR})

                if "speakers" in insight_obj:  # loop over speakers (added 0.9.1)
                    for local_obj in insight_obj['speakers']:
                        if "id" in local_obj and "instances" in local_obj and len(local_obj["instances"]) > 0:  # validate object
                            for speaker_obj in local_obj['instances']:
                                time_begin = pt_parse(speaker_obj['start'])
                                time_end = pt_parse(speaker_obj['end'])
                                speaker_label = f"speaker_{local_obj['id']}"
                                # TODO: should we use recognition probability in this interval instead of just 1.0?
                                list_items.append( {"time_begin": time_begin, "source_event": "speech", "tag_type": "identity",
                                    "time_end": time_end, "time_event": time_begin, "tag": f"speaker_{speaker_label}",
                                    "score": self.SCORE_DEFAULT, "details": "",
                                    "extractor": self.EXTRACTOR})

                if "ocr" in insight_obj:  # loop over ocr
                    for local_obj in insight_obj['ocr']:
                        if "text" in local_obj and "instances" in local_obj and len(local_obj["text"]) > 0:  # validate object
                            local_box = {'box': {'w': round(local_obj['width'], self.ROUND_DIGITS), 'h': round(local_obj['height'], self.ROUND_DIGITS),
                                                'l': round(local_obj['left'], self.ROUND_DIGITS), 't': round(local_obj['top'], self.ROUND_DIGITS)},
                                        'transcript': local_obj['text'] }
                            for time_obj in local_obj["instances"]:  # walk through all appearances
                                time_begin = pt_parse(time_obj['start'])
                                time_end = pt_parse(time_obj['end'])
                                list_items.append( {"time_begin": time_begin, "source_event": "ocr", "tag_type": "transcript",
                                    "time_end": time_end, "time_event": time_begin, "tag": Flatten.TAG_TRANSCRIPT,
                                    "score": float(local_obj["confidence"]), 
                                    "details": json.dumps(local_box),
                                    "extractor": self.EXTRACTOR})

                if "shots" in insight_obj:  # loop over shot
                    for local_obj in insight_obj['shots']:
                        if "keyFrames" in local_obj and "instances" in local_obj:  # validate object
                            details_obj = { }
                            if 'tags' in local_obj:
                                details_obj['shot_type'] = local_obj['tags']

                            time_event = None
                            if 'keyFrames' in local_obj and local_obj['keyFrames']:   # try to get a specific keyframe
                                key_frame_obj = local_obj['keyFrames'][0]   # grab first frame
                                if "instances" in key_frame_obj:
                                    time_event = pt_parse(key_frame_obj['instances'][0]['start'])                            

                            for time_obj in local_obj["instances"]:  # walk through all appearances
                                time_begin = pt_parse(time_obj['start'])
                                time_end = pt_parse(time_obj['end'])
                                if time_event is None:
                                    time_event = time_begin
                                list_items.append( {"time_begin": time_begin, "source_event": "video", "tag_type": "shot",
                                    "time_end": time_end, "time_event": time_event, "tag": "shot",
                                    "score": self.SCORE_DEFAULT, "details": json.dumps(details_obj),
                                    "extractor": self.EXTRACTOR})

                if "scenes" in insight_obj:  # loop over scenes
                    for local_obj in insight_obj['scenes']:
                        if "instances" in local_obj:  # validate object
                            for time_obj in local_obj["instances"]:  # walk through all appearances
                                time_begin = pt_parse(time_obj['start'])
                                time_end = pt_parse(time_obj['end'])
                                list_items.append( {"time_begin": time_begin, "source_event": "video", "tag_type": "scene",
                                    "time_end": time_end, "time_event": time_begin, "tag": "scene",
                                    "score": self.SCORE_DEFAULT, "details": "",
                                    "extractor": self.EXTRACTOR})

        if len(list_items) > 0:   # return the whole thing as dataframe
            return DataFrame(list_items)

        if run_options["verbose"]:
            self.logger.critical(f"Missing nested 'summarizedInsights' or 'videos' from source 'azure_videoindexer'")
        return None
