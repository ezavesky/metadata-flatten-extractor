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

from pytimeparse import parse as pt_parse

from contentai_metadata_flatten.parsers import Flatten

class Parser(Flatten):
    def __init__(self, path_content, logger=None):
        super().__init__(path_content, logger=logger)
        self.EXTRACTOR = "dsai_metadata"
        self.SCORE_DEFAULT_FIXED = 0.75

    @staticmethod
    def known_types():
        """Return the output types for this generator
        :return: list.  List of output types (file types) for this generator
        """
        return ['keyword', 'identity', 'tag', 'scene', 'topic', 'brand', 'shot', 'transcript']

    def parse(self, run_options):
        """Flatten CAE Indexing results

        :param: run_options (dict): specific runtime information
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """
        dict_data = self.get_extractor_results(self.EXTRACTOR, "metadata.json")

        list_items = []
        list_keywords = []
        if "keywords" in dict_data:  # loop over keywords
            for local_obj in dict_data['keywords']:
                list_keywords.append({'tag':local_obj, 'tag_type':"keyword"})

        # TODO: populate with other keywords (e.g. trending?)

        key_sentence = {}
        if "smartTags" in dict_data:  # loop over smart tags
            for local_type in ["programNE", "epgNE", "commercialNE"]:
                if local_type in dict_data["smartTags"]:
                    for local_obj in dict_data["smartTags"][local_type]:
                        if "neType" in local_obj and "namedEntity" in local_obj and "sentNumber" in local_obj:  # validate object
                            insight_obj = {"tag": local_obj["namedEntity"], "tag_type": "entity",  # could be 'brand' too or 'identity'?
                                "details": {"type": local_obj["neType"], 'description': local_obj['neLabel'], 'weight': local_obj['weight'] } }
                            for sent_id in local_obj["sentNumber"]:
                                sent_id = int(sent_id)
                                if sent_id not in key_sentence:  # save this instance data a this sentence
                                    key_sentence[sent_id] = []
                                key_sentence[sent_id].append(insight_obj)

        if "sent" in dict_data:  # loop over transcripts
            insight_obj = dict_data["sent"]
            for local_obj in insight_obj:
                if "text" in local_obj and "start" in local_obj and len(local_obj["text"]) > 0:  # validate object
                    time_begin = float(local_obj['start'])/1000
                    time_duration = float(local_obj['duration'])/1000
                    detail_obj = { "transcript": local_obj["text"] }
                    if "ccstart" in local_obj:
                        detail_obj['caption'] = {"time_begin": float(local_obj['ccstart'])/1000}
                        detail_obj['caption']["time_end"] = float(local_obj['ccduration'])/1000 + detail_obj['caption']["time_begin"]
                    list_items.append( {"time_begin": time_begin, "source_event": "speech", "tag_type": "transcript",
                        "time_end": time_begin + time_duration, "time_event": time_begin, "tag": Flatten.TAG_TRANSCRIPT,
                        "score": self.SCORE_DEFAULT_FIXED, "details": json.dumps(detail_obj), "extractor": self.EXTRACTOR})

                    # process other named entities that indicted this sentence
                    sent_id = int(local_obj["number"])
                    if sent_id in key_sentence:
                        for insight_obj in key_sentence[sent_id]:
                            list_items.append( {"time_begin": time_begin, "source_event": "speech", "tag_type": insight_obj['tag_type'],
                                "time_end": time_begin + time_duration, "time_event": time_begin, "tag": insight_obj['tag'],
                                "score": self.SCORE_DEFAULT, "details": json.dumps(insight_obj['details']), "extractor": self.EXTRACTOR})

                    # now process quickly for keywords
                    lower_scan = local_obj["text"].lower()
                    for insight_obj in list_keywords:
                        if insight_obj['tag'].lower() in lower_scan:   # just check for presence
                            list_items.append( {"time_begin": time_begin, "source_event": "speech", "tag_type": insight_obj['tag_type'],
                                "time_end": time_begin + time_duration, "time_event": time_begin, "tag": insight_obj['tag'],
                                "score": self.SCORE_DEFAULT, "details": "", "extractor": self.EXTRACTOR})

        if "silence" in dict_data:  # loop over audio
            for local_obj in dict_data['silence']:
                if "start" in local_obj and "duration" in local_obj:  # validate object
                    time_begin = float(local_obj['start'])/1000
                    time_duration = float(local_obj['duration'])/1000
                    list_items.append({"time_begin": time_begin, "source_event": "audio", "tag_type": "tag",
                        "time_end": time_begin + time_duration, "time_event": time_begin, "tag": "silence",
                        "score": self.SCORE_DEFAULT_FIXED, "details": "", "extractor": self.EXTRACTOR})

        if "audio" in dict_data:  # loop over audio concepts
            if 'regions' in dict_data['audio']:
                for local_obj in dict_data['audio']['regions']:
                    if "start" in local_obj and "duration" in local_obj and 'concepts' in local_obj:  # validate object
                        time_begin = float(local_obj['start'])/1000
                        time_duration = float(local_obj['duration'])/1000
                        for score_obj in local_obj['concepts']:
                            list_items.append({"time_begin": time_begin, "source_event": "audio", "tag_type": "tag",
                                "time_end": time_begin + time_duration, "time_event": time_begin, "tag": score_obj['name'],
                                "score": round(float(score_obj['score']), self.ROUND_DIGITS), "details": "", "extractor": self.EXTRACTOR})

        if "commercial" in dict_data:  # loop over scenes
            for local_obj in dict_data['commercial']:
                if "start" in local_obj and "duration" in local_obj:  # validate object
                    time_begin = float(local_obj['start'])/1000
                    time_duration = float(local_obj['duration'])/1000
                    list_items.append({"time_begin": time_begin, "source_event": "video", "tag_type": "scene",
                        "time_end": time_begin + time_duration, "time_event": time_begin, "tag": "commercial",
                        "score": self.SCORE_DEFAULT, "details": "", "extractor": self.EXTRACTOR})

        for local_type in ['tms', 'iab']:  # loop over TMS and IAB concepts
            if local_type in dict_data and 'regions' in dict_data[local_type]:
                for local_obj in dict_data[local_type]['regions']:
                    if "start" in local_obj and "duration" in local_obj and 'concepts' in local_obj:  # validate object
                        time_begin = float(local_obj['start'])/1000
                        time_duration = float(local_obj['duration'])/1000
                        for score_obj in local_obj['concepts']:
                            list_items.append({"time_begin": time_begin, "source_event": "video", "tag_type": "topic",
                                "time_end": time_begin + time_duration, "time_event": time_begin, "tag": score_obj['name'],
                                "score": round(float(score_obj['score']), self.ROUND_DIGITS), "details": "", "extractor": self.EXTRACTOR})


        if "mmimg" in dict_data:  # overall validation
            if 'images' in dict_data['mmimg'] and 'width' in dict_data['mmimg'] and 'height' in dict_data['mmimg']:  # overall validation
                img_width = float(dict_data['mmimg']['width'])
                img_height = float(dict_data['mmimg']['width'])
                img_timing = {}
                
                img_id_last = -1
                kfcluster_max = self.SCORE_DEFAULT   # this is given in raw distance, so we're going to min/max normalize
                for local_obj in dict_data['mmimg']['images']:  # loop over images for timing construction
                    time_begin = float(local_obj['start'])/1000
                    img_id = int(local_obj['id'])
                    img_timing[img_id] = {'time_begin': time_begin, 'time_end': time_begin}
                    if (img_id - 1) in img_timing:  # seek back for end timing
                        img_timing[img_id - 1]['time_end'] = img_timing[img_id]['time_begin'] - 1/1000
                    img_id_last = img_id
                    if 'kfcluster' in local_obj and len(local_obj['kfcluster']):
                        kfcluster_max = max(kfcluster_max, float(local_obj['kfcluster']['score']))
                if img_id_last > -1 and 'duration' in dict_data:  # time the final image/shot
                    img_timing[img_id_last]['time_end'] = float(dict_data['duration'])/1000 - 1/1000
                    
                for local_obj in dict_data['mmimg']['images']:  # loop over images for other extraction
                    details_obj = {}
                    img_id = int(local_obj['id'])
                    if 'type' in local_obj:  # udpate 0.7.0, make into an array
                        details_obj['shot_type'] = [local_obj['type']]
                    # first, publish the shot for this image
                    list_items.append( {"time_begin": img_timing[img_id]['time_begin'], "source_event": "video", "tag_type": "shot",
                        "time_end": img_timing[img_id]['time_end'], "time_event": img_timing[img_id]['time_begin'], "tag": "shot",
                        "score": self.SCORE_DEFAULT_FIXED, "details": json.dumps(details_obj),
                        "extractor": self.EXTRACTOR})
                    
                    if "face" in local_obj:  # process faces
                        for insight_obj in local_obj['face']:
                            details_obj = {}
                            if 'x' in insight_obj and 'w' in insight_obj:
                                details_obj['box'] = {'w': round(float(insight_obj['w']) / img_width, self.ROUND_DIGITS), 
                                    'h': round(float(insight_obj['h']) / img_width, self.ROUND_DIGITS),
                                    'l': round(float(insight_obj['x']) / img_height, self.ROUND_DIGITS), 
                                    't': round(float(insight_obj['y']) / img_height, self.ROUND_DIGITS) }
                            if 'rec' in insight_obj:   # specific identity
                                list_items.append( {"time_begin": img_timing[img_id]['time_begin'], "source_event": "face", "tag_type": "identity",
                                    "time_end": img_timing[img_id]['time_end'], "time_event": img_timing[img_id]['time_begin'], 
                                    "tag": insight_obj['rec']['name'].replace("_", " "),
                                    "score": float(insight_obj['rec']['confidence']), "details": json.dumps(details_obj),
                                    "extractor": self.EXTRACTOR})

                            if 'cluster' in insight_obj:   # general face cluster
                                list_items.append( {"time_begin": img_timing[img_id]['time_begin'], "source_event": "face", "tag_type": "identity",
                                    "time_end": img_timing[img_id]['time_end'], "time_event": img_timing[img_id]['time_begin'], 
                                    "tag": f"face_cluster_{insight_obj['cluster']['id']}",
                                    "score": min(self.SCORE_DEFAULT_FIXED, float(insight_obj['cluster']['score'])), "details": json.dumps(details_obj),
                                    "extractor": self.EXTRACTOR})
                    
                    object_map = {'logo': 'brand', 'object': 'tag'}
                    for local_type in object_map:  # loop over logo and object
                        if local_type in local_obj:
                            for insight_obj in local_obj[local_type]:
                                details_obj = {}
                                if 'x' in insight_obj and 'w' in insight_obj:
                                    details_obj['box'] = {'w': round(float(insight_obj['w']) / img_width, self.ROUND_DIGITS), 
                                        'h': round(float(insight_obj['h']) / img_width, self.ROUND_DIGITS),
                                        'l': round(float(insight_obj['x']) / img_height, self.ROUND_DIGITS), 
                                        't': round(float(insight_obj['y']) / img_height, self.ROUND_DIGITS) }
                                list_items.append( {"time_begin": img_timing[img_id]['time_begin'], "source_event": "image", "tag_type": object_map[local_type],
                                    "time_end": img_timing[img_id]['time_end'], "time_event": img_timing[img_id]['time_begin'], 
                                    "tag": insight_obj['name'].replace("_", " "),
                                    "score": round(min(self.SCORE_DEFAULT_FIXED, float(insight_obj['score'])), self.ROUND_DIGITS), "details": json.dumps(details_obj),
                                    "extractor": self.EXTRACTOR})

                    if 'concept' in local_obj:   # process concepts
                        for insight_obj in local_obj['concept']:
                            list_items.append({"time_begin": img_timing[img_id]['time_begin'], "source_event": "image", "tag_type": "tag",
                                "time_end": img_timing[img_id]['time_end'], "time_event": img_timing[img_id]['time_begin'], 
                                "tag": insight_obj['name'], "score": round(float(insight_obj['score']), self.ROUND_DIGITS), "details": "", "extractor": self.EXTRACTOR})

                    if 'kfcluster' in local_obj and len(local_obj['kfcluster']):   # process kfcluster (duplicate frames)
                        details_obj = local_obj['kfcluster']
                        # TODO: investigate whether kfcluster score is a distance or a similarity; this code assumes distance!
                        list_items.append({"time_begin": img_timing[img_id]['time_begin'], "source_event": "image", "tag_type": "scene",
                            "time_end": img_timing[img_id]['time_end'], "time_event": img_timing[img_id]['time_begin'], 
                            "tag": "duplicate", "score": 1 - round(float(local_obj['kfcluster']['score']) / kfcluster_max, self.ROUND_DIGITS), 
                            "details": json.dumps(details_obj), "extractor": self.EXTRACTOR})

        if "mmpara" in dict_data:  # loop over paragraph segments to make scenes (from speech)
            for local_obj in dict_data['mmpara']:
                if "start" in local_obj and "duration" in local_obj:  # validate object
                    time_begin = float(local_obj['start'])/1000
                    time_duration = float(local_obj['duration'])/1000
                    details_obj = {}
                    if "sentstart" in local_obj and "sentend" in local_obj:  # retain number of sentences
                        details_obj = {'sentences': int(local_obj["sentend"]) - int(local_obj["sentstart"]) + 1}
                    list_items.append({"time_begin": time_begin, "source_event": "speech", "tag_type": "scene",
                        "time_end": time_begin + time_duration, "time_event": time_begin, "tag": "story",
                        "score": self.SCORE_DEFAULT, "details": json.dumps(details_obj), "extractor": self.EXTRACTOR})


        # TODO: additional parsing for these data
        # viewers  --> ??
        # segments  --> scenes?

        if len(list_items) > 0:   # return the whole thing as dataframe
            return DataFrame(list_items)

        if run_options["verbose"]:
            self.logger.critical(f"Missing nested sections from source '{self.EXTRACTOR}'")
        return None
