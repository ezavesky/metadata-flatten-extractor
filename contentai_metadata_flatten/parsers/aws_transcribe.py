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
        self.EXTRACTOR = "aws_transcribe"

    @staticmethod
    def known_types():
        """Return the output types for this generator
        :return: list.  List of output types (file types) for this generator
        """
        return ['keyword', 'transcript', 'identity']

    def parse(self, run_options):
        """Flatten AWS Transcription
            - https://docs.aws.amazon.com/transcribe/

        :param: run_options (dict): specific runtime information
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """
        dict_data = self.get_extractor_results(self.EXTRACTOR, "data.json")
        if "results" not in dict_data or "items" not in dict_data["results"]:
            if run_options["verbose"]:
                self.logger.critical(f"Missing nested 'results' from source '{self.EXTRACTOR}'")
            return None

        # read data.json
        #   "results": { "transcripts": { "transcript": xxx }, 
        #       "items": [ { "start_time": "5.96", "end_time": "6.46", "alternatives": [ 
        #           { "confidence": "1.0", "content": "Hello" } ], "type": "pronunciation" }, ... ]
        #       "items },

        list_items = []

        for local_obj in dict_data["results"]["items"]:  # traverse items
            if local_obj["type"] == "pronunciation" and "start_time" in local_obj:
                time_begin = float(local_obj["start_time"])
                time_end = float(local_obj["end_time"])
                for trans_obj in local_obj["alternatives"]:   # add new item for this word
                    list_items.append( {"time_begin": time_begin, "source_event": "speech", "tag_type": "word",
                        "time_end": time_end, "time_event": time_begin, "tag": trans_obj["content"],
                        "score": float(trans_obj["confidence"]), "details": "",
                        "extractor": self.EXTRACTOR})

        if len(list_items) > 0:
            time_begin = list_items[0]['time_begin']
            time_end = list_items[-1]['time_end']

            if "transcripts" in dict_data["results"]:
                for trans_obj in dict_data["results"]["transcripts"]:
                    str_trans = trans_obj["transcript"]
                    num_words = len(re.split(r"\s+", str_trans))
                    list_items.append( {"time_begin": time_begin, "source_event": "speech", "tag_type": "transcript",
                        "time_end": time_end, "time_event": time_begin, "tag": Flatten.TAG_TRANSCRIPT,
                        "score": self.SCORE_DEFAULT, "details": json.dumps({"words": num_words, "transcript": str_trans}),
                        "extractor": self.EXTRACTOR})

        # add speakers as identity?
        if "speaker_labels" in dict_data["results"] and len(dict_data["results"]["speaker_labels"]["segments"]) > 0:
            for local_obj in dict_data["results"]["speaker_labels"]["segments"]:
                time_begin = float(local_obj["start_time"])
                time_end = float(local_obj["end_time"])
                speaker_label = local_obj["speaker_label"].split('_')[-1]
                # TODO: should we use recognition probability in this interval instead of just 1.0?
                list_items.append( {"time_begin": time_begin, "source_event": "speech", "tag_type": "identity",
                    "time_end": time_end, "time_event": time_begin, "tag": f"speaker_{speaker_label}",
                    "score": self.SCORE_DEFAULT, "details": "",
                    "extractor": self.EXTRACTOR})

        if len(list_items) > 0:
            return DataFrame(list_items).drop_duplicates()
        if run_options["verbose"]:
            self.logger.critical(f"Missing nested 'results' from source '{self.EXTRACTOR}'")
        return None
        
