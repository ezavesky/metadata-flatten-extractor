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
import re
import json
from pandas import DataFrame

from contentai_metadata_flatten.parsers import Flatten

class Parser(Flatten):
    def __init__(self, path_content, logger=None):
        super().__init__(path_content, logger=logger)
        self.EXTRACTOR = "gcp_videointelligence_speech_transcription"

    @staticmethod
    def known_types():
        """Return the output types for this generator
        :return: list.  List of output types (file types) for this generator
        """
        return ['keyword', 'transcript', 'identity']

    def parse(self, run_options):
        """Flatten GCP Speech Recognition 
            - https://cloud.google.com/video-intelligence/docs/transcription

        :param: run_options (dict): specific runtime information 
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """
        # read data.json
        #   "annotationResults": [ {  "speechTranscriptions": [ {
        #       "alternatives": [ { "transcript": "Play Super Bowl 50 for here tonight. ", "confidence": 0.8140063881874084,
        #       "words": [ { "startTime": "0s", "endTime": "0.400s", "word": "Play", "confidence": 0.9128385782241821 },
        dict_data = self.get_extractor_results(self.EXTRACTOR, "data.json")
        if "annotationResults" not in dict_data:
            if run_options["verbose"]:
                self.logger.critical(f"Missing nested 'annotationResults' from source 'gcp_videointelligence_speech_transcription'")
            return None

        list_items = []
        re_time_clean = re.compile(r"s$")
        for annotation_obj in dict_data["annotationResults"]:  # traverse items
            if "speechTranscriptions" not in annotation_obj:  # validate object
                self.logger.critical(f"Missing nested 'speechTranscriptions' in annotation chunk '{annotation_obj}'")
                return None
            for speech_obj in annotation_obj["speechTranscriptions"]:  # walk through all parts
                if "alternatives" not in speech_obj:
                    self.logger.critical(f"Missing nested 'alternatives' in speechTranscriptions chunk '{speech_obj}'")
                    return None

                for alt_obj in speech_obj["alternatives"]:   # walk through speech parts
                    time_end = 0
                    time_begin = 1e200
                    if "words" in alt_obj and len(alt_obj['words']) > 0:  # parse all words for this transcript chunk
                        speaker_last = None
                        speaker_begin = None
                        speaker_end = None
                        speaker_score = 0
                        speaker_segments = 0
                        num_words = 0
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
                                "extractor": self.EXTRACTOR})
                            num_words += 1

                            # { ... "confidence": 0.9128385782241821,  "speakerTag": 3 } ...  (added 0.8.6)
                            if "speakerTag" in word_obj:  # if speaker is consistent, add it here
                                reset_speaker = True
                                if word_obj["speakerTag"] == speaker_last:   # same speaker?
                                    if time_begin_clean == speaker_end:   # right after last speech segment? extend
                                        speaker_end = time_end_clean
                                        speaker_segments += 1
                                        speaker_score += float(word_obj["confidence"])
                                        reset_speaker = False
                                if reset_speaker:   # speaker mismatch or restart
                                    if speaker_begin is not None:   # close last speaker segment
                                        list_items.append( {"time_begin": speaker_begin, "source_event": "speech", "tag_type": "identity",
                                            "time_end": speaker_end, "time_event": speaker_begin, "tag": f"speaker_{speaker_last}",
                                            "score": round(speaker_score / speaker_segments, self.ROUND_DIGITS), "details": "",
                                            "extractor": self.EXTRACTOR})
                                    speaker_last = word_obj["speakerTag"]
                                    speaker_begin = time_begin_clean   # reset timing information
                                    speaker_end = time_end_clean
                                    speaker_segments = 1
                                    speaker_score = float(word_obj["confidence"])

                        if speaker_begin is not None:   # close last speaker segment
                            list_items.append( {"time_begin": speaker_begin, "source_event": "speech", "tag_type": "identity",
                                "time_end": speaker_end, "time_event": speaker_begin, "tag": f"speaker_{speaker_last}",
                                "score": round(speaker_score / speaker_segments, self.ROUND_DIGITS), "details": "",
                                "extractor": self.EXTRACTOR})

                        if "transcript" in alt_obj:  # generate top-level transcript item, after going through all words
                            list_items.append( {"time_begin": time_begin, "source_event": "speech", "tag_type": "transcript",
                                "time_end": time_end, "time_event": time_begin, "tag": Flatten.TAG_TRANSCRIPT,
                                "score": float(alt_obj["confidence"]), 
                                "details": json.dumps({"words": num_words, "transcript": alt_obj["transcript"]}),
                                "extractor": self.EXTRACTOR})



        # added duplicate drop 0.4.1 for some reason this extractor has this bad tendency
        if len(list_items) > 0:
            return DataFrame(list_items).drop_duplicates()
        if run_options["verbose"]:
            self.logger.critical(f"Missing nested 'alternatives' in speechTranscriptions chunks from source 'gcp_videointelligence_speech_transcription'")
        return None
        
