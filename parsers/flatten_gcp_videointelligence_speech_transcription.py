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

from os import path
from . import Flatten

class Parser(Flatten):
    def __init__(self, path_content):
        super().__init__(path_content)

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
        dict_data = self.get_extractor_results("gcp_videointelligence_speech_transcription", "data.json")
        if not dict_data:  # do we need to load it locally?
            path_content = path.join(self.path_content, "gcp_videointelligence_speech_transcription", "data.json")
            dict_data = self.json_load(path_content)
            if not dict_data:
                path_content += ".gz"
                dict_data = self.json_load(path_content)

        if "annotationResults" not in dict_data:
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
                                "extractor": "gcp_videointelligence_speech_transcription"})
                            num_words += 1

                        if "transcript" in alt_obj:  # generate top-level transcript item, after going through all words
                            list_items.append( {"time_begin": time_begin, "source_event": "speech", "tag_type": "transcript",
                                "time_end": time_end, "time_event": time_begin, "tag": Flatten.TAG_TRANSCRIPT,
                                "score": float(alt_obj["confidence"]), 
                                "details": json.dumps({"words": num_words, "transcript": alt_obj["transcript"]}),
                                "extractor": "gcp_videointelligence_speech_transcription"})

        # added duplicate drop 0.4.1 for some reason this extractor has this bad tendency
        if len(list_items) > 0:
            return pd.DataFrame(list_items).drop_duplicates()
        self.logger.critical(f"Missing nested 'alternatives' in speechTranscriptions chunks from source 'gcp_videointelligence_speech_transcription'")
        return None
        