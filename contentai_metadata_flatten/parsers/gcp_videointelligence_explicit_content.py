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
import re

from contentai_metadata_flatten.parsers import Flatten

class Parser(Flatten):
    def __init__(self, path_content, logger=None):
        super().__init__(path_content, logger=logger)
        self.EXTRACTOR = "gcp_videointelligence_explicit_content"

    @staticmethod
    def known_types():
        """Return the output types for this generator
        :return: list.  List of output types (file types) for this generator
        """
        return ['moderation']

    def parse(self, run_options):
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
        dict_data = self.get_extractor_results(self.EXTRACTOR, "data.json")

        if "annotationResults" not in dict_data:
            if run_options["verbose"]:
                self.logger.critical(f"Missing nested 'annotationResults' from source 'gcp_videointelligence_explicit_content'")
            return None

        re_time_clean = re.compile(r"s$")
        for annotation_obj in dict_data["annotationResults"]:  # traverse items
            if "explicitAnnotation" in annotation_obj:  # validate object
                if "frames" not in annotation_obj["explicitAnnotation"]:  # validate object
                    self.logger.critical(f"Missing nested 'frames' in shot chunk '{annotation_obj['explicitAnnotation']}'")
                    return None
                list_items = []
                for frame_item in annotation_obj["explicitAnnotation"]["frames"]:
                    if "timeOffset" in frame_item:
                        time_clean = float(re_time_clean.sub('', frame_item["timeOffset"]))
                        dict_scores = {n:n.split("Likelihood")[0] for n in frame_item.keys() if not n.startswith("time") }
                        for n in dict_scores:  # a little bit of a dance, but flexiblity for future explicit types
                            list_items.append( {"time_begin": time_clean, "source_event": "image",  "tag_type": "moderation",
                                "time_end": time_clean, "time_event": time_clean, "tag": dict_scores[n],                   
                                "score": Flatten.GCP_LIKELIHOOD_MAP[frame_item[n]], "details": "",
                                "extractor": self.EXTRACTOR})
                return DataFrame(list_items)

        if run_options["verbose"]:
            self.logger.critical(f"Missing nested 'explicitAnnotation' from source 'gcp_videointelligence_explicit_content'")
        return None
        
