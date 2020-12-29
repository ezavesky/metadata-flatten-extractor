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
        self.EXTRACTOR = "ibm_max_audio_classifier"

    @staticmethod
    def known_types():
        """Return the output types for this generator
        :return: list.  List of output types (file types) for this generator
        """
        return ['tag']

    def parse(self, run_options):
        """Flatten IBM MAX Audio Classifier results  -- https://github.com/IBM/MAX-Audio-Classifier

        :param: run_options (dict): specific runtime information
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """
        dict_data = self.get_extractor_results(self.EXTRACTOR, "data.json")

        # [{"00:00:00": [{"label_id": "/m/01x3z", "label": "Clock", "probability": 0.08157104253768921}, 
        #   {"label_id": "/m/01v_m0", "label": "Sine wave", "probability": 0.07585805654525757}, 
        #   {"label_id": "/m/07qjznl", "label": "Tick-tock", "probability": 0.0729670524597168}, 
        #   {"label_id": "/m/07pp_mv", "label": "Alarm", "probability": 0.06461244821548462}, 
        #   {"label_id": "/m/04rlf", "label": "Music", "probability": 0.06068521738052368}], 
        # {"00:00:01": ... } ]

        base_obj = {"source_event": "audio", "tag_type": "tag", "extractor": self.EXTRACTOR}

        list_items = []
        idx_begin_last = 0
        time_begin_last = 0
        for time_code in dict_data:  # step through each second in asset
            # from this snippit -- https://stackoverflow.com/a/6402859
            time_begin = sum(int(x) * 60 ** i for i, x in enumerate(reversed(time_code.split(':'))))

            # update prior items to have a good start time
            if len(list_items):
                for idx_update in range(idx_begin_last, len(list_items)):
                    list_items[idx_update]["time_end"] = time_begin
                time_begin_last = list_items[idx_begin_last]["time_begin"]
            # interpolate end time las time
            time_end = (time_begin - time_begin_last) + time_begin
            idx_begin_last = len(list_items)

            if type(dict_data[time_code]) == list:   # not timing, is list
                for local_obj in dict_data[time_code]:   # iterate through all objects
                    if 'label' in local_obj and 'probability' in local_obj:  # validate the object input
                        new_obj = {"tag": local_obj['label'],
                            "time_begin": time_begin, "time_end": time_end,
                            "time_event": time_begin, "score": round(local_obj['probability'], self.ROUND_DIGITS),
                            "details": json.dumps({"model": local_obj['label_id']})}
                        new_obj.update(base_obj)
                        list_items.append(new_obj)

        if len(list_items) > 0:   # return the whole thing as dataframe
            return DataFrame(list_items)

        if run_options["verbose"]:
            self.logger.critical(f"Missing nested sections from source '{self.EXTRACTOR}'")
        return None
