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
        self.EXTRACTOR = "dsai_musicnn"

    @staticmethod
    def known_types():
        """Return the output types for this generator
        :return: list.  List of output types (file types) for this generator
        """
        return ['tag']

    def parse(self, run_options):
        """Flatten DSAI MusicNN results

        :param: run_options (dict): specific runtime information
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """
        dict_data = self.get_extractor_results(self.EXTRACTOR, "data.json")

        list_timing = {}
        if "timing" in dict_data:  # look-up timing items first 
            for local_obj in dict_data['timing']:
                list_timing[local_obj['id']] = {"time_begin": local_obj['start'], "time_event": local_obj['start'],
                                                "time_end": local_obj['start'] + local_obj['duration']}
        if not list_timing:
            if run_options["verbose"]:
                self.logger.critical(f"Missing timing array for extractor '{self.EXTRACTOR}', aborting")
            return None

        list_items = []
        for type_classifier in dict_data:
            if type_classifier != "timing" and type(dict_data[type_classifier]) == list:   # not timing, is list
                for local_obj in dict_data[type_classifier]:   # iterate through all objects
                    if 'id' in local_obj and local_obj['id'] in list_timing:  # validate the object input
                        timing_obj = list_timing[local_obj['id']]  # deref for timing object
                        for tag_name in local_obj:
                            if tag_name != 'id':
                                new_obj = {"source_event": "audio", "tag_type": "tag", "tag": tag_name,
                                            "score": local_obj[tag_name], "details": json.dumps({"model": type_classifier}), 
                                            "extractor": self.EXTRACTOR}
                                new_obj.update(timing_obj)
                                list_items.append(new_obj)     

        if len(list_items) > 0:   # return the whole thing as dataframe
            return DataFrame(list_items)

        if run_options["verbose"]:
            self.logger.critical(f"Missing nested sections from source '{self.EXTRACTOR}'")
        return None
