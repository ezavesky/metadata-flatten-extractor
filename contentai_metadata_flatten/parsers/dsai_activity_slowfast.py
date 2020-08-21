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
from pandas import DataFrame, read_csv
from io import StringIO
import json

from pytimeparse import parse as pt_parse

from contentai_metadata_flatten.parsers import Flatten

class Parser(Flatten):
    def __init__(self, path_content, logger=None):
        super().__init__(path_content, logger=logger)
        self.EXTRACTOR = "dsai_activity_slowfast"

    @staticmethod
    def known_types():
        """Return the output types for this generator
        :return: list.  List of output types (file types) for this generator
        """
        return ['tag']

    def get_source_types(self, column_clean):
        # (activity)
        # video_clip,Time_begin,Time_end,Time_event,category0,score0,category1,score1,category2,score2
        # isnt_romantic_clip_00000.mp4,0.00,10.88,0.00,beatboxing,0.06312230030695597,answering questions,0.045281600952148435,archery,0.04213473399480184

        if "video_clip" in column_clean:  # suspect it's video activity
            return {'type': "video", 'column_prefix':['category', 'score']}
        return None

    def parse(self, run_options):
        """Flatten SlowFast actions results

        :param: run_options (dict): specific runtime information
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """
        dict_data = self.get_extractor_results(self.EXTRACTOR, "results.csv", is_json=False)
        if not dict_data:
            if run_options["verbose"]:
                self.logger.critical(f"Empty result string for extractor '{self.EXTRACTOR}', aborting")
            return None
        
        buffer_string = StringIO(dict_data)
        df_raw = read_csv(buffer_string)   # attempt to parse from string
        column_clean = [x.lower() for x in list(df_raw.columns)]   # convert all columns to lower case
        df_raw.columns = column_clean

        source_type = self.get_source_types(column_clean)
        if source_type is None:  # unknown parser 
            self.logger.critical(f"Unknown data type from detected CSV column set '{column_clean}', aborting")
            return None

        column_timing = ["time_begin", "time_end", "time_event"]
        for col_name in column_timing:
            if col_name not in column_clean:
                self.logger.critical(f"Missing critical timing event column set '{col_name}', aborting")
                return None
        df_raw[column_timing] = df_raw[column_timing].astype(float)   # convert to better time format
        
        list_items = []
        for row_idx, row_data in df_raw.iterrows():
            base_obj = {"source_event": source_type["type"], "tag_type": "tag", "extractor": self.EXTRACTOR}
            for col_name in column_timing:  # copy basic timing
                base_obj[col_name] = row_data[col_name]
        
            # iterate through other prefixes
            idx_prefix = 0
            while True:
                label_name = f"{source_type['column_prefix'][0]}{idx_prefix}"
                score_name = f"{source_type['column_prefix'][1]}{idx_prefix}"
                if not (label_name in column_clean and score_name in column_clean):  # stop looping
                    break
                else:
                    new_obj = {"score": row_data[score_name], "tag": row_data[label_name]}
                    new_obj.update(base_obj)
                    list_items.append(new_obj)
                idx_prefix += 1

        if len(list_items) > 0:   # return the whole thing as dataframe
            return DataFrame(list_items)

        if run_options["verbose"]:
            self.logger.critical(f"No valid events detected for '{self.EXTRACTOR}'")
        return None
