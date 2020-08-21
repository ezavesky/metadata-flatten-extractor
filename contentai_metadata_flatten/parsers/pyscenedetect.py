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
        self.EXTRACTOR = "pyscenedetect"

    @staticmethod
    def known_types():
        """Return the output types for this generator
        :return: list.  List of output types (file types) for this generator
        """
        return ['shot']

    def retrieve_output(self, file_name, run_options):
        """Helper to retrieve a specific file from chained output"""
        dict_data = self.get_extractor_results(self.EXTRACTOR, file_name, is_json=False)
        if not dict_data:
            if run_options["verbose"]:
                self.logger.critical(f"Empty result string for extractor '{self.EXTRACTOR}', aborting")
            return None
        return read_csv(StringIO(dict_data), skiprows=1)   # both input files have an extra header line

    def parse(self, run_options):
        """Flatten SlowFast actions results

        :param: run_options (dict): specific runtime information
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """
        df_frames = self.retrieve_output("stats.csv", run_options)
        df_scenes = self.retrieve_output("scenes.csv", run_options)
        if df_frames is None or df_scenes is None:
            if run_options["verbose"]:
                self.logger.critical(f"Empty shot or scenen file for extractor '{self.EXTRACTOR}', aborting")
            return None

        df_frames["Frame Number"] = df_frames["Frame Number"].astype(int)

        # (scene format)
        # Scene Number,Start Frame,Start Timecode,Start Time (seconds),End Frame,End Timecode,End Time (seconds),Length (frames),Length (timecode),Length (seconds)
        # 1,0,00:00:00.000,0.000,169,00:00:05.639,5.639,169,00:00:05.639,5.639

        # (frame format)
        # Frame Number,Timecode,content_val,delta_hue,delta_lum,delta_sat
        # 1,00:00:00.033,0.0,0.0,0.0,0.0

        base_obj = {"source_event": "video", "tag_type": "shot", "extractor": self.EXTRACTOR, "score": self.SCORE_DEFAULT}

        list_items = []
        for row_idx, row_data in df_scenes.iterrows():
            item_new = {"time_begin": round(row_data["Start Time (seconds)"], self.ROUND_DIGITS),
                        "time_end": round(row_data["End Time (seconds)"], self.ROUND_DIGITS),
                        "time_event": round(row_data["Start Time (seconds)"], self.ROUND_DIGITS), "details": ""}
            item_new.update(base_obj)

            # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.agg.html
            df_frame_sub = df_frames[(df_frames["Frame Number"] >= int(row_data["Start Frame"])) & \
                                     (df_frames["Frame Number"] < int(row_data["End Frame"]))]
            df_frame_agg = df_frame_sub[["content_val","delta_hue","delta_lum","delta_sat"]].agg(["mean", "min", "max"]).unstack()
            flat_index = ['_'.join(x) for x in list(df_frame_agg.index.to_flat_index())]
            df_frame_agg.index = flat_index

            # include features from frames, perhaps as an average, min, and max?
            item_new["details"] = json.dumps(df_frame_agg.round(self.ROUND_DIGITS).to_dict())
            list_items.append(item_new)

        if len(list_items) > 0:   # return the whole thing as dataframe
            return DataFrame(list_items)

        if run_options["verbose"]:
            self.logger.critical(f"No valid events detected for '{self.EXTRACTOR}'")
        return None

