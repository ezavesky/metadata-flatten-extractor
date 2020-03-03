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

from . import Generate

class Generator(Generate):
    def __init__(self, path_destination):
        super().__init__(path_destination, "wbTimeTaggedMetadata", ".json", universal=True)
        self.template_path = path.join(self.PATH_DATA, 'templates', "wbTimeTaggedMetadata.json")

    def distill_type(self, output_obj, timed_row):
        """Render input type to the right JSON/object format...

        :param: output_obj (dict): input/target dict object to update with type and data
        :param: timed_row (pd.Series): a single row for output with some expected column names
        :returns: (bool): time coverage indicator True (concrete and lasting through whole event),  False (descriptive, not necessarily entire timespan), None (unknown type)
        """

        if timed_row["tag_type"] == "tag":    # generic audio, visual, or textual tag
            output_obj["dataTypeId"] = "timedEvent"
            output_obj["dataObject"] = {"name": timed_row["tag"], "source": timed_row["source_event"], 
                                        "score": timed_row["score"], "extractor": timed_row["extractor"] }
            return False
        return None

    def append_timed(self, output_set, timed_row):
        """Append timed row as either timespan or frame to output object...

        :param: output_set (dict): sets of timed objects ['descriptiveTimespans', 'concreteTimespans', 'frames']
        :param: timed_row (pd.Series): a single row for output with some expected column names
        :returns: (dict): modified output object
        """
        if timed_row["time_begin"] == timed_row["time_end"]:   # detect this is a frame input
            new_frame = {"wbtcd:frameLocation":  { "valueFSTC": float(timed_row["time_event"]), "timeUnits": "seconds", "frameAccuracy": 0.001 },
                            "wbtcd:frameData": {  } }
            new_frame["wbtcd:frameLocation"]["valueFSTC"] = float(timed_row["time_event"])
            if self.distill_type(new_frame["wbtcd:frameData"], timed_row) is not None:
                # TODO: hash/check for object collision?
                output_set["frames"].append(new_frame)

        else:   # detect that this is a timespan
            new_span = { "start": float(timed_row["time_begin"]), "end": float(timed_row["time_end"]), "units": "seconds", "accuracy": 0.001 }
            type_update = self.distill_type(new_span, timed_row)
            if type_update is not None:
                # TODO: hash/check for object collision?
                type_update = output_set["concreteTimespans" if type_update else "descriptiveTimespans"].append(new_span)

        return output_set

    def generate(self, path_output, run_options, df):
        """Generate wbTimeTaggedMetadata from flattened results

        :param: path_output (str): path for output of the file 
        :param: run_options (dict): specific runtime information 
        :param: df (DataFrame): dataframe of events to break down
        :returns: (int): count of items on successful decoding and export, zero otherwise
        """
        num_items = 0
        if not path.exists(self.template_path) and not path.exists(self.template_path):   # if no template and not appending...
            self.logger.critical(f"Template generator file `{self.template_path}` not found, processing aborted.")
            return num_items   # return empty dataframe

        obj_out = None
        if path.exists(path_output):    # load a prior output
            obj_out = self.json_load(path_output)
            # TODO: work out logic for loading/appending prior output
            # TODO: integrate this logic as a parser class as well

            # generators.Generate.logger.info(f"Loaded {len(df_prior)} existing events from {self.path_destination}...")
            # df = pd.concat([df, df_prior])
            # num_prior = len(df)
            # df.drop_duplicates(inplace=True)
            # generators.Generate.logger.info(f"Duplicates removal shrunk from {num_prior} to {len(df)} surviving events...")
        
        else:   # use template to generate a new output
            obj_out = self.json_load(self.template_path)

        output_set = {'descriptiveTimespans':[], 'concreteTimespans':[], 'frames':[]}

        # insert empty frames and timespans for processing
        if "wbtcd:frames" in obj_out:
            output_set["frames"] = obj_out["wbtcd:frames"]
        if "wbtcd:timespans" in obj_out:
            if "descriptiveTimespans" in obj_out["wbtcd:timespans"]:
                output_set["descriptiveTimespans"] = obj_out["wbtcd:timespans"]["descriptiveTimespans"]
            if "concreteTimespans" in obj_out["wbtcd:timespans"]:
                output_set["concreteTimespans"] = obj_out["wbtcd:timespans"]["concreteTimespans"]

        # TODO: generate extra trappings and specific object
        for idx_r, val_r in df.iterrows():   # walk through all rows to generate
            self.append_timed(output_set, val_r)

        # clean up any empty entries for schema compliance
        if len(output_set["frames"]):
            obj_out["wbtcd:frames"] = output_set["frames"]
            num_items += len(output_set["frames"])
        if len(output_set["descriptiveTimespans"]):
            if "wbtcd:timespans" not in obj_out:
                obj_out["wbtcd:timespans"] = {}
            obj_out["wbtcd:timespans"]["descriptiveTimespans"] = output_set["descriptiveTimespans"]
            num_items += len(output_set["descriptiveTimespans"])
        if len(output_set["concreteTimespans"]):
            if "wbtcd:timespans" not in obj_out:
                obj_out["wbtcd:timespans"] = {}
            obj_out["wbtcd:timespans"]["concreteTimespans"] = output_set["concreteTimespans"]
            num_items += len(output_set["concreteTimespans"])

        self.json_save(path_output, obj_out)      # write out json object
        return num_items