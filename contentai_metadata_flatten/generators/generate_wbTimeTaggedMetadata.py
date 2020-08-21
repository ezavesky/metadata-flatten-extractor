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
import hashlib   # for key hashing

from contentai_metadata_flatten.generators import Generate

class Generator(Generate):
    def __init__(self, path_destination, logger=None):
        super().__init__(path_destination, "wbTimeTaggedMetadata", ".json", universal=True, logger=logger)
        self.template_path = path.join(self.PATH_DATA, 'templates', "wbTimeTaggedMetadata.json")
        self.schema_path = path.join(self.PATH_DATA, 'templates', "metadataEvent.schema.json")

    @staticmethod
    def known_types():
        """Return the output types for this generator
        :return: list.  List of output types (file types) for this generator
        """
        return ["json"]

    def distill_type(self, output_obj, timed_row):
        """Render input type to the right JSON/object format...

        :param: output_obj (dict): input/target dict object to update with type and data
        :param: timed_row (pd.Series): a single row for output with some expected column names
        :returns: (bool): time coverage indicator True (concrete and lasting through whole event),  False (descriptive, not necessarily entire timespan), None (unknown type)
        """
        full_coverage = False
        col_row = list(timed_row.index)
        if not ("tag" in col_row and "score" in col_row and "source_event" in col_row):
            return None
        output_obj["dataObject"] = {"name": timed_row["tag"], "source": timed_row["source_event"], "type":timed_row["tag_type"],
                                    "score": timed_row["score"], "extractor": timed_row["extractor"] }
        output_obj["dataTypeId"] = "timedEvent"  # generic audio, visual, or textual tag
        
        if "details" in timed_row:    # check for special object
            details_obj = None
            if len(timed_row["details"]):    # face identity
                details_obj = json.loads(timed_row["details"])
                if 'box' in details_obj:
                    output_obj['box'] = details_obj['box']
                    output_obj["dataTypeId"] = "timedObject"  # object with specific coordinates
                    full_coverage = (timed_row["time_begin"] == timed_row["time_end"])   # only full coverage if singleton event
                if "uri" in details_obj:
                    output_obj["uri"] = details_obj["uri"]
                elif "url" in details_obj:
                    output_obj["uri"] = details_obj["url"]
                elif "urls" in details_obj:
                    output_obj["uri"] = details_obj["urls"]
                if 'transcript' in details_obj:
                    output_obj["transcript"] = details_obj["transcript"]
                    output_obj["dataTypeId"] = "timedText"  # object with specific coordinates
                    full_coverage = True

                # TODO: detail JSON : AgeRange (aws face), Pose (aws face), face (bounding box for person, azure), kfcluster (cae), shot_type (shot tags, azure)

        return full_coverage

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
                output_set["frames"].append(new_frame)

        else:   # detect that this is a timespan
            new_span = { "start": float(timed_row["time_begin"]), "end": float(timed_row["time_end"]), "units": "seconds", "accuracy": 0.001 }
            type_update = self.distill_type(new_span, timed_row)
            if type_update is not None:
                type_update = output_set["concreteTimespans" if type_update else "descriptiveTimespans"].append(new_span)

        return output_set

    def hash_key(self, obj_new, col_check, raw_str=""):
        """Hash input data into a specific hex key

        :param: obj_new (dict): input data 
        :param: col_check (list): list of strings (column names) to combine/bash
        :param: raw_str (str): external data to include in hash
        :returns: (int): count of items on successful decoding and export, zero otherwise
        """
        raw_str += "_".join([str(obj_new[col_name]) for col_name in col_check if col_name in obj_new])  # allow optional columns
        return hashlib.md5(raw_str.encode()).hexdigest()

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
        output_set = {'descriptiveTimespans':[], 'concreteTimespans':[], 'frames':[]}
        if path.exists(path_output):    # load a prior output
            self.logger.info(f"Loading existing JSON {path_output} ...")
            obj_out = self.json_load(path_output)

            if "wbtcd:frames" in obj_out:
                output_set["frames"] = obj_out["wbtcd:frames"]
            if "wbtcd:timespans" in obj_out:
                if "descriptiveTimespans" in obj_out["wbtcd:timespans"]:
                    output_set["descriptiveTimespans"] = obj_out["wbtcd:timespans"]["descriptiveTimespans"]
                if "concreteTimespans" in obj_out["wbtcd:timespans"]:
                    output_set["descriptiveTimespans"] = obj_out["wbtcd:timespans"]["concreteTimespans"]

            # TODO: integrate this logic as a parser class as well
        
        else:   # use template to generate a new output
            obj_out = self.json_load(self.template_path)
            # TODO: consider dynamically repopulating event groupins with items and objects from schema?

        idx_write = 0
        for idx_r, val_r in df.iterrows():   # walk through all rows to generate
            self.append_timed(output_set, val_r)
            if (idx_write % 20000) == 0:
                self.logger.info(f"Processing item {idx_write}/{len(df)} ...")
            idx_write += 1
        num_prior = 0

        column_unique = ["name", "source", "extractor"]   # define some collision columns
        column_unique_data = ["box"]   # use sparingly, but extra hash aginst data (added 0.8.6)

        # clean up any empty entries for schema compliance
        if len(output_set["frames"]):
            self.logger.info(f"Processing {len(output_set['frames'])} 'frame' events...")
            obj_out["wbtcd:frames"] = []
            hash_prior = {}
            num_prior += len(output_set["frames"])   # compute raw count as well
            for obj_new in output_set["frames"]:   # parse each frame, combine both object data and frame time
                hash_key = self.hash_key(obj_new["wbtcd:frameData"]["dataObject"], column_unique, 
                                         str(obj_new["wbtcd:frameLocation"]["valueFSTC"]))
                hash_key = self.hash_key(obj_new["wbtcd:frameData"], column_unique_data, hash_key)
                if hash_key not in hash_prior:
                    hash_prior[hash_key] = 1
                    obj_out["wbtcd:frames"].append(obj_new)
            num_items += len(obj_out["wbtcd:frames"])

        if len(output_set["descriptiveTimespans"]):
            self.logger.info(f"Processing {len(output_set['descriptiveTimespans'])} 'descriptiveTimespans' events...")
            if "wbtcd:timespans" not in obj_out:
                obj_out["wbtcd:timespans"] = {}
            obj_out["wbtcd:timespans"]["descriptiveTimespans"] = []
            hash_prior = {}
            num_prior += len(output_set["descriptiveTimespans"])   # compute raw count as well
            for obj_new in output_set["descriptiveTimespans"]:   # parse each event, all data in event object itself
                hash_key = self.hash_key(obj_new["dataObject"], column_unique, str(obj_new["start"]))
                hash_key = self.hash_key(obj_new, column_unique_data, hash_key)
                if hash_key not in hash_prior:
                    hash_prior[hash_key] = 1
                    obj_out["wbtcd:timespans"]["descriptiveTimespans"].append(obj_new)
            num_items += len(obj_out["wbtcd:timespans"]["descriptiveTimespans"])

        if len(output_set["concreteTimespans"]):
            self.logger.info(f"Processing {len(output_set['concreteTimespans'])} 'concreteTimespans' events...")
            if "wbtcd:timespans" not in obj_out:
                obj_out["wbtcd:timespans"] = {}
            obj_out["wbtcd:timespans"]["concreteTimespans"] = []
            hash_prior = {}
            num_prior += len(output_set["concreteTimespans"])   # compute raw count as well
            for obj_new in output_set["concreteTimespans"]:
                hash_key = self.hash_key(obj_new["dataObject"], column_unique, str(obj_new["start"]))
                hash_key = self.hash_key(obj_new, column_unique_data, hash_key)
                if hash_key not in hash_prior:
                    hash_prior[hash_key] = 1
                    obj_out["wbtcd:timespans"]["concreteTimespans"].append(obj_new)
            num_items += len(obj_out["wbtcd:timespans"]["concreteTimespans"])

        self.logger.info(f"Duplicates removal shrunk from {num_prior} to {num_items} surviving events...")
        self.json_save(path_output, obj_out)      # write out json object
        return num_items
