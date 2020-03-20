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

from metadata_flatten.parsers import Flatten

class Parser(Flatten):
    def __init__(self, path_content):
        super().__init__(path_content)
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
        if not dict_data:  # do we need to load it locally?
            if 'extractor' in run_options:
                path_content = path.join(self.path_content, file_name)
            else:
                path_content = path.join(self.path_content, self.EXTRACTOR, file_name)
            if not path.exists(path_content):
                path_content += ".gz"
            dict_data = self.text_load(path_content)
        if len(dict_data) < 1:
            if run_options["verbose"]:
                self.logger.critical(f"Empty result string for extractor '{self.EXTRACTOR}', aborting")
            return None
        return read_csv(dict_data, skiprows=1)   # both input files have an extra header line

    def parse(self, run_options):
        """Flatten SlowFast actions results

        :param: run_options (dict): specific runtime information
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """
        df_shots = self.retrieve_output("stats.csv", run_options)
        df_frames = self.retrieve_output("scenes.csv", run_options)
        if df_frames is None or df_scenes is None:
            if run_options["verbose"]:
                self.logger.critical(f"Empty shot or scenen file for extractor '{self.EXTRACTOR}', aborting")
            return None

        # (scene format)
        # Scene Number,Start Frame,Start Timecode,Start Time (seconds),End Frame,End Timecode,End Time (seconds),Length (frames),Length (timecode),Length (seconds)
        # 1,0,00:00:00.000,0.000,169,00:00:05.639,5.639,169,00:00:05.639,5.639

        # (frame format)
        # Frame Number,Timecode,content_val,delta_hue,delta_lum,delta_sat
        # 1,00:00:00.033,0.0,0.0,0.0,0.0

        # TODO: include features from frames, perhaps as an average, min, and max?

        base_obj = {"source_event": "video", "tag_type": "shot", "extractor": self.EXTRACTOR, "score": 1.0}

        list_items = []
        for row_idx, row_data in df_raw.iterrows():
            item_new = {"time_begin": row_data["Start Time (seconds)"], "time_end": row_data["End Time (seconds)"], 
                        "time_event": row_data["Start Time (seconds)"], "details": ""}
            item_new.update(base_obj)
            list_items.append(item_new)

        if len(list_items) > 0:   # return the whole thing as dataframe
            return DataFrame(list_items)

        if run_options["verbose"]:
            self.logger.critical(f"No valid events detected for '{self.EXTRACTOR}'")
        return None


### --- this code will likely be dropped (below) --------

# # method to read/extract the segments
# def extract_segments(path_video, path_output):
#     dir_video = path_video
#     if path.isfile(path_video):  # get directory
#         dir_video = path.dirname(path_video)
#     elif path.isdir(path_video):   # look for video
#         if path.exists(path.join(path_video, "videohd.mp4")):
#             path_video = path.join(path_video, "videohd.mp4")
#         elif path.exists(path.join(path_video, "video.mp4")):
#             path_video = path.join(path_video, "video.mp4")
#     if path_video == dir_video:   # error!?
#         logger.critical(f"Failed to discover video directory ('{dir_video}'?) and file ('{path_video}'?), aborting")
#         return None

#     if path.isfile(path_output):  # correct output to be a directory
#         path_output = path.dirname(path_output)

#     path_shots = path.join(path_output, "shots.csv")
#     if path.exists(path_shots):
#         logger.info(f"Skipping recompute of shots file '{path_shots}'")
#         return read_csv(path_shots)

#     else:   # fall back to pyscene detect
#         PYSCENEDETECT_SCALE = 5   #  --downscale {PYSCENEDETECT_SCALE} 
#         PYSCENEDETECT_FRAME_SKIP = 1   # --frame-skip {PYSCENEDETECT_FRAME_SKIP} 
#         PYSCENEDETECT_RAW_SCENE = "shots_raw.csv"
#         PYSCENEDETECT_TEMPLATE = "shot-$SCENE_NUMBER-$IMAGE_NUMBER"  # --filename {PYSCENEDETECT_TEMPLATE}
#         # https://pyscenedetect.readthedocs.io/en/latest/examples/usage/
#         str_execute = f"scenedetect --input {path_video} --output {path_output}  "\
#             f"--stats stats.csv detect-content list-scenes -f {PYSCENEDETECT_RAW_SCENE} "\
#             f"save-images --num-images 1 "
#         logger.info(f"Attempting to run pyscene detect ({str_execute})")

#         path_raw_scene = path.join(path_output, PYSCENEDETECT_RAW_SCENE)
#         if not path.exists(path_raw_scene):  # attempt generation
#             system(str_execute)
#         else:
#             logger.info(f"Skipping regeneration of '{path_raw_scene}' with pyscenedetect...")

#         if not path.exists(path_raw_scene):
#             logger.critical(f"Failed to find '{path_raw_scene}' after scene extraction with pyscenedetect, aborting. (is it installed? `pip install pyscenedetect`)")
#             return None 
        
#         # processing to find the right scene information (frame time, frame name, start, stop, duration)
#         logger.info(f"Parsing output '{path_raw_scene}' ...")
#         df_shots = read_csv(path_raw_scene, skiprows=0, header=1, engine='python')
#         df_shots.rename(inplace=True, columns={"Scene Number": "shot", "Start Frame": "frame_begin", "End Frame": "frame_end",
#                         "Start Time (seconds)": "time_begin", "End Time (seconds)": "time_end", "Length (frames)": "frame_count",
#                         "Length (seconds)": "time_duration"})
#         df_shots = df_shots.drop(columns=["Start Timecode", "End Timecode", "Length (timecode)"])
#         for name_col in ["time_end", "time_begin", "time_duration"]:   # scale to millis
#             df_shots[name_col] *= 1000 
#         df_shots["time_frame"] = df_shots["time_begin"] + df_shots["time_duration"] / 2.0
#         df_shots["path"] = path.abspath(path_video)
#         df_shots["frame_path"] = ""
#         df_shots.sort_values(["shot"], inplace=True)  # sort and generate unique id
#         df_shots = segment.tag_unique(df_shots, ["path", "time_begin"], "uni", include_prior=True)

#         # discover frames associated with it (e.g. "video-Scene-002-01.jpg")
#         frame_files = glob.glob(path.join(path_output, "*-Scene-*.jpg"))
#         for name_file in frame_files:
#             frame_parts = path.basename(name_file).split("-")
#             shot_id = int(frame_parts[-2])-1   # it's 1-based, ugh!
#             idx_match = df_shots[df_shots["shot"] == shot_id].index[0]
#             if idx_match:  # valid match? compute new filename
#                 file_ext = path.splitext(name_file)
#                 name_new = path.join(path.dirname(name_file), "".join([df_shots.loc[idx_match, "uni"], file_ext[-1]]))
#                 df_shots.loc[idx_match, "frame_path"] = path.basename(name_new)
#                 if path.exists(name_file):   # wasn't already renamed? do it now
#                     rename(name_file, name_new)

#         # done reading from pyscenedetect

#     logger.info(f"Found {len(df_shots)} shots with total duration {df_shots['time_end'].max()}...")
#     df_shots.to_csv(path_shots, index=False)   # be sure to save hard work!
#     return df_shots

# ### --- this code will likely be dropped (above) --------
