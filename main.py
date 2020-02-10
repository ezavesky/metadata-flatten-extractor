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

import sys
from os import path, makedirs
import importlib

import pandas as pd

import contentai
import parsers

def main():
    # check for a single argument as input for the path as an override
    if len(sys.argv) > 1:
        parsers.Flatten.logger.info(f"Detected command line input: {sys.argv}")
        contentai.content_path = sys.argv[-1]
        if not path.exists(contentai.content_path):
            parsers.Flatten.logger.fatal(f"Content path '{contentai.content_path}' does not exist, aborting.")
            return -1
        contentai.result_path = path.dirname(contentai.content_path)
    
    # extract data from contentai.content_url
    # or if needed locally use contentai.content_path
    # after calling contentai.download_content()
    parsers.Flatten.logger.info("Skipping raw content download from ContentAI")
    # contentai.download_content()   # critical, do not download content, we post-process!

    if not path.exists(contentai.result_path):
        makedirs(contentai.result_path)

    for extractor_name in parsers.modules:  # iterate through auto-discovered packages
        # call process with i/o specified
        path_output = path.join(contentai.result_path, "flatten_" + extractor_name + ".csv")

        # allow injection of parameters from environment
        input_vars = {'path_result': path_output, "force_overwrite": True, 
                        "compressed": True, 'all_frames': False, 'time_offset':0}
        if contentai.metadata is not None:  # see README.md for more info
            input_vars.update(contentai.metadata)

        if "compressed" in input_vars and input_vars["compressed"]:  # allow compressed version
            input_vars["path_result"] += ".gz"

        df = None
        if path.exists(input_vars['path_result']) and input_vars['force_overwrite']:
            parsers.Flatten.logger.info(f"Skipping re-process of {input_vars['path_result']}...")
        else:
            parser_module = importlib.import_module(f"parsers.{extractor_name}")  # load module
            parser_obj = getattr(parser_module, "Parser")   # get class template
            parser_instance = parser_obj(contentai.content_path)   # create instance
        
            parsers.Flatten.logger.info(f"ContentAI argments: {input_vars}")
            df = parser_instance.parse(input_vars)  # attempt to process

        if df is not None:  # skip bad results
            if input_vars['time_offset'] != 0:  # need offset?
                parsers.Flatten.logger.info(f"Applying time offset of {input_vars['time_offset']} seconds to {len(df)} events...")
                for col_name in ['time_begin', 'time_end', 'time_event']:
                    df[col_name] += input_vars ['time_offset']

            df_prior = None
            if path.exists(input_vars['path_result']):
                df_prior = pd.read_csv(input_vars['path_result'])
                parsers.Flatten.logger.info(f"Loaded {len(df_prior)} existing events from {input_vars['path_result']}...")
                df = pd.concat([df, df_prior])
                num_prior = len(df)
                df.drop_duplicates(inplace=True)
                parsers.Flatten.logger.info(f"Duplicates removal shrunk from {num_prior} to {len(df)} surviving events...")

            df.sort_values("time_begin").to_csv(input_vars['path_result'], index=False)
            parsers.Flatten.logger.info(f"Wrote {len(df)} items to result file '{input_vars['path_result']}'")


if __name__ == "__main__":
    main()
