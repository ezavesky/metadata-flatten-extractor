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

import sys
from os import path, makedirs
import importlib

import pandas as pd

import contentai
import parsers
import generators

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

    list_parser_modules = parsers.modules
    if 'extractor' in contentai.metadata:  # add ability to specify sepcific extractor
        list_parser_modules = [f"flatten_{contentai.metadata['extractor']}"]

    list_generator_modules = generators.modules
    if 'generator' in contentai.metadata:  # add ability to specify sepcific extractor
        list_generator_modules = [f"generate_{contentai.metadata['generator']}"]

    # allow injection of parameters from environment
    input_vars = {"force_overwrite": True, "verbose":False,
                    "compressed": True, 'all_frames': False, 'time_offset':0}
    if contentai.metadata is not None:  # see README.md for more info
        input_vars.update(contentai.metadata)

    need_generation = False
    map_outputs = {}
    for extractor_name in list_parser_modules:  # iterate through auto-discovered packages
        for generator_name in list_generator_modules:  # iterate through auto-discovered packages
            generator_module = importlib.import_module(f"generators.{generator_name}")  # load module
            generator_obj = getattr(generator_module, "Generator")   # get class template
            generator_instance = generator_obj(contentai.result_path)   # create instance
            map_outputs[generator_name] = {'module': generator_instance, 'path': generator_instance.get_output_path(extractor_name)}
            if "compressed" in input_vars and input_vars["compressed"]:  # allow compressed version
                map_outputs[generator_name]["path"] += ".gz"
            need_generation |= (generator_instance.is_universal or not path.exists(map_outputs[generator_name]["path"]))

        df = None
        if not need_generation and not input_vars['force_overwrite']:
            parsers.Flatten.logger.info(f"Skipping re-process of {input_vars['path_result']}...")
        else:
            parser_module = importlib.import_module(f"parsers.{extractor_name}")  # load module
            parser_obj = getattr(parser_module, "Parser")   # get class template
            parser_instance = parser_obj(contentai.content_path)   # create instance
        
            if input_vars["verbose"]:
                parsers.Flatten.logger.info(f"ContentAI argments: {input_vars}")
            df = parser_instance.parse(input_vars)  # attempt to process

            if df is None:  # skip bad results
                if 'extractor' in contentai.metadata:
                    parsers.Flatten.logger.warning(f"Specified extractor `{contentai.metadata['extractor']}` failed to find data. " \
                        f"Verify that input directory {contentai.content_path} points directly to file...")

        if df is not None:
            if input_vars['time_offset'] != 0:  # need offset?
                parsers.Flatten.logger.info(f"Applying time offset of {input_vars['time_offset']} seconds to {len(df)} events ('{input_vars['path_result']}')...")
                for col_name in ['time_begin', 'time_end', 'time_event']:
                    df[col_name] += input_vars ['time_offset']

            for generator_name in map_outputs:  # iterate through auto-discovered packages
                num_items = map_outputs[generator_name]['module'].generate(map_outputs[generator_name]["path"], input_vars, df)  # attempt to process
                parsers.Flatten.logger.info(f"Wrote {num_items} items as '{generator_name}' to result file '{map_outputs[generator_name]['path']}'")
        pass

if __name__ == "__main__":
    main()
