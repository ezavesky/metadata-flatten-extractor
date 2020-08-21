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
import argparse
from pathlib import Path
import logging

import pandas as pd
import contentaiextractor as contentai

if __name__ == '__main__':
    # patch the path to include this object
    pathRoot = str(Path(__file__).resolve().parent.parent)
    if pathRoot not in sys.path:
        sys.path.append(pathRoot)

from contentai_metadata_flatten import parsers, generators


def flatten(input_params=None, args=None, logger=None):
    # from contentai_metadata_flatten import parsers
    if logger is None:
        logger = logging.getLogger()
    logging.basicConfig(level=logging.WARNING)

    parser = argparse.ArgumentParser(
        description="""A script to perform metadata parsing""",
        epilog="""
        Launch to parse a set of downloaded and flattened assets... 
            python main.py --path_content=path/to/dir --path_result results
    """, formatter_class=argparse.RawTextHelpFormatter)
    submain = parser.add_argument_group('main execution and evaluation functionality')
    submain.add_argument('--path_content', dest='path_content', type=str, default=contentai.content_path, 
                            help='input video path for files to label')
    submain.add_argument('--path_result', dest='path_result', type=str, default=contentai.result_path, 
                            help='output path for samples')
    submain.add_argument('--verbose', dest='verbose', default=False, action='store_true', 
                            help='verbosely print operations')
    submain = parser.add_argument_group('input and parsing options')
    submain.add_argument('--extractor', dest='extractor', type=str, default="", 
                            help='specify one extractor to flatten, skipping nested module import (*default=all*, e.g. ``dsai_metadata``)')
    submain.add_argument('--time_offset', dest='time_offset', type=int, default=0, 
                            help='when merging events for an asset split into multiple parts, time in seconds (*default=0*); negative numbers will cause a truncation (skip) of events happening before the zero time mark *(added v0.7.1)*')
    submain.add_argument('--all_frames', dest='all_frames', default=False, action='store_true', 
                            help='for video-based events, log all instances in box or just the center')
    submain = parser.add_argument_group('output modulation')
    submain.add_argument('--generator', dest='generator', type=str, default="", 
                            help='specify one generator for output, skipping nested module import (*default=all*)')
    submain.add_argument('--no_compression', dest='compressed', default=True, action='store_false', 
                            help="compress output CSVs instead of raw write (*default=True*, e.g. append ‘.gz’)")
    submain.add_argument('--force_overwrite', dest='force_overwrite', default=False, action='store_true', 
                            help="compforce existing files to be overwritten (*default=False*)")

    if args is not None:
        config = vars(parser.parse_args(args))
    else:
        config = vars(parser.parse_args())
    if input_params is not None:
        config.update(input_params)

    # allow injection of parameters from environment
    contentai_metadata = contentai.metadata()
    if contentai_metadata is not None:  # see README.md for more info
        config.update(contentai_metadata)
    logger.info(f"Run arguments: {config}")
    if not config['path_content'] or not config['path_result']:
        logger.critical(f"Missing content path ({config['path_content']}) or result path ({config['path_result']})")
        parser.print_help(sys.stderr)
        return []

    path_result = Path(config['path_result'])
    if not path_result.exists():
        path_result.mkdir(parents=True)

    list_parser_modules = parsers.get_by_name(config['extractor'] if len(config['extractor']) else None)
    list_generator_modules = generators.get_by_name(config['generator'] if len(config['generator']) else None)
    path_source = Path(config['path_content'])
    if not path_source.is_dir():
        path_source = path_source.parent
    path_source = str(path_source.resolve())

    need_generation = False
    map_outputs = {}
    set_results = set()
    for parser_obj in list_parser_modules:  # iterate through auto-discovered packages
        for generator_obj in list_generator_modules:  # iterate through auto-discovered packages
            generator_instance = generator_obj['obj'](str(path_result), logger=logger)   # create instance
            generator_name = generator_obj['name']
            map_outputs[generator_name] = {'module': generator_instance, 'path': generator_instance.get_output_path(parser_obj['name'])}
            if "compressed" in config and config["compressed"]:  # allow compressed version
                map_outputs[generator_name]["path"] += ".gz"
            need_generation |= (generator_instance.is_universal or not Path(map_outputs[generator_name]["path"]).exists())

        df = None
        if not need_generation and not config['force_overwrite']:
            logger.info(f"Skipping re-process of {config['path_result']}...")
        else:
            parser_instance = parser_obj['obj'](path_source, logger=logger)   # create instance
        
            if config["verbose"]:
                logger.info(f"ContentAI arguments: {config}")
            df = parser_instance.parse(config)  # attempt to process

            if df is None:  # skip bad results
                if len(config['extractor']):
                    logger.warning(f"Specified extractor `{config['extractor']}` failed to find data. " \
                        f"Verify that input directory {path_source} points directly to file...")

        if df is not None:
            if config['time_offset'] != 0:  # need offset?
                logger.info(f"Applying time offset of {config['time_offset']} seconds to {len(df)} events ('{parser_obj['name']}')...")
                for col_name in ['time_begin', 'time_end', 'time_event']:
                    df[col_name] += config['time_offset']
            df.drop(df[df["time_begin"] < 0].index, inplace=True)  # drop rows if trimmed from front

            for generator_name in map_outputs:  # iterate through auto-discovered packages
                if map_outputs[generator_name]['module'].is_universal or not Path(map_outputs[generator_name]["path"]).exists():
                    num_items = map_outputs[generator_name]['module'].generate(map_outputs[generator_name]["path"], config, df)  # attempt to process
                    logger.info(f"Wrote {num_items} items as '{generator_name}' to result file '{map_outputs[generator_name]['path']}'")
                else:
                    logger.info(f"Skipping re-generate of {generator_name} to file '{map_outputs[generator_name]['path']}''...")
                set_results.add(map_outputs[generator_name]["path"])

    # resolve and return fully qualified path
    return [str(Path(config['path_result']).joinpath(k).resolve()) for k in set_results]

def main():
    """Helper wrapper for CLI return status"""
    flatten()

if __name__ == "__main__":
    flatten()
