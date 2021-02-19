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
"""
basic tests
"""

import tempfile
import shutil
import pytest
import pandas as pd
from pathlib import Path
import logging

PATH_TEST = Path(__file__).parent.joinpath('data', 'results-hbomax', 'job-default')
PATH_TEST_ALT = Path(__file__).parent.joinpath('data', 'results-friends', 'job-default')
PATH_TEST_TROUBLE = Path(__file__).parent.joinpath('data', 'results-trouble', 'job-default')

PATH_TEST_SPLIT = Path(__file__).parent.joinpath('data', 'results-split')

from contentai_metadata_flatten import parsers
from contentai_metadata_flatten.main import flatten

def test_subdirs():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    for asset_active in [PATH_TEST_TROUBLE, PATH_TEST, PATH_TEST_ALT]:
        path_asset_str = str(asset_active.resolve())
        for path_sub in asset_active.rglob("*"):
            if path_sub.is_dir():
                list_parser = parsers.get_by_name(path_sub.name)
                if len(list_parser) < 1:   # at least one member
                    logger.warning(f"WARNING, missing extractor '{path_sub.name}', from {str(path_sub)}")
                    continue
                # assert len(list_parser) > 1   # at least one member
                logger.info(f"Processing extractor '{path_sub.name}', from {str(path_sub)}")
                parser_instance = list_parser[0]['obj'](path_asset_str, logger=logger)
                config_default = parser_instance.default_config()
                input_df = parser_instance.parse(config_default)
                assert len(input_df) > 0


def test_split_timing():
    # goal is to check the assembly of multiple parts using a 'timing' file

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    path_temp = Path(tempfile.mkdtemp())

    for asset_dir in PATH_TEST_SPLIT.rglob("part*"):
        if asset_dir.is_dir():
            # test straight result parse
            dict_result = flatten({"path_content": str(asset_dir.joinpath("test.mp4").resolve()), 
                                "path_result": str(path_temp), 
                                "time_offset_source":str(asset_dir.joinpath("timing.txt"))}, args=[])

    for path_gen_csv in path_temp.rglob("*comskip*csv*"):
        df = pd.read_csv(str(path_gen_csv))
        # want to confirm that events exist in times from at least 0-59m, 60-119m, 120m+
        assert len(df[df["time_begin"] < 3600])
        assert len(df[(df["time_begin"] > 3600) & (df["time_begin"] < 7200)])
        assert len(df[df["time_begin"] > 7200])

    shutil.rmtree(path_temp)   # cleanup
