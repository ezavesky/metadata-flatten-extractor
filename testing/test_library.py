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

from contentai_metadata_flatten import parsers

def test_subdirs():
    path_asset_str = str(PATH_TEST.resolve())

    logger = logging.getLogger()
    logging.basicConfig(level=logging.WARNING)

    for path_sub in PATH_TEST.rglob("*"):
        if path_sub.is_dir():
            list_parser = parsers.get_by_name(path_sub.name)
            if len(list_parser) < 1:   # at least one member
                logger.warning(f"WARNING, missing extractor '{path_sub.name}', from {str(path_sub)}")
                continue
            # assert len(list_parser) > 1   # at least one member
            logger.warning(f"Processing extractor '{path_sub.name}', from {str(path_sub)}")

            parser_instance = list_parser[0]['obj'](path_asset_str, logger=logger)
            config_default = parser_instance.default_config()
            input_df = parser_instance.parse(config_default)
            assert len(input_df) > 0

    # config['extractor'] = content_extractor_name
    # list_parser_modules = parsers.get_by_name(config['extractor'])
    # for parser_obj in list_parser_modules:  # iterate through auto-discovered packages
    #     parser_instance = parser_obj['obj'](rootPath, logger=logger)
    #     input_df = parser_instance.parse(config)


    # # test bad input or output
    # list_result = flatten(args=["--path_result", str(path_temp)])
    # assert len(list_result) == 0
    # list_result = flatten(args=["--path_content", str(PATH_TEST_ALT.joinpath("test.mp4").resolve())])
    # assert len(list_result) == 0

