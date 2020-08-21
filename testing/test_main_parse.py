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
import json
import pandas as pd
from pathlib import Path

PATH_TEST = Path(__file__).parent.joinpath('data', 'results-hbomax')

from contentai_metadata_flatten.main import flatten


def test_programmatic():
    path_temp = Path(tempfile.mkdtemp()).resolve()

    # test bad input or output
    list_result = flatten(args=["--path_result", str(path_temp)])
    assert len(list_result) == 0
    list_result = flatten(args=["--path_content", str(PATH_TEST.joinpath("test.mp4").resolve())])
    assert len(list_result) == 0

    # test straight result parse
    list_result = flatten(args=["--path_content", str(PATH_TEST.joinpath("test.mp4").resolve()), 
                                 "--path_result", str(path_temp)])
    num_results_long = len(list_result)
    assert num_results_long >= 2
    shutil.rmtree(str(path_temp))   # cleanup

    # test directory input instead of file
    list_result = flatten(args=["--path_content", str(PATH_TEST.resolve()), 
                                 "--path_result", str(path_temp)])
    assert num_results_long == len(list_result)
    shutil.rmtree(str(path_temp))   # cleanup

    # test only single input (extractor)
    list_result = flatten(args=["--path_content", str(PATH_TEST.resolve()), "--extractor", "azure_videoindexer",
                                 "--verbose", "--path_result", str(path_temp)])
    assert 2 == len(list_result)
    shutil.rmtree(str(path_temp))   # cleanup

    # test only single input and output (extractor + generator)
    list_result = flatten(args=["--path_content", str(PATH_TEST.resolve()), "--extractor", "azure_videoindexer",
                                  "--generator", "flattened_csv", "--path_result", str(path_temp)])
    assert 1 == len(list_result)
    df_single = pd.read_csv(list_result[0]).sort_values(["time_begin", "tag"])
    shutil.rmtree(str(path_temp))   # cleanup
    assert len(df_single) > 0

    # test time offset
    time_offset = 5
    list_result = flatten(args=["--path_content", str(PATH_TEST.resolve()), "--extractor", "azure_videoindexer",
                                 "--time_offset", str(time_offset),
                                 "--generator", "flattened_csv", "--path_result", str(path_temp)])
    assert 1 == len(list_result)
    df_offset = pd.read_csv(list_result[0]).sort_values(["time_begin", "tag"])
    shutil.rmtree(str(path_temp))   # cleanup
    assert len(df_offset) > 0

    # make sure same data
    assert len(df_offset) == len(df_single)
    for idx in range(len(df_offset)):
        assert abs((df_offset.iloc[0]["time_begin"] - df_single.iloc[0]["time_begin"]) - time_offset) < 0.1

    # test non-compressed version
    list_uncompressed = flatten(args=["--path_content", str(PATH_TEST.resolve()), "--extractor", "azure_videoindexer",
                                  "--no_compression", "--generator", "flattened_csv", "--path_result", str(path_temp)])
    assert len(list_result) == len(list_uncompressed)
    for idx in range(len(list_result)):
        assert list_result[idx].endswith(".gz")
        assert not list_uncompressed[idx].endswith(".gz")
    shutil.rmtree(str(path_temp))   # cleanup


def test_cli():
    import os

    path_temp = Path(tempfile.mkdtemp())

    os.system(f"contentai-metadata-flatten --path_result {path_temp} --path_content {str(PATH_TEST.joinpath('test.mp4').resolve())} ")
    list_result_cli = [x for x in path_temp.rglob("*") if not x.is_dir()]
    print(list_result_cli)
    assert len(list_result_cli) > 0

    # test straight result parse
    list_result = flatten(args=["--path_content", str(PATH_TEST.joinpath("test.mp4").resolve()), 
                                 "--path_result", str(path_temp)])
    assert len(list_result_cli) == len(list_result) 
    shutil.rmtree(path_temp)   # cleanup


# # test all frames