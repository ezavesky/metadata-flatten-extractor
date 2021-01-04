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
    dict_result = flatten(args=["--path_result", str(path_temp)])
    assert "data" not in dict_result
    dict_result = flatten(args=["--path_content", str(PATH_TEST.joinpath("test.mp4").resolve())])
    assert "data" not in dict_result

    # test straight result parse
    dict_result = flatten(args=["--path_content", str(PATH_TEST.joinpath("test.mp4").resolve()), 
                                "--path_result", str(path_temp)])
    assert "data" in dict_result
    num_results_long = len(dict_result['data'])
    assert num_results_long > 1
    shutil.rmtree(str(path_temp))   # cleanup

    # test directory input instead of file
    dict_result = flatten(args=["--path_content", str(PATH_TEST.resolve()), 
                                "--path_result", str(path_temp)])
    assert "data" in dict_result and num_results_long == len(dict_result['data'])
    shutil.rmtree(str(path_temp))   # cleanup


def test_generator():
    path_temp = Path(tempfile.mkdtemp()).resolve()

    # test only single input (extractor)
    dict_result = flatten(args=["--path_content", str(PATH_TEST.resolve()), "--extractor", "azure_videoindexer",
                                "--verbose", "--path_result", str(path_temp)])
    assert "generated" in dict_result
    assert 2 == len(dict_result['generated'])
    shutil.rmtree(str(path_temp))   # cleanup

    # with no output (v1.3.0+)
    dict_result = flatten({"path_content": str(PATH_TEST.resolve()), "extractor": "azure_videoindexer",
                           "generator": "", "verbose": True, "path_result": str(path_temp)}, args=[])
    assert "generated" not in dict_result
    assert not [x for x in path_temp.rglob("*") if not x.is_dir()]

    # with no output (v1.3.0+)
    dict_result = flatten(args=["--path_content", str(PATH_TEST.resolve()), "--extractor", "azure_videoindexer",
                                "--generator", None, "--verbose", "--path_result", str(path_temp)])
    assert "generated" not in dict_result
    assert not [x for x in path_temp.rglob("*") if not x.is_dir()]

    # with no output (v1.3.0+)
    dict_result = flatten(args=["--path_content", str(PATH_TEST.resolve()), "--extractor", "azure_videoindexer",
                                "--generator", " ", "--verbose", "--path_result", str(path_temp)])
    assert "generated" not in dict_result
    assert not [x for x in path_temp.rglob("*") if not x.is_dir()]

    # test only single input and output (extractor + generator)
    dict_result = flatten(args=["--path_content", str(PATH_TEST.resolve()), "--extractor", "azure_videoindexer",
                                "--generator", "flattened_csv", "--path_result", str(path_temp)])
    assert "generated" in dict_result and 1 == len(dict_result['generated'])
    assert len([x for x in path_temp.rglob("*") if not x.is_dir()]) == 1
    df_single = pd.read_csv(dict_result['generated'][0]['path']).sort_values(["time_begin", "tag"])
    shutil.rmtree(str(path_temp))   # cleanup
    assert len(df_single) > 0

    # test time offset
    time_offset = 5
    dict_result = flatten(args=["--path_content", str(PATH_TEST.resolve()), "--extractor", "azure_videoindexer",
                                "--time_offset", str(time_offset),
                                "--generator", "flattened_csv", "--path_result", str(path_temp)])
    assert "generated" in dict_result and 1 == len(dict_result['generated'])
    df_offset = pd.read_csv(dict_result['generated'][0]['path']).sort_values(["time_begin", "tag"])
    shutil.rmtree(str(path_temp))   # cleanup
    assert len(df_offset) > 0

    # make sure same data
    assert len(df_offset) == len(df_single)
    for idx in range(len(df_offset)):
        assert abs((df_offset.iloc[0]["time_begin"] - df_single.iloc[0]["time_begin"]) - time_offset) < 0.1

    # test non-compressed version
    dict_uncompressed = flatten(args=["--path_content", str(PATH_TEST.resolve()), "--extractor", "azure_videoindexer",
                                      "--no_compression", "--generator", "flattened_csv", "--path_result", str(path_temp)])
    assert "generated" in dict_uncompressed and 1 == len(dict_uncompressed['generated'])
    assert dict_result['generated'][0]['path'].endswith(".gz")
    assert not dict_uncompressed['generated'][0]['path'].endswith(".gz")
    shutil.rmtree(str(path_temp))   # cleanup


def test_cli():
    import os

    path_temp = Path(tempfile.mkdtemp())

    os.system(f"contentai-metadata-flatten --path_result {path_temp} --path_content {str(PATH_TEST.joinpath('test.mp4').resolve())} ")
    list_result_cli = [x for x in path_temp.rglob("*") if not x.is_dir()]
    print(list_result_cli)
    assert len(list_result_cli) > 0

    # test straight result parse
    dict_result = flatten({"path_content": str(PATH_TEST.joinpath("test.mp4").resolve()), 
                           "path_result": str(path_temp)}, args=[])
    assert "generated" in dict_result and len(dict_result['generated'])
    print(dict_result['generated'])
    assert len(list_result_cli) == len(dict_result['generated'])
    shutil.rmtree(path_temp)   # cleanup


# # test all frames