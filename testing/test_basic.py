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
from os import path

PATH_TEST = path.join(path.dirname(path.dirname(path.abspath(__file__))), 'data')


def test_version():
    from contentai_metadata_flatten import _version
    assert _version.__package__ == "contentai_metadata_flatten"


def test_main():
    from contentai_metadata_flatten import main


def test_discovery():
    from contentai_metadata_flatten import generators, parsers

    list_gen = generators.get_by_type('csv')
    assert len(list_gen) > 0  # at least one member

    list_gen = generators.get_by_type(['csv', 'json'])
    assert len(list_gen) > 1  # at least one member

    list_gen = generators.get_by_name('csv')
    assert len(list_gen) > 0  # at least one member

    list_parser = parsers.get_by_type('moderation')
    assert len(list_parser) > 0  # at least one member

    list_parser = parsers.get_by_type(['shot', 'scene'])
    assert len(list_parser) > 1  # at least one member

    list_parser = parsers.get_by_name('aws')
    assert len(list_parser) > 1   # at least one member


def test_packages():
    from contentai_metadata_flatten import generators, parsers
    
    list_gen = generators.get_by_name('TimeTaggedMetadata')
    assert len(list_gen) > 0  # at least one member

    instance_gen = list_gen[0]['obj']('junk')
    assert path.exists(instance_gen.schema_path)   # need to have the template/schema path





# validate against input and basic parsing?
# drop rows if negative index in time
# drop/merge repeat rows
# validate inclusion or replacement of overwrite
# validate consistentcy between CSV and other generators
