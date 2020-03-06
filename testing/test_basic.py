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

from metadata_flatten import _version

PATH_TEST = path.join(path.dirname(path.dirname(path.abspath(__file__))), 'data')

def test_version():
    assert _version.__package__ == "metadata_flatten"

# validate against input and basic parsing?
# drop rows if negative index in time
# drop/merge repeat rows
# validate inclusion or replacement of overwrite
# validate consistentcy between CSV and other generators
