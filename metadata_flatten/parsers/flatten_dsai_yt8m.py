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
import json

from pytimeparse import parse as pt_parse

# NOTE: we reuse the parser (also CSV source) for this type as well
from metadata_flatten.parsers.flatten_dsai_activity_slowfast import Parser as ParserBase

class Parser(ParserBase):
    def __init__(self, path_content):
        super().__init__(path_content)
        self.EXTRACTOR = "dsai_yt8m"

    def get_source_types(self, column_clean):
        # (yt8m)
        # file,Time_begin,Time_end,Time_event,label_id0,label0,probability0,label_id1,label1,probability1,label_id2,label2,probability2,label_id3,label3,probability3,label_id4,label4,probability4
        # output000001.png,2,2,2,231,motel,0.158193097,122,discotheque,0.070194781,158,gas_station,0.062356248,129,elevator/door,0.059626624,177,home_theater,0.055273291

        if "file" in column_clean:  # suspect it's scene images
            return {'type': "image", 'column_prefix':['label', 'probability']}
        return None
