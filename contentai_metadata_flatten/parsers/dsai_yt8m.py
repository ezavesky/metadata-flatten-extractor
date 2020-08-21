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
from contentai_metadata_flatten.parsers.dsai_activity_slowfast import Parser as ParserBase

class Parser(ParserBase):
    def __init__(self, path_content, logger=None):
        super().__init__(path_content, logger=logger)
        self.EXTRACTOR = "dsai_yt8m"

    def get_source_types(self, column_clean):
        # (yt8m)
        # video_clip,Time_begin,Time_end,Time_event,category0,score0,category1,score1,category2,score2
        # 0,0.0,10.0,0.0,Animation,0.412782,IPhone,0.283587,Video game,0.12803900000000001

        if "video_clip" in column_clean:  # suspect it's scene images
            return {'type': "video", 'column_prefix':['category', 'score']}
        return None
