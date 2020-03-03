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
import json
import re
from pandas import DataFrame

from . import Generate

class Generator(Generate):
    def __init__(self, path_destination):
        super().__init__(path_destination)
        self.GENERATOR = "flattened_csv"

    def generate(self, run_options, df):
        """Generate CSV from flattened results

        :param: run_options (dict): specific runtime information 
        :returns: (DataFrame): DataFrame on successful decoding and export, None (or exception) otherwise
        """

        df_prior = None
        if path.exists(self.path_destination):
            df_prior = pd.read_csv(self.path_destination)
            generators.Generate.logger.info(f"Loaded {len(df_prior)} existing events from {self.path_destination}...")
            df = pd.concat([df, df_prior])
            num_prior = len(df)
            df.drop_duplicates(inplace=True)
            generators.Generate.logger.info(f"Duplicates removal shrunk from {num_prior} to {len(df)} surviving events...")

        df.sort_values("time_begin").to_csv(self.path_destination, index=False)
        return df