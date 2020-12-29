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
import pandas as pd

from contentai_metadata_flatten.generators import Generate

class Generator(Generate):
    def __init__(self, path_destination, logger=None):
        super().__init__(path_destination, "csv", ".csv", logger=logger)

    @staticmethod
    def known_types():
        """Return the output types for this generator
        :return: list.  List of output types (file types) for this generator
        """
        return ["csv"]

    def generate(self, path_output, run_options, df):
        """Generate CSV from flattened results

        :param: path_output (str): path for output of the file 
        :param: run_options (dict): specific runtime information 
        :param: df (DataFrame): dataframe of events to break down
        :returns: (int): count of items on successful decoding and export, zero otherwise
        """

        df_prior = None
        if path.exists(path_output):
            df_prior = pd.read_csv(path_output)
            self.logger.info(f"Loaded {len(df_prior)} existing events from {path_output}...")
            df = pd.concat([df, df_prior])
            num_prior = len(df)
            df.drop_duplicates(inplace=True)
            self.logger.info(f"Duplicates removal shrunk from {num_prior} to {len(df)} surviving events...")

        df.sort_values("time_begin").to_csv(path_output, index=False)
        return len(df)
