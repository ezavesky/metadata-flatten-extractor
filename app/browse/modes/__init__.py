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

# Imports
import pkgutil
import importlib

# save module list at this level
modules = {}
for _, _name, _ in pkgutil.iter_modules(__path__):
    page_module = importlib.import_module(f"modes.{_name}")  # load module
    if hasattr(page_module, "main_page"):
        func_main = getattr(page_module, "main_page")   # get class template
        modules[_name] = func_main
