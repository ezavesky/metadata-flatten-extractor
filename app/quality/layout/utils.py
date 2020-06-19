# -*- coding: utf-8 -*-
#! python
# ===============LICENSE_START=======================================================
# scene-me Apache-2.0
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


import datetime as dt
import uuid  # for generating random uuid
import math


import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import plotly.graph_objects as go
from dash.exceptions import PreventUpdate

import logging

logger = logging.getLogger()
formatter = logging.Formatter(fmt='%(asctime)s %(name)-12s %(levelname)-8s %(message)s', datefmt='%m-%d %H:%M')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.handlers = []
logger.addHandler(handler)
logger.propagate = False


### ------------------------------------------------
###  --- Core Layout and Callback funcitonality ----
### ------------------------------------------------


def dt_format(dtn=None, brief=False):
    if dtn is None:
        dtn = dt.datetime.now()
    if brief:
        return dtn.strftime("%H:%M:%S.%f")
    return dtn.strftime("%Y-%m-%d %H:%M:%S")


def dt_recent(dtn=None):
    """
    Get a datetime object or a int() Epoch timestamp and return a
    pretty string like 'an hour ago', 'Yesterday', '3 months ago',
    'just now', etc
    (https://stackoverflow.com/a/1551394)
    """
    now = dt.datetime.now()
    if type(dtn) is int:
        diff = now - dt.datetime.fromtimestamp(dtn)
    elif isinstance(dtn, dt.datetime):
        diff = now - dtn
    else:
        diff = now - now
    second_diff = diff.seconds
    day_diff = diff.days

    if day_diff < 0:
        return ''

    if day_diff == 0:
        if second_diff < 10:
            return "just now"
        if second_diff < 60:
            return str(second_diff) + " seconds ago"
        if second_diff < 120:
            return "a minute ago"
        if second_diff < 3600:
            return str(second_diff / 60) + " minutes ago"
        if second_diff < 7200:
            return "an hour ago"
        if second_diff < 86400:
            return str(second_diff / 3600) + " hours ago"
    if day_diff == 1:
        return "Yesterday"
    if day_diff < 7:
        return str(day_diff) + " days ago"
    if day_diff < 31:
        return str(day_diff / 7) + " weeks ago"
    if day_diff < 365:
        return str(day_diff / 30) + " months ago"
    return str(day_diff / 365) + " years ago"


def media_compute_image(path, image_idx, image_small=True):
    if image_small:
        return "/media/80x45.png"
    return "/media/640x360.png"


def generate_filters(store):
    """generate filters to be used"""
    return []

