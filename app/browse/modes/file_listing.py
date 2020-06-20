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
import streamlit as st
import pandas as pd
from os import path
import re
import math

import altair as alt

from .utilities import *
from .common.preprocessing import *

NUM_SUMMARY = 10

### ------------ main rendering page and sidebar ---------------------

def main_page(data_dir=None, media_file=None, ignore_update=False, symlink=""):
    """Main page for execution"""
    # read in version information
    ux_report = st.empty()
    ux_progress = st.empty()

    list_files, path_new = data_discover(PATH_BASE_BUNDLE, data_dir, bundle_files=True)

    df = data_load(PATH_BASE_BUNDLE, data_dir, True, ignore_update)
    if df is None:
        st.error("No data could be loaded, please check configuration options.")
        return None
    df_live = main_sidebar(df)

    # Create the runtime info
    if len(df_live) < MIN_INSIGHT_COUNT:
        st.markdown("## Too few samples")
        st.markdown("The specified filter criterion are too rigid. Please modify your exploration and try again.")
        return None

    st.markdown("Explore and download source files or data exceprts for events that are analyzed in the browser.")
    df_files = pd.DataFrame([[str(x.name), x.stat().st_size, str(x.resolve()), str(x.stem)] for x in list_files], 
                            columns=["name", "size", "path", "stem"])
    df_files["events"] = len(df)
    df_files["url"] = ""
    
    for idx, row in df_files.iterrows():
        str_url = download_link(symlink, name_link=row["stem"], path_src=row["path"])
        df_files.loc[idx, "url"] = str_url
        df_files.loc[idx, "name"] = f"<a href='{str_url}' target='_blank' title='extractor source data for {row['stem']}'>{row['stem']}</a>"
    list_extractors = df_live["extractor"].unique()

    for name_extractor in list_extractors:
        rows = df_files[df_files["stem"].str.contains(name_extractor)]
        num_events = len(df_live[df_live["extractor"] == name_extractor])
        for idx, row in rows.iterrows():
            df_files.loc[idx, "events"] = num_events
            str_url = download_link(symlink, name_link=name_extractor, path_src=row["path"])
            if df_files.loc[idx, "url"] is not None:
                df_files.loc[idx, "url"] = str_url
                df_files.loc[idx, "name"] = f"<a href='{str_url}' target='_blank' title='extractor source data for {name_extractor}'>{name_extractor}</a>"

    list_html = ["<table style='font-size:smaller'><thead><tr><th>extractor</th><th>file</th><th>events</th><th>size</th></tr></thead><tbody>"]
    for idx, row in df_files.iterrows():
        list_html.append("<tr>")
        for col in ["name", "stem", "events", "size"]:
            list_html.append(f"<td>{row[col]}</td>")
        list_html.append("</tr>")
    list_html.append("</tbody></table>")
    st.markdown("## Extractor data sources with matching events")
    st.write("".join(list_html), unsafe_allow_html=True)

    
    # finally sneak a peak at the raw table
    st.markdown(f"### random sample of {SAMPLE_TABLE} events")
    str_url = download_link(symlink, "CSV Table", df_live)
    if str_url is not None:
        print("Generated CSV Table URL", str_url)
    st.dataframe(df_live.sample(SAMPLE_TABLE))

    return df_live


def main_sidebar(df, sort_list=None):
    # Generate the slider filters based on the data available in this subset of titles
    # Only show the slider if there is more than one value for that slider, otherwise, don't filter

    type_unique = ["All"] + list(df["tag_type"].unique())
    idx_initial = 0
    filter_tag = st.sidebar.selectbox("Event Type", type_unique, index=idx_initial)
    df_sub = df
    if filter_tag != "All":
        df_sub = df[df['tag_type'] == filter_tag]
    idx_match = [True] * len(df_sub)    # start with whole index

    # strict tag source filter
    source_filter = ["All"] + list(df_sub["source_event"].unique())
    source_tag = st.sidebar.selectbox("Event Source", source_filter, index=0)
    if source_tag != "All":
        idx_match &= (df_sub['source_event'] == source_tag)
    df_filter = df_sub[idx_match]

    # hard work done, return the trends!
    if sort_list is None:
        return df_filter
    # otherwise apply sorting right now
    return df_filter.sort_values(by=[v[0] for v in sort_list], 
                                       ascending=[v[1] for v in sort_list])
