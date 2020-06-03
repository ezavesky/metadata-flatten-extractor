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
import math

import altair as alt

from .utilities import *

# https://stackoverflow.com/a/16710842 - split, but save quotes
REGEX_SEARCH = re.compile(r'(?:[^\s,"]|"(?:\\.|[^"])*")+')

### ------------ main rendering page and sidebar ---------------------

def main_page(data_dir=None, media_file=None, ignore_update=False, symlink=""):
    """Main page for execution"""
    # read in version information
    ux_report = st.empty()
    ux_progress = st.empty()

    df = data_load("data_bundle", data_dir, True, ignore_update)
    df_label = data_label_serialize(data_dir)
    # print(tree_query.data.shape)

    if df is None:
        st.error("No data could be loaded, please check configuration options.")
        return
    df_live = main_sidebar(df)

    # Create the runtime info
    if len(df_live) == 0:
        st.markdown("## Too few samples")
        st.markdown("The specified filter criterion are too rigid. Please modify your exploration and try again.")
        return

    # plunk down a dataframe for people to explore as they want
    st.markdown(f"## text search (top {min(SAMPLE_TABLE, len(df_live))}/{len(df_live)} events)")
    df_live.sort_values(["score", "duration"], ascending=[False, True], inplace=True)

    # time plot for textual instances
    st.markdown("### timeline for text events")
    df_sub = quick_hist(df_live, None, False)
    quick_timeseries(df_live, df_sub, None, "scatter")      # time chart of top N 

    # visual and other tags in events
    st.markdown("### tags within text event shots")
    list_shots = df_live["shot"].unique()
    df_sub_shot = df[df["shot"].isin(list_shots)]
    df_sub = quick_hist(df_sub_shot, "tag")  # quick tag hist

    # generate instance inspection
    df_instance = None
    if media_file is None or not path.exists(media_file):
        st.markdown(f"*Media file `{media_file}` not found or readable, can not generate clips.*")
    elif df_live is None or len(df_live) < MIN_INSIGHT_COUNT:
        st.markdown("The specified filter criterion are too rigid. Please modify your exploration and try again.")
    else: 
        df_instance = clip_display(df_live, df, media_file, field_group="tag",
                                    label_dir=data_dir, df_label=df_label)

    return df_live



def main_sidebar(df):
    # Generate the slider filters based on the data available in this subset of titles
    # Only show the slider if there is more than one value for that slider, otherwise, don't filter

    valid_types = ["word", "entity"]
    df_sub = df[df["tag_type"].isin(valid_types)]
    keywords_new = st.sidebar.text_input("Text-search (entities, speech/text)", value='', type='default')
    idx_match = [False] * len(df_sub)    # start with whole index
    df_lower = df_sub["tag"].str.lower()
    
    found_text = []
    for phrase in REGEX_SEARCH.findall(keywords_new):
        idx_match_sub = (df_lower.str.contains(phrase.strip('"').lower())) & (df_sub["details"] != NLP_STOPWORD)
        num_match = len(df_sub[idx_match_sub])
        print(num_match, phrase)
        if num_match > 0:
            idx_match |= idx_match_sub
            found_text.append(f"{phrase} ({num_match})")
    
    st.sidebar.markdown(f"<div style='font-size:smaller;text-align:left;margin-top:-1em;'><strong>{' '.join(found_text)}</strong></div>", unsafe_allow_html=True)

    # if we didn't have any text matches, start over
    if len(keywords_new) == 0:
        idx_match = df_sub["details"] != NLP_STOPWORD    # start with whole index

    # TODO: filter out items with bad labels?

    # # Filter by slider inputs to only show relevant events
    df_filter = df_sub[idx_match]

    list_shots = df_filter["shot"].unique()
    df_filter_shots = df[(df["tag_type"]=="shot") & (df["shot"].isin(list_shots))].copy()
    st.sidebar.markdown("<hr style='margin-top:-0.25em; margin-bottom:-0.25em' />", unsafe_allow_html=True)

    # compute the distribution of shot time
    st.sidebar.markdown("<div style='font-size:smaller;text-align:left'>Filtered Shot Duration Distribution</div>", unsafe_allow_html=True)
    df_filter_shots["seconds"] = df_filter_shots["duration"].apply(lambda x: math.floor(x * 4)/4)
    df_filter_count = aggregate_tags(df_filter_shots, "shot", "seconds")
    chart = alt.Chart(df_filter_count, height=ALTAIR_SIDEBAR_HEIGHT, width=ALTAIR_SIDEBAR_WIDTH).mark_bar().encode(
        x=alt.X('seconds', sort=None),
        y=alt.Y('count', sort=None, scale=alt.Scale(type="log"), stack=None),
        tooltip=['count','seconds'])
    st.sidebar.altair_chart(chart.interactive())

    # hard work done, return the trends!
    return df_filter
