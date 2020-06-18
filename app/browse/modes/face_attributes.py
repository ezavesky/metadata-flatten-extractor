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
from .common.preprocessing import *


LIST_NOFACE_TAGS = ["Face", "Age"]


### ------------ main rendering page and sidebar ---------------------

def main_page(data_dir=None, media_file=None, ignore_update=False, symlink=""):
    """Main page for execution"""
    # read in version information
    ux_report = st.empty()
    ux_progress = st.empty()

    df = data_load(PATH_BASE_BUNDLE, data_dir, True, ignore_update)
    df_label = data_label_serialize(data_dir)
    # print(tree_query.data.shape)

    if df is None:
        st.error("No data could be loaded, please check configuration options.")
        return
    df_sub = df[df["tag_type"]=="face"]
    if len(df_sub) < 1:
        st.error("No face attributes (emotions, etc.) detected with current data, please check input metadata.")
        return
    df_live = main_sidebar(df)

    # Create the runtime info
    if len(df_live) == 0:
        st.markdown("## Too few samples")
        st.markdown("The specified filter criterion are too rigid. Please modify your exploration and try again.")
        return

    # plunk down a dataframe for people to explore as they want
    st.markdown(f"## face attributes (top {min(SAMPLE_TABLE, len(df_live))}/{len(df_live)} events)")
    df_live.sort_values(["score", "duration"], ascending=[False, True], inplace=True)

    df_instance = None
    if media_file is None or not path.exists(media_file):
        st.markdown(f"*Media file `{media_file}` not found or readable, can not generate clips.*")
    elif df_live is None or len(df_live) < MIN_INSIGHT_COUNT:
        st.markdown("The specified filter criterion are too rigid. Please modify your exploration and try again.")
    else: 
        df_instance = clip_display(df_live, df, media_file, field_group="tag",
                                    label_dir=data_dir, df_label=df_label)

    # time plot for textual instances
    st.markdown("### face events")
    df_sub = quick_hist(df_live, None)  # quick tag hist
    quick_timeseries(df_live, df_sub, None, "scatter")      # time chart of top N 

    # generate instance inspection
    # df_instance = None
    # if media_file is None or not path.exists(media_file):
    #     st.markdown(f"*Media file `{media_file}` not found or readable, can not generate clips.*")
    # elif df_live is None or len(df_live) < MIN_INSIGHT_COUNT:
    #     st.markdown("The specified filter criterion are too rigid. Please modify your exploration and try again.")
    # else: 
    #     df_instance = clip_display(df_live, df, media_file, field_group="tag",
    #                                 label_dir=data_dir, df_label=df_label)

    return df_live



def main_sidebar(df):
    # Generate the slider filters based on the data available in this subset of titles
    # Only show the slider if there is more than one value for that slider, otherwise, don't filter

    df_sub = df[df["tag_type"]=="face"]
    idx_match = [True] * len(df_sub)

    # aggregate for tag
    list_attributes = list(set(df_sub["tag"].unique()).difference(LIST_NOFACE_TAGS))
    sel_attribute = st.sidebar.multiselect("Shot Attributes", list_attributes)

    # filtering mode
    text_found = st.sidebar.empty()
    mode_set = ["union (OR search)", "exclusion (NOT(OR) search)", "intersection (AND search)"]
    filter_mode = st.sidebar.selectbox("Filter Mode", mode_set, index=0)

    # age filtering
    df_age = df_sub[df_sub["tag"]=="Age"].copy()
    if len(df_age):
        df_age["low"] = df_age["details"].apply(lambda x: json.loads(x)["AgeRange"]["Low"]).astype(int)
        df_age["high"] = df_age["details"].apply(lambda x: json.loads(x)["AgeRange"]["High"]).astype(int)
        value = (int(df_age["low"].min()), int(df_age["high"].max()))
        age_bound = st.sidebar.slider("Age Range", min_value=value[0], max_value=value[1], value=value)
        time_include = df_age[(df_age["low"] >= age_bound[0]) & (df_age["high"] <= age_bound[1])].index.unique()
        idx_match &= df_sub.index.isin(time_include)

    st.sidebar.markdown("<hr style='margin-top:-0.25em; margin-bottom:-0.25em' />", unsafe_allow_html=True)

    # strict timeline slider
    value = (int(df.index.min().seconds // 60), int(df.index.max().seconds // 60))
    time_bound = st.sidebar.slider("Event Time Range (min)", min_value=value[0], max_value=value[1], value=value)
    idx_match &= (df_sub['time_begin'] >= pd.to_timedelta(time_bound[0], unit='min')) \
                    & (df_sub['time_end'] <= pd.to_timedelta(time_bound[1], unit='min'))

    # confidence measure
    value = (df_sub["score"].min(), df_sub["score"].max())
    score_cutoff = df_sub[idx_match]['score'].mean()
    score_bound = st.sidebar.slider("Insight Score", min_value=value[0], max_value=value[1], 
                                    value=(max(value[0],score_cutoff), value[1]), step=0.01, 
                                    key=f"score_{'_'.join(sel_attribute)}")
    idx_match &= (df_sub['score'] >= score_bound[0]) & (df_sub['score'] <= score_bound[1])

    found_attributes = []
    if sel_attribute:
        shot_include = []
        df_filter = df_sub[idx_match]
        # df_attributes = aggregate_tags(df_sub[idx_match], None, "tag")
        if filter_mode == mode_set[0]:   # union
            time_include = df_filter[df_filter["tag"].isin(sel_attribute)].index.unique()
            idx_match &= df_sub.index.isin(time_include)
        elif filter_mode == mode_set[1]:   # xor
            time_include = df_filter[df_filter["tag"].isin(sel_attribute)].index.unique()
            idx_match &= ~df_sub.index.isin(time_include)
        elif filter_mode == mode_set[2]:   # intersect
            idx_match &= df_sub["tag"].isin(sel_attribute)

    # exclude generic type 'face' that just has bounding boxes
    df_filter = df_sub[idx_match & (df_sub['tag'].isin(list_attributes))]

    st.sidebar.markdown("<hr style='margin-top:-0.25em; margin-bottom:-0.25em' />", unsafe_allow_html=True)

    # compute the distribution of shot time
    list_shots = df_sub[idx_match]['shot'].unique()
    df_filter_shots = df[(df["tag_type"]=="shot") & (df["shot"].isin(list_shots))].copy()
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
