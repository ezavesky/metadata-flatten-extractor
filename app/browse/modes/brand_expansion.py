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

### ------------ main rendering page and sidebar ---------------------

def main_page(data_dir=None, media_file=None, ignore_update=False, symlink="", mapping_model=""):
    """Main page for execution"""
    # read in version information
    ux_report = st.empty()
    ux_progress = st.empty()

    df = data_load(PATH_BASE_BUNDLE, data_dir, True, ignore_update, nlp_model=mapping_model)
    # print(tree_query.data.shape)

    if df is None:
        st.error("No data could be loaded, please check configuration options.")
        return None
    tree_query, tree_shots = data_index(PATH_BASE_VECTORS, data_dir, df, ignore_update=ignore_update)   # convert data to numbers
    df_label = data_label_serialize(data_dir)
    df_live = main_sidebar(df)

    # Create the runtime info
    if len(df_live) < MIN_INSIGHT_COUNT:
        st.markdown("## Too few samples")
        st.markdown("The specified filter criterion are too rigid. Please modify your exploration and try again.")
        return None

    # plunk down a dataframe for people to explore as they want
    st.markdown(f"## brand expansion (top {min(SAMPLE_TABLE, len(df_live))}/{len(df_live)} events)")
    df_live.sort_values(["score", "duration"], ascending=[False, True], inplace=True)

    df_instance = None
    if media_file is None or not path.exists(media_file):
        st.markdown(f"*Media file `{media_file}` not found or readable, can not generate clips.*")
    elif df_live is None or len(df_live) < MIN_INSIGHT_COUNT:
        st.markdown("The specified filter criterion are too rigid. Please modify your exploration and try again.")
    else: 
        df_instance = clip_display(df_live, df, media_file, field_group="extractor",
                                    label_dir=data_dir, df_label=df_label)

    # compute the distribution
    st.markdown("### source tag distributions")
    df_sub = df_live.copy()
    df_sub["score"] = df_live["score"].apply(lambda x: math.floor(x * 100)/100)
    df_sub = aggregate_tags(df_sub, None, ["extractor","score"])

    # df_sub = df_sub.pivot(index="score", columns="tag_type", values="count").fillna(0)
    chart = alt.Chart(df_sub, height=ALTAIR_DEFAULT_HEIGHT, width=ALTAIR_DEFAULT_WIDTH).mark_bar().encode(
        x=alt.X('score', sort=None, stack=None),
        y=alt.Y('count', sort=None, scale=alt.Scale(type="log"), stack=None),
        color='extractor',
        tooltip=['extractor','count','score'])
    st.altair_chart(chart.interactive())

    if df_instance is not None:   # dump out the row as JSON
        st.markdown("### selected instance data")
        st.json(df_instance.to_json(orient='records'))

    # grab shot IDs, use them to grab all rows from data
    st.markdown("## data for brand expansion")
    list_shots = df_live["shot"].unique()
    df_sub = df[df["shot"].isin(list_shots)].copy()
    df_sub["score"] = df_sub["score"].apply(lambda x: math.floor(x * 100)/100)
    df_sub = aggregate_tags(df_sub, None, ["tag_type","score"])
    # df_sub = df_sub.pivot(index="score", columns="tag_type", values="count").fillna(0)
    chart = alt.Chart(df_sub, height=ALTAIR_DEFAULT_HEIGHT, width=ALTAIR_DEFAULT_WIDTH).mark_bar().encode(
        x=alt.X('score', sort=None),
        y=alt.Y('count', sort=None, scale=alt.Scale(type="log"), stack=None),
        color='tag_type',
        tooltip=['tag_type','count','score'])
    st.altair_chart(chart.interactive())

    # query with shot list to find similar instances...
    num_exmples = st.number_input("Examples for Query", min_value=5, value=5, max_value=20)
    num_neighbors = st.number_input("Number of Neighbors", min_value=5, value=10, max_value=20)
    # run query against indexed data with parameters
    df_expanded = data_query(tree_query, tree_shots, df_live.head(num_exmples)["shot"].unique(), 
                            num_neighbors, exclude_input_shots=True)

    # for now, plot what new distributin looks like
    st.markdown("### visual tags after brand expansion")
    df_sub = df[df["shot"].isin(df_expanded['shot'])].copy()
    quick_hist(df_sub, "tag")  # quick tag hist

    # save for expanded brand exploration, exclude our currently selected tag (from df_live)
    st.markdown("### overall distributions after brand expansion")
    df_brand_expand = df_sub[(df_sub["tag_type"]=="brand") & (df_sub["tag"]!=df_live["tag"][0])]
    df_sub["score"] = df_sub["score"].apply(lambda x: math.floor(x * 100)/100)
    df_sub = aggregate_tags(df_sub, None, ["tag_type","score"])
    # df_sub = df_sub.pivot(index="score", columns="tag_type", values="count").fillna(0)
    chart = alt.Chart(df_sub, height=ALTAIR_DEFAULT_HEIGHT, width=ALTAIR_DEFAULT_WIDTH).mark_bar().encode(
        x=alt.X('score', sort=None),
        y=alt.Y('count', sort=None, scale=alt.Scale(type="log"), stack=None),
        color='tag_type',
        tooltip=['tag_type','count','score'])
    st.altair_chart(chart.interactive())

    st.markdown("### Lookalike Brands By Content Expansion")
    df_sub = quick_hist(df_brand_expand, "brand")

    df_instance = None
    if media_file is None or not path.exists(media_file):
        st.markdown(f"*Media file `{media_file}` not found or readable, can not generate clips.*")
    elif df_live is None or len(df_live) < MIN_INSIGHT_COUNT:
        st.markdown("The specified filter criterion are too rigid. Please modify your exploration and try again.")
    else: 
        df_instance = clip_display(df_brand_expand, df, media_file, field_group="tag",
                                    label_dir=data_dir, df_label=df_label)

    return df_live


def main_sidebar(df):
    # Generate the slider filters based on the data available in this subset of titles
    # Only show the slider if there is more than one value for that slider, otherwise, don't filter

    df_sub = df[df["tag_type"]=="brand"]
    df_group = aggregate_tags(df_sub, None, ["tag"])
    unique_tag = df_group[df_group["count"] >= MIN_INSIGHT_COUNT]["tag"].unique()   # must have a min count

    filter_tag = st.sidebar.selectbox("Brand", unique_tag)
    df_sub = df_sub[(df_sub["tag_type"]=="brand") & (df_sub["tag"]==filter_tag)]
    idx_match = [True] * len(df_sub)    # start with whole index
    st.sidebar.markdown(f"<div style='font-size:smaller;text-align:left;margin-top:-1em;'><em>(brands with mininum of {MIN_INSIGHT_COUNT} instances)</em></div>", unsafe_allow_html=True)

    # strict timeline slider
    value = (int(df.index.min().seconds // 60), int(df.index.max().seconds // 60))
    time_bound = st.sidebar.slider("Event Time Range (min)", min_value=value[0], max_value=value[1], value=value)
    idx_match &= (df_sub['time_begin'] >= pd.to_timedelta(time_bound[0], unit='min')) \
                    & (df_sub['time_end'] < pd.to_timedelta(time_bound[1], unit='min'))

    # confidence measure
    if len(df_sub) > 1:  # only if there are more than two samples
        value = (df_sub["score"].min(), df_sub["score"].max())
        score_bound = st.sidebar.slider("Insight Score", min_value=value[0], max_value=value[1], value=value, step=0.01)
        idx_match &= (df_sub['score'] >= score_bound[0]) & (df_sub['score'] <= score_bound[1])

    # Filter by slider inputs to only show relevant events
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
