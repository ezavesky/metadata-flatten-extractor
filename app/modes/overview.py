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

### ------------ main rendering page and sidebar ---------------------

def main_page(data_dir=None, media_file=None, ignore_update=False, symlink=""):
    """Main page for execution"""
    # read in version information
    ux_report = st.empty()
    ux_progress = st.empty()

    df = data_load("data_bundle", data_dir, True, ignore_update)
    if df is None:
        st.error("No data could be loaded, please check configuration options.")
        return
    df_live = main_sidebar(df)

    # Create the runtime info
    if len(df_live) < TOP_LINE_N:
        st.markdown("## Too few samples")
        st.markdown("The specified filter criterion are too rigid. Please modify your exploration and try again.")
        return

    # logger.info(list(df.columns))

    st.markdown("## frequency analysis")

    # frequency bar chart for found labels / tags
    st.markdown("### popular tags")
    df_sub = quick_hist(df_live, "tag")  # quick tag hist
    quick_timeseries(df_live, df_sub, "tag", "scatter")      # time chart of top N 

    # frequency bar chart for types of faces

    # frequency bar chart for keywords
    st.markdown("### popular textual keywords")
    df_sub = aggregate_tags(df_live, "word", "details")
    if not NLP_TOKENIZE or len(df_sub) < 5:   # old method before NLP stop word removal
        df_sub = aggregate_tags(df_live, "word", "tag")
        df_sub = df_sub.iloc[math.floor(len(df_sub) * NLP_FILTER):]
        num_clip = list(df_sub["count"])[0]
        st.markdown(f"*Note: The top {round(NLP_FILTER * 100, 1)}% of most frequent events (more than {num_clip} instances) have been dropped.*")
    else:
        st.markdown(f"*Note: Results after stop word removal.*")
        df_sub.rename(columns={"details":"tag"}, inplace=True)
        df_sub = df_sub[(df_sub["tag"] != NLP_STOPWORD) & (df_sub["tag"] != "")]
    quick_sorted_barchart(df_sub)

    # frequency bar chart for found labels / tags
    st.markdown("### popular textual named entities")
    df_sub = quick_hist(df_live, "entity")  # quick tag hist

    # frequency bar chart for brands
    st.markdown("### popular brands")
    df_sub = quick_hist(df_live, "brand")
    quick_timeseries(df_live, df_sub, "brand", "scatter")      # time chart of top N 

    # frequency bar chart for celebrities
    st.markdown("### popular celebrities")
    df_sub = quick_hist(df_live, "identity")
    quick_timeseries(df_live, df_sub, "identity", "scatter")      # time chart of top N 
    
    # frequency bar chart for emotions
    st.markdown("### frequent emotion and sentiment")
    df_sub = aggregate_tags(df_live, ["sentiment", "emotion"])
    quick_timeseries(df_live, df_sub, ["sentiment", "emotion"], "scatter")      # time chart of top N 

    # frequency bar chart for celebrities
    st.markdown("### moderation events timeline")
    df_sub = quick_hist(df_live, "moderation", False)
    quick_timeseries(df_live, df_sub, "moderation", "scatter")      # time chart of top N 

    # compute the distribution
    st.markdown("### overall tag distributions")
    df_sub = df_live.copy()
    df_sub["score"] = df_live["score"].apply(lambda x: math.floor(x * 100)/100)
    df_sub = aggregate_tags(df_sub, None, ["tag_type","score"])
    # df_sub = df_sub.pivot(index="score", columns="tag_type", values="count").fillna(0)
    chart = alt.Chart(df_sub, height=ALTAIR_DEFAULT_HEIGHT, width=ALTAIR_DEFAULT_WIDTH).mark_bar().encode(
        x=alt.X('score', sort=None),
        y=alt.Y('count', sort=None, scale=alt.Scale(type="log"), stack=None),
        color='tag_type',
        tooltip=['tag_type','count','score'])
    st.altair_chart(chart.interactive())

    return df_live



def main_sidebar(df, sort_list=None):
    # Generate the slider filters based on the data available in this subset of titles
    # Only show the slider if there is more than one value for that slider, otherwise, don't filter
    idx_match = [True] * len(df)    # start with whole index

    df_text_df = df[(df["tag_type"]=="brand") | (df["tag_type"]=="identity")]
    sel_keywords = st.sidebar.multiselect("Brand, Celebrity (intersecting search)", list(df_text_df['tag'].unique()), default=None)
    shot_list_keyword = set(df_text_df['shot'].unique())
    for keyword in sel_keywords:
        shot_list_keyword = shot_list_keyword.intersection(df_text_df[df_text_df["tag"]==keyword]['shot'].unique())
    st.sidebar.markdown("<hr style='margin-top:-0.25em; margin-bottom:-0.25em' />", unsafe_allow_html=True)
    # print("KEY", shot_list_keyword, shot_list)
    if len(shot_list_keyword) > 0:    # filter indexes by text match
        idx_match &= df['shot'].isin(shot_list_keyword)

    # strict timeline slider
    value = (int(df.index.min().seconds // 60), int(df.index.max().seconds // 60))
    time_bound = st.sidebar.slider("Event Time Range (min)", min_value=value[0], max_value=value[1], value=value)
    idx_match &= (df['time_begin'] >= pd.to_timedelta(time_bound[0], unit='min')) \
                    & (df['time_end'] <= pd.to_timedelta(time_bound[1], unit='min'))

    # confidence measure
    value = (df["score"].min(), df["score"].max())
    score_bound = st.sidebar.slider("Insight Score", min_value=value[0], max_value=value[1], value=value, step=0.01)
    idx_match &= (df['score'] >= score_bound[0]) & (df['score'] <= score_bound[1])

    # extract shot extents (shot length)
    df_shot = df[df["tag_type"]=="shot"]
    value = (int(df_shot["duration"].min()), int(df_shot["duration"].max()))
    if value[0] != value[1]:
        duration_bound = st.sidebar.slider("Shot Duration (sec)", min_value=value[0], max_value=value[1], value=value, step=1)
        # updated to use duration of shot as filter, not the item directly
        idx_shot = df_shot[(df_shot['duration'] >= duration_bound[0]) & (df_shot['duration'] <= duration_bound[1])]["shot"].unique()
        idx_match &= df["shot"].isin(idx_shot)
    
    # list for selected shot source
    shot_source = st.sidebar.selectbox("Shot Source", list(df[df["tag_type"]=="shot"]["extractor"].unique()) )
    st.sidebar.markdown("<div style='margin-top:-1em;font-size:smaller;text-align:center'>(currently disabled)</div>", unsafe_allow_html=True)

    # extract faces (emotion)

    # Filter by slider inputs to only show relevant events
    df_filter = df[idx_match]
    st.sidebar.markdown("<hr style='margin-top:-0.25em; margin-bottom:-0.25em' />", unsafe_allow_html=True)

    # compute the distribution of shot time
    st.sidebar.markdown("<div style='font-size:smaller;text-align:left'>Filtered Shot Duration Distribution</div>", unsafe_allow_html=True)
    df_sub = df_filter.copy()
    df_sub["seconds"] = df_sub["duration"].apply(lambda x: math.floor(x * 4)/4)
    df_sub = aggregate_tags(df_sub, "shot", "seconds")
    chart = alt.Chart(df_sub, height=ALTAIR_SIDEBAR_HEIGHT, width=ALTAIR_SIDEBAR_WIDTH).mark_bar().encode(
        x=alt.X('seconds', sort=None),
        y=alt.Y('count', sort=None, scale=alt.Scale(type="log"), stack=None),
        tooltip=['count','seconds'])
    st.sidebar.altair_chart(chart.interactive())

    # hard work done, return the trends!
    if sort_list is None:
        return df_filter
    # otherwise apply sorting right now
    return df_filter.sort_values(by=[v[0] for v in sort_list], 
                                       ascending=[v[1] for v in sort_list])
