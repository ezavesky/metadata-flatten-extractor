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
import numpy as np
from os import path, system, unlink
import math
import json

import altair as alt

presence_bars = False  # toggle to show presence indicators as a graph

from . import *

NUM_SUMMARY = 10

### ------------ main rendering page and sidebar ---------------------

def main_page(data_dir=None, media_file=None, ignore_update=False):
    """Main page for execution"""
    # read in version information
    ux_report = st.empty()
    ux_progress = st.empty()

    if data_dir is None:
        data_dir = path.join(path.dirname(version_path), "results")
    if media_file is None:
        media_file = path.join(data_dir, "videohd.mp4")

    df = data_load("data_bundle", data_dir, True, ignore_update)
    # TODO: future download capability ...
    # df.to_csv(path.join(data_dir, "data_bundle.snapshot.csz.gz"), sep='|')
    if df is None:
        st.error("No data could be loaded, please check configuration options.")
        return
    df_live = main_sidebar(df)

    # Create the runtime info
    if len(df_live) < SAMPLE_N:
        st.markdown("## Too few samples")
        st.markdown("The specified filter criterion are too rigid. Please modify your exploration and try again.")
        return

    # plunk down a dataframe for people to explore as they want
    st.markdown(f"## filtered exploration ({SAMPLE_N}/{len(df_live)} events)")
    sort_unique = ["random", "score - descending", "score - ascending", "time_begin", "time_end", 
                                         "duration - ascending", "duration - descending"]
    order_tag = st.selectbox("Sort Metric", sort_unique, index=sort_unique.index('score - descending'))
    order_ascend = order_tag.split('-')[-1].strip() == "ascending"  # eval to true/false
    order_sort = order_tag.split('-')[0].strip()
    if order_tag == "random":
        df_sub = df_live.sample(SAMPLE_N)
    else:
        df_sub = df_live.sort_values(order_sort, ascending=order_ascend).head(SAMPLE_N)


    if media_file is None or not path.exists(media_file):
        st.markdown(f"*Media file `{media_file}` not found or readable, can not generate clips.*")
    elif df_live is None or len(df_live) < SAMPLE_N:
        st.markdown("The specified filter criterion are too rigid. Please modify your exploration and try again.")
    else: 
        _, clip_ext = path.splitext(path.basename(media_file))
        media_clip = path.join(path.dirname(media_file), "".join(["temp_clip", clip_ext]))
        media_image = path.join(path.dirname(media_file), "temp_thumb.jpg")

        sel_tag = st.selectbox("Tag for Visualzation", list(df_live["tag"].unique())) 
        # sor to find the best scoring, shortest duration clip
        df_sub_sel = df_live[df_live["tag"]==sel_tag]
        # get begin_time with max score for selected celeb
        sel_shot = df_sub_sel["shot"][0]
        time_event_sec = int(df_sub_sel['time_begin'][0] / np.timedelta64(1, 's'))   # convert to seconds 
        # pull row from all data
        row_first = df[(df["shot"]==sel_shot) & (df["tag_type"]=="shot")].head(1)
        time_begin_sec = int(row_first['time_begin'] / np.timedelta64(1, 's'))   # convert to seconds
        time_duration_sec = int(row_first["duration"])  # use shot duration
        if time_duration_sec < DEFAULT_CLIPLEN:   # min clip length
            time_duration_sec = DEFAULT_CLIPLEN
        time_str = str(row_first['time_begin'][0])

        if st.button("Play Clip"):
            status = clip_video(media_file, media_clip, int(time_begin_sec-DEFAULT_REWIND), time_duration_sec)
            if status == 0: # play clip
                st.video(open(media_clip, 'rb'))
                st.markdown(f"*Tag: {sel_tag} (score: {row_first['score'][0]}) @ {time_str} ({round(time_duration_sec, 2)}s)*")
            elif status == -1:
                st.markdown("**Error:** ffmpeg not found in path. Cannot create video clip.")
            else:
                st.markdown("**Error:** creating video clip.")
        else:       # print thumbnail
            media_data = clip_media(media_file, media_image, time_event_sec)
            if media_data is not None:
                st.image(media_data, use_column_width=True,
                        caption=f"Tag: {sel_tag} (score: {row_first['score'][0]}) @ {time_str}")

    # compute the distribution
    st.markdown("### overall tag distributions")
    df_sub = df_live.copy()
    df_sub["score"] = df_live["score"].apply(lambda x: math.floor(x * 100)/100)
    df_sub = aggregate_tags(df_sub, None, ["tag","score"])
    if len(df_sub) > NUM_SUMMARY:
        score_cut = df_sub.iloc[NUM_SUMMARY]["count"]
        df_sub.loc[df_sub["count"] < score_cut, "tag"] = "(other)"

    # df_sub = df_sub.pivot(index="score", columns="tag_type", values="count").fillna(0)
    chart = alt.Chart(df_sub, height=ALTAIR_DEFAULT_HEIGHT, width=ALTAIR_DEFAULT_WIDTH).mark_bar().encode(
        x=alt.X('score', sort=None, stack=None),
        y=alt.Y('count', sort=None, scale=alt.Scale(type="log"), stack=None),
        color='tag',
        tooltip=['tag','count','score'])
    st.altair_chart(chart.interactive())

    return df_live



def main_sidebar(df, sort_list=None):
    # Generate the slider filters based on the data available in this subset of titles
    # Only show the slider if there is more than one value for that slider, otherwise, don't filter

    type_unique = ["All"] + list(df["tag_type"].unique())
    filter_tag = st.sidebar.selectbox("Tag Type for Exploration", type_unique, index=type_unique.index('identity'))
    df_sub = df
    if filter_tag != "All":
        df_sub = df[df['tag_type'] == filter_tag]
    idx_match = [True] * len(df_sub)    # start with whole index

    # strict timeline slider
    value = (int(df.index.min().seconds // 60), int(df.index.max().seconds // 60))
    time_bound = st.sidebar.slider("Event Time Range (min)", min_value=value[0], max_value=value[1], value=value)
    idx_match &= (df_sub['time_begin'] >= pd.to_timedelta(time_bound[0], unit='min')) \
                    & (df_sub['time_end'] <= pd.to_timedelta(time_bound[1], unit='min'))

    # confidence measure
    value = (df_sub["score"].min(), df_sub["score"].max())
    score_bound = st.sidebar.slider("Insight Score", min_value=value[0], max_value=value[1], value=value, step=0.01)
    idx_match &= (df_sub['score'] >= score_bound[0]) & (df_sub['score'] <= score_bound[1])

    # sel_keywords = st.sidebar.multiselect("Tags (union search)", list(df[idx_match]['tag'].unique()), default=None)
    # shot_list = set(df[idx_match]['shot'].unique())
    # for keyword in sel_keywords:
    #     shot_list = shot_list.union(df[df["tag"]==keyword]['shot'].unique())
    # st.sidebar.markdown("<hr style='margin-top:-0.25em; margin-bottom:-0.25em' />", unsafe_allow_html=True)
    # # print("KEY", shot_list_keyword, shot_list)
    # if len(shot_list) > 0:    # filter indexes by text match
    #     idx_match &= df['shot'].isin(shot_list)

    # Filter by slider inputs to only show relevant events
    df_filter = df_sub[idx_match]
    list_shots = df_filter["shot"].unique()
    df_filter_shots = df[(df["tag_type"]=="shot") & (df["shot"].isin(list_shots))].copy()
    st.sidebar.markdown("<hr style='margin-top:-0.25em; margin-bottom:-0.25em' />", unsafe_allow_html=True)

    # compute the distribution of shot time
    st.sidebar.markdown("<div style='font-size:smaller;text-align:left'>Filtered Shot Duration Distribution</div>", unsafe_allow_html=True)
    df_filter_shots["seconds"] = df_filter_shots["duration"].apply(lambda x: math.floor(x * 4)/4)
    df_filter_shots = aggregate_tags(df_filter_shots, "shot", "seconds")
    chart = alt.Chart(df_filter_shots, height=ALTAIR_SIDEBAR_HEIGHT, width=ALTAIR_SIDEBAR_WIDTH).mark_bar().encode(
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
