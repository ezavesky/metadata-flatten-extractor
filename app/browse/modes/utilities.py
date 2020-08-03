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
import pandas as pd
import numpy as np
from pathlib import Path
import hashlib

import altair as alt
import streamlit as st

import logging

from .common import preprocessing, media

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

TOP_HISTOGRAM_N = 15   # max number of elements to show in histogram
TOP_LINE_N = 5   # max number of samples to show in timeseries plot
MIN_INSIGHT_COUNT = 3   # min count for samples in 'insight' viewing (e.g. brand, text, event)
NLP_FILTER = 0.025   # if stop word analysis not ready, what is HEAD/TAIL trim for frequencyy?
SAMPLE_TABLE = 100   # how many samples go in the dataframe table dump
MAX_LOCK_COUNT = 3   # how many lock loops shoudl we wait (for labels)

DEFAULT_REWIND = 2   # how early to start clip from max score (sec)
DEFAULT_CLIPLEN = 5   # length of default cllip (sec)
DEFAULT_REWIND_FRAME = -0.25   # rewind for frame-specific starts

ALTAIR_DEFAULT_WIDTH = 660   # width of charts
ALTAIR_DEFAULT_HEIGHT = 220   # height of charts
ALTAIR_SIDEBAR_WIDTH = 280   # width of charts (in sidebar)
ALTAIR_SIDEBAR_HEIGHT = 180   # height of charts (in sidebar)

LABEL_TEXT = ['Invalid', 'Unverified', 'Valid']  # -1, 0, 1 for labeling interface

URL_SYMLINK_BASE = "static"  # static links from the web

### ------------ dataframe and chart functions ---------------------


def quick_sorted_barchart(df_sub, field_group="tag"):
    """Create bar chart (e.g. histogram) with no axis sorting..."""
    if False:   #  https://github.com/streamlit/streamlit/issues/385
        st.bar_chart(df_sub["count"].head(TOP_HISTOGRAM_N))
    else:
        # https://github.com/streamlit/streamlit/blob/5e8e0ec1b46ac0b322dc48d27494be674ad238fa/lib/streamlit/DeltaGenerator.py
        chart = alt.Chart(df_sub.head(TOP_HISTOGRAM_N), width=ALTAIR_DEFAULT_WIDTH, height=ALTAIR_DEFAULT_HEIGHT).mark_bar().encode(
            x=alt.X('count', sort=None, title='instance count'),
            y=alt.Y(field_group, sort=None),
            tooltip=[field_group, 'count', 'mean', 'min'])
        st.altair_chart(chart.interactive())


def quick_hist(df_live, tag_type, show_hist=True, field_group="tag"):
    """Helper function to draw aggregate histograms of tags"""
    df_sub = preprocessing.aggregate_tags(df_live, tag_type, field_group)
    if len(df_sub) == 0:
        st.markdown("*Active filters removed all instances or no events were available for this display.*")
        return None
    # unfortunately, a bug currently overrides sort order
    if show_hist:
        quick_sorted_barchart(df_sub, field_group)
    return df_sub


def quick_timeseries(df_live, df_sub, tag_type, graph_type='line'):
    """Helper function to draw a timeseries for a few top selected tags..."""
    if df_sub is None:
        return
    if len(df_sub) == 0:
        st.markdown("*Active filters removed all instances or no events were available for this display.*")
        return None
    df_subtags = df_live
    if tag_type is not None:
        if type(tag_type) != list:
            tag_type = [tag_type]
        df_subtags = df_live[df_live["tag_type"].isin(tag_type)]
 
    add_tag = st.selectbox("Additional Timeline Tag", list(df_sub["tag"].unique()))
    tag_top = list(df_sub["tag"].head(TOP_LINE_N)) + [add_tag]

    df_sub = df_subtags[df_subtags["tag"].isin(tag_top)]    # filter top
    df_sub = df_sub[["tag", "score"]]   # select only score and tag name
    df_sub.index = df_sub.index.round('1T')
    df_filtered = pd.DataFrame([])
    for n in tag_top:    # resample each top tag
        df_resample = pd.DataFrame(df_sub[df_sub["tag"] == n].resample('1T', offset="0s").mean()["score"]).dropna()
        df_resample.columns = ["score"]
        # need to convert to date time -- https://github.com/altair-viz/altair/issues/967#issuecomment-399774414
        df_resample["tag"] = n
        df_filtered = pd.concat([df_filtered, df_resample], sort=False)
    df_filtered["score"] = df_filtered["score"].round(3)
    # timezone weirdness to prevent incorrect rendering in different time zones - 
    #    https://altair-viz.github.io/user_guide/times_and_dates.html?highlight=hours
    dt_start = pd.Timestamp("2020-01-01T00:00:00", tz="UTC")
    df_filtered["time"] = df_filtered.index + dt_start
    if graph_type == 'scatter':
        chart = alt.Chart(df_filtered).mark_circle().encode(
            x=alt.X('utchoursminutes(time)', sort=None, title='time'),
            y=alt.Y('score', sort=None, scale=alt.Scale(zero=False), stack=None),
            color='tag',
            tooltip=['tag','utchoursminutes(time)','score'])
    elif graph_type == 'area':
        chart = alt.Chart(df_filtered).mark_area(opacity=0.5).encode(
            x=alt.X('utchoursminutes(time)', sort=None, title='time'),
            y=alt.Y('score', sort=None, stack=None, scale=alt.Scale(zero=False)),
            color='tag', 
            tooltip=['tag','utchoursminutes(time)','score'])
    else:
        chart = alt.Chart(df_filtered).mark_line().encode(
            x=alt.X('utchoursminutes(time)', sort=None, title='time'),
            y=alt.Y('score', sort=None, scale=alt.Scale(zero=False)),
            color='tag',
            tooltip=['tag','utchoursminutes(time)','score'])
    st.altair_chart(chart.interactive().properties(width=ALTAIR_DEFAULT_WIDTH, height=ALTAIR_DEFAULT_HEIGHT))


### ------------ content functions ---------------------


@st.cache(suppress_st_warning=False)
def manifest_parse_cached(manifest_file):
    return media.manifest_parse(manifest_file)


@st.cache(suppress_st_warning=False)
def clip_media_cached(media_file, media_output, start):
    """Helper function to create video clip"""
    status = media.clip_media(media_file, media_output, start, image_only=True)
    path_media = Path(media_output)
    if status == 0 and path_media.exists():
        with path_media.open('rb') as f:
            return f.read()
    return None


def clip_display(df_live, df, media_file, field_group="tag", label_dir=None, df_label=None):
    """Create visual for video or image with selection of specific instance"""
    list_sel = []
    for idx_r, val_r in df_live.iterrows():   # make it something readable
        time_duration_sec = float(val_r["duration"])  # use shot duration
        str_duration = "" if time_duration_sec < 0.1 else f"({round(time_duration_sec, 2)}s)"
        list_sel.append(f"{len(list_sel)} - (score: {round(val_r['score'],4)}) " \
            f"@ {preprocessing.timedelta_str(val_r['time_begin'])} {str_duration} ({val_r[field_group]})")
        if len(list_sel) >= SAMPLE_TABLE:
            break
    if not list_sel:
        st.markdown("_(no instances to display)_")
        return None
    instance_sel = st.selectbox("Instance Display", list_sel)
    instance_idx = int(instance_sel.split(' ')[0])
    row_sel = pd.DataFrame([df_live.iloc[instance_idx]])

    # get begin_time with max score for selected celeb
    sel_shot = row_sel["shot"][0]
    time_event_sec = float(row_sel['time_begin'][0] / np.timedelta64(1, 's'))   # convert to seconds 
    # pull row from all data
    row_first = df[(df["shot"]==sel_shot) & (df["tag_type"]=="shot")].head(1)
    time_begin_sec = float(row_first['time_begin'][0] / np.timedelta64(1, 's'))   # convert to seconds
    time_duration_sec = float(row_first["duration"][0])  # use shot duration
    if time_duration_sec < DEFAULT_CLIPLEN:   # min clip length
        time_duration_sec = DEFAULT_CLIPLEN
    caption_str = f"*Tag: {row_sel['tag'][0]}, Instance: {instance_idx} (score: {round(row_sel['score'][0], 4)}) @ " \
        f"{preprocessing.timedelta_str(row_first['time_begin'][0])} ({round(time_duration_sec, 2)}s)*"

    dir_media = Path(media_file).parent
    media_clip = dir_media.joinpath(".".join(["temp_clip", Path(media_file).suffix]))
    media_image = dir_media.joinpath("temp_thumb.jpg")

    if st.button("Play Clip", key=f"{field_group}_{time_begin_sec}"):
        status = media.clip_video(media_file, media_clip, int(time_begin_sec-DEFAULT_REWIND), time_duration_sec)
        if status == 0: # play clip
            st.video(open(media_clip, 'rb'))
            st.markdown(caption_str)
        elif status == -1:
            st.markdown("**Error:** ffmpeg not found in path. Cannot create video clip.")
        else:
            st.markdown(f"**Error:** Could not create video clip for time interval [{time_begin_sec}s, duration {time_duration_sec}s].")
            st.markdown("_(no instances to display)_")
    else:       # print thumbnail
        media_data = clip_media_cached(media_file, media_image, time_event_sec-DEFAULT_REWIND_FRAME)
        if media_data is not None:
            st.image(media_data, use_column_width=True, caption=caption_str)
        else:
            st.markdown(f"**Error:** Could not create image at time offset [{time_begin_sec}s].")

    if label_dir is not None:
        label_display(label_dir, df_label, row_sel)
    return row_sel


def label_display(label_dir, df_label, row_sel):
    label_initial = 1   # start with unlabeled state
    if df_label is not None:
        label_initial_row = df_label[(df_label["time_begin"]==row_sel['time_begin'][0]) \
                                    & (df_label["extractor"]==row_sel['extractor'][0]) \
                                    & (df_label["tag"]==row_sel['tag'][0]) \
                                    & (df_label["tag_type"]==row_sel['tag_type'][0])]
        if len(label_initial_row):
            label_initial = int(label_initial_row['label']) + 1
    time_event_sec = float(row_sel['time_begin'][0] / np.timedelta64(1, 's'))   # convert to seconds 
    label_instance = st.radio(f"Label for Instance '{row_sel['tag'][0]}'", index=label_initial,
        key=f"{row_sel['tag'][0]}_label_{row_sel['extractor'][0]}_{time_event_sec}", options=LABEL_TEXT)
    label_new = LABEL_TEXT.index(label_instance)
    if label_new != label_initial:  # only write on delta; 
        data_label_serialize(label_dir, row_sel, label_new - 1)


## ---- data load functions --- 

# def download_content(content_bytes, mime_type, filename):
#     # Not officially supported 2/9/20 - https://github.com/streamlit/streamlit/issues/400
#     # too heavy: https://user-images.githubusercontent.com/42288570/70138254-2b8d3e80-1690-11ea-8968-9c94ee2c9709.gif
#     response = make_response(content_bytes, 200)
#     response.headers['Content-type'] = 'application/pdf'
#     response.headers['Content-disposition'] = f'Content-Disposition: inline; filename="{filename}"'

@st.cache(suppress_st_warning=True)
def data_discover(stem_datafile, data_dir, bundle_files):
    return preprocessing.data_discover_raw(stem_datafile, data_dir, bundle_files)

@st.cache(suppress_st_warning=True)
def data_load(stem_datafile, data_dir, allow_cache=True, ignore_update=False, nlp_model="en_core_web_lg"):
    ux_report = st.empty()
    ux_progress = st.progress(0)

    def _local_update(str_new="", progress=0, is_warning=False):   # simple callback from load process
        if is_warning:
            st.sidebar.warning(str_new)
        else:
            if len(str_new):
                ux_report.info(str_new)
                if progress > 0:
                    ux_progress.progress(progress)
            else:
                ux_report.empty()
                ux_progress.empty()

    return preprocessing.data_load_callback(stem_datafile, data_dir, allow_cache=allow_cache,
                                           ignore_update=ignore_update, fn_callback=_local_update,
                                           nlp_model=nlp_model)


def download_link(path_temp, name_link=None, df=None, path_src=None):
    path_target = Path(path_temp)
    m = hashlib.md5()  # get unique code for this table
    if path_temp is None or len(path_temp)==0:
        st.markdown("**Downloadable data not available, please check temporary path symlink.**")
        return
    if df is not None:
        m.update(str(len(df)).encode())
        m.update(str(len(df["time_begin"].unique())).encode())
        m.update(str(len(df["tag"].unique())).encode())
    elif path_src is not None:
        m.update(path_src.encode())
    else:
        st.markdown("**Eror: Neither a dataframe nor an input path were detected for downloadable data.**")
        return

    str_symlink = str(path_target.name)
    str_unique = m.hexdigest()[:8]
    str_url = None
    if df is not None:
        if st.button("Download Data", key=f"table_{str_unique}"):
            path_write = path_target.joinpath(f"table_{str_unique}.csv.gz")
            if not path_write.exists():
                df.to_csv(str(path_write), index=False)
                str_url = f"{URL_SYMLINK_BASE}/{str_symlink}/{str(path_write.name)}"
            st.markdown(f"[{name_link}]({str_url})")
        else:
            return None   # otherwise, button not clicked
    else:       # otherwise, just make a symlink to existing path
        suffixes = "".join(Path(path_src).suffixes)
        str_file = f"file_{str_unique}{suffixes}"
        if name_link is not None:
            str_file = f"file_{name_link}_{str_unique}{suffixes}"
        path_link = path_target.joinpath(str_file)
        if not path_link.exists():
            path_link.symlink_to(path_src, True)
        str_url = f"{URL_SYMLINK_BASE}/{str_symlink}/{str(path_link.name)}"
    
    return str_url


@st.cache(suppress_st_warning=True, allow_output_mutation=True)
def data_index(stem_datafile, data_dir, df, allow_cache=True, ignore_update=False):
    ux_report = st.empty()
    ux_progress = st.progress(0)
    def _local_update(str_new="", progress=0, is_warning=False):   # simple callback from load process
        if is_warning:
            st.sidebar.warning(str_new)
        else:
            if len(str_new):
                ux_report.info(str_new)
                if progress > 0:
                    ux_progress.progress(progress)
            else:
                ux_report.empty()
                ux_progress.empty()

    return preprocessing.data_index_callback(stem_datafile, data_dir=data_dir, df=df, allow_cache=allow_cache, 
                                              ignore_update=ignore_update, fn_callback=_local_update)



def data_label_serialize(label_dir, df_new=None, label_new=None):
    ux_report = st.empty()
    def _local_update(str_new="", progress=0, is_warning=False):   # simple callback from load process
        if is_warning:
            st.sidebar.warning(str_new)
        else:
            if len(str_new):
                ux_report.info(str_new)
            else:
                ux_report.empty()

    return preprocessing.data_label_serialize_callback(label_dir, df_new, label_new, fn_callback=_local_update)
