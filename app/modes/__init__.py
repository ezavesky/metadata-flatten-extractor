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
import pkgutil
import pandas as pd
import numpy as np
from os import path, system, unlink
from pathlib import Path
import re
import hashlib
import glob
import math
import json

import altair as alt

import logging
import warnings
from sys import stdout as STDOUT

# save module list at this level
modules = [name for _, name, _ in pkgutil.iter_modules(__path__)]

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
NLP_TOKENIZE = True
NLP_STOPWORD = "_stopword_"

TOP_HISTOGRAM_N = 15
TOP_LINE_N = 5
NLP_FILTER = 0.025
SAMPLE_N = 10

DEFAULT_REWIND = 2 # how early to start clip from max score (sec)
DEFAULT_CLIPLEN = 5 # length of default cllip (sec)

ALTAIR_DEFAULT_WIDTH = 660   # width of charts
ALTAIR_DEFAULT_HEIGHT = 220   # height of charts
ALTAIR_SIDEBAR_WIDTH = 280   # width of charts
ALTAIR_SIDEBAR_HEIGHT = 180   # height of charts


### ------------ helper functions ---------------------

# @st.cache(suppress_st_warning=False)
def aggregate_tags(df_live, tag_type, field_group="tag"):
    df_sub = df_live
    if tag_type is not None:
        if type(tag_type) != list:
            tag_type = [tag_type]
        df_sub = df_live[df_live["tag_type"].isin(tag_type)]
    # df_sub = df_sub.resample('1S', base=0).max().dropna().groupby(field_group)["score"] \  # consider resampling to seconds/minutnes?
    df_sub = df_sub.groupby(field_group)["score"] \
                .agg(['count', 'mean', 'max', 'min']).reset_index(drop=False) \
                .sort_values(["count", "mean"], ascending=False)
    df_sub[["mean", "min", "max"]] = df_sub[["mean", "min", "max"]].apply(lambda x: round(x, 3))
    return df_sub


def quick_sorted_barchart(df_sub):
    """Create bar chart (e.g. histogram) with no axis sorting..."""
    if False:   #  https://github.com/streamlit/streamlit/issues/385
        st.bar_chart(df_sub["count"].head(TOP_HISTOGRAM_N))
    else:
        # https://github.com/streamlit/streamlit/blob/5e8e0ec1b46ac0b322dc48d27494be674ad238fa/lib/streamlit/DeltaGenerator.py
        chart = alt.Chart(df_sub.head(TOP_HISTOGRAM_N), width=ALTAIR_DEFAULT_WIDTH, height=ALTAIR_DEFAULT_HEIGHT).mark_bar().encode(
            x=alt.X('count', sort=None, title='instance count'),
            y=alt.Y('tag', sort=None),
            tooltip=['tag', 'count', 'mean', 'min'])
        st.altair_chart(chart.interactive())


def quick_hist(df_live, tag_type, show_hist=True, field_group="tag"):
    """Helper function to draw aggregate histograms of tags"""
    df_sub = aggregate_tags(df_live, tag_type, field_group)
    if len(df_sub) == 0:
        st.markdown("*Sorry, the active filters removed all events for this display.*")
        return None
    # unfortunately, a bug currently overrides sort order
    if show_hist:
        quick_sorted_barchart(df_sub)
    return df_sub


def quick_timeseries(df_live, df_sub, tag_type, graph_type='line'):
    """Helper function to draw a timeseries for a few top selected tags..."""
    if df_sub is None:
        return
    if type(tag_type) != list:
        tag_type = [tag_type]
    add_tag = st.selectbox("Additional Timeline Tag", list(df_sub["tag"].unique()))
    tag_top = list(df_sub["tag"].head(TOP_LINE_N)) + [add_tag]

    df_subtags = df_live[df_live["tag_type"].isin(tag_type)]
    df_sub = df_subtags[df_subtags["tag"].isin(tag_top)]    # filter top
    df_sub = df_sub[["tag", "score"]]   # select only score and tag name
    df_sub.index = df_sub.index.round('1T')
    df_filtered = pd.DataFrame([])
    for n in tag_top:    # resample each top tag
        df_resample = pd.DataFrame(df_sub[df_sub["tag"] == n].resample('1T', base=0).mean()["score"]).dropna()
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


def clip_video(media_file, media_output, start, duration=1, image_only=False):
    """Helper function to create video clip"""
    if path.exists(media_output):
        unlink(media_output)
    if (system("which ffmpeg")==0):  # check if ffmpeg is in path
        if not image_only:
            return system(f"ffmpeg -ss {start} -i {media_file} -t {duration} -c copy -y {media_output}")
        else: 
            # TODO: do we allow force of an aspect ratio for bad video transcode?  e.g. -vf 'scale=640:360' 
            return system(f"ffmpeg  -ss {start} -i {media_file} -r 1 -t 1 -f image2 -y {media_output}")  
    else:
        return -1


@st.cache(suppress_st_warning=False)
def clip_media(media_file, media_output, start):
    """Helper function to create video clip"""
    status = clip_video(media_file, media_output, start, image_only=True)
    if status == 0:
        with open(media_output, 'rb') as f:
            return f.read()
    return None
        
# def download_content(content_bytes, mime_type, filename):
#     # Not officially supported 2/9/20 - https://github.com/streamlit/streamlit/issues/400
#     # too heavy: https://user-images.githubusercontent.com/42288570/70138254-2b8d3e80-1690-11ea-8968-9c94ee2c9709.gif
#     response = make_response(content_bytes, 200)
#     response.headers['Content-type'] = 'application/pdf'
#     response.headers['Content-disposition'] = f'Content-Disposition: inline; filename="{filename}"'


## ---- data load functions --- 

@st.cache(suppress_st_warning=True)
def data_load(stem_datafile, data_dir, allow_cache=True, ignore_update=False):
    """Because of repetitive loads in streamlit, a method to read/save cache data according to modify time."""
    # generate a checksum of the input files
    m = hashlib.md5()
    list_files = []
    for filepath in sorted(Path(data_dir).rglob(f'flatten_*.csv*')):
        list_files.append(filepath)
        m.update(str(filepath.stat().st_mtime).encode())

    path_backup = None
    for filepath in Path(data_dir).glob(f'{stem_datafile}.*.pkl.gz'):
        path_backup = filepath
        break

    if not list_files and path_backup is None:
        logger.critical(f"Sorry, no flattened or cached files found, check '{data_dir}'...")
        return None 

    # NOTE: according to this article, we should use 'feather' but it has depedencies, so we use pickle
    # https://towardsdatascience.com/the-best-format-to-save-pandas-data-414dca023e0d
    path_new = path.join(data_dir, f"{stem_datafile}.{m.hexdigest()[:8]}.pkl.gz")

    # see if checksum matches the datafile (plus stem)
    if allow_cache and (path.exists(path_new) or path_backup is not None):
        if path.exists(path_new):  # if so, load old datafile, skip reload
            return pd.read_pickle(path_new)
        elif len(list_files) == 0 or ignore_update:  # only allow backup if new files weren't found
            st.warning(f"Warning: Using datafile `{path_backup.name}` with no grounded reference.  Version skew may occur.")
            return pd.read_pickle(path_backup)
        else:   # otherwise, delete the old backup
            unlink(path_backup.resolve())
    
    # time_init = pd.Timestamp('2010-01-01T00')  # not used any more
    ux_report = st.empty()
    ux_progress = st.empty()
    ux_report.info(f"Data has changed, regenerating core data bundle file {path_new}...")

    # Add a placeholder
    latest_iteration = st.empty()
    ux_progress = st.progress(0)
    task_buffer = 6   # account for time-norm, sorting, shot-mapping, named entity, dup-dropping
    task_count = len(list_files)+task_buffer

    df = None
    for task_idx in range(len(list_files)):
        f = list_files[task_idx]
        ux_progress.progress(math.floor(float(task_idx)/task_count*100))
        ux_report.info(f"Loading file '{f.name}'...")
        df_new = pd.read_csv(str(f.resolve()))
        df = df_new if df is None else pd.concat([df, df_new], axis=0, sort=False)
    df["details"].fillna("", inplace=True)

    logger.info(f"Known columns: {list(df.columns)}")
    logger.info(f"Known types: {list(df['tag_type'].unique())}")

    # extract shot extents
    ux_report.info(f"... mapping shot id to all events....")
    ux_progress.progress(math.floor(float(task_idx)/task_count*100))
    task_idx += 1
    df["duration"] = df["time_end"] - df["time_begin"]
    df["shot"] = 0

    # TODO: allow multiple shot providers

    # build look-up table for all shots by second
    UPSAMPLE_TIME = 4  # how many map intervals per second?
    shot_timing = range(int(math.ceil(df["time_begin"].max())) * UPSAMPLE_TIME)  # create index
    # for speed, we generate a mapping that can round the time into a reference...
    #   [t0, t1, t2, t3, ....] and use that mapping for duration and shot id
    shot_lookup = None
    shot_duration = None
    for idx_shots, df_shots in df[df["tag_type"]=="shot"].groupby("extractor"):
        if shot_lookup is not None:
            break
        shot_lookup = []
        shot_duration = []
        # logger.info(f"Generating shot mapping from type '{df_shots['extractor'][0]}'...")
        df_shots = df_shots.sort_values("time_begin")
        idx_shot = 0
        for row_idx, row_shot in df_shots.iterrows():
            ref_shot = int(math.floor(row_shot["time_begin"] * UPSAMPLE_TIME))
            # print("check", ref_shot, idx_shot, row_shot["duration"])
            while ref_shot >= len(shot_duration):
                # print("push", ref_shot, idx_shot, row_shot["duration"])
                shot_lookup.append(idx_shot)
                shot_duration.append(row_shot["duration"])
            idx_shot += 1
    ref_shot = int(math.floor(df["time_begin"].max() * UPSAMPLE_TIME))
    while ref_shot >= len(shot_duration):   # extend the mapping array until max time
        shot_lookup.append(shot_duration[-1])
        shot_duration.append(shot_duration[-1])

    df["shot"] = df["time_begin"].apply(lambda x: shot_lookup[int(math.floor(x * UPSAMPLE_TIME))])     # now map the time offset 
    df["duration"] = df["time_begin"].apply(lambda x: shot_duration[int(math.floor(x * UPSAMPLE_TIME))])     # now map the time offset 

    # default with tag for keywords
    df.loc[df["tag_type"]=="word", "details"] = df[df["tag_type"]=="word"]
    if NLP_TOKENIZE:
        # extract/add NLP tags from transcripts
        ux_report.info(f"... detecting NLP-based textual entities....")
        ux_progress.progress(math.floor(float(task_idx)/task_count*100))
        task_idx += 1
        try:
            import spacy
        except Exception as e:
            logger.critical("Missing `spacy`? Consider installing the library (`pip install spacy`) and data model once more. (e.g. python -m spacy download en_core_web_sm)")
        # models - https://spacy.io/models/en 
        # execute -- python -m spacy download en_core_web_sm
        # https://github.com/explosion/spacy-models/releases/download/en_core_web_md-2.2.5/en_core_web_md-2.2.5.tar.gz
        nlp = spacy.load('en_core_web_sm')
        list_new = []
        df_sub = df[df["tag_type"]=="transcript"]
        idx_sub = 0

        for row_idx, row_transcript in df_sub.iterrows():
            ux_report.info(f"... detecting NLP-based textual entities ({idx_sub}/{len(df_sub)})....")
            # https://spacy.io/usage/linguistic-features#named-entities
            detail_obj = json.loads(row_transcript['details'])
            idx_sub += 1
            if "transcript" in detail_obj:
                for entity in nlp(detail_obj["transcript"]).ents:
                    row_new = row_transcript.to_dict()
                    row_new["details"] = entity.label_
                    row_new["tag"] = entity.text
                    row_new["tag_type"] = "entity"
                    list_new.append(row_new)
        ux_report.info(f"... integrating {len(list_new)} new text entities....")
        df_entity = pd.DataFrame(list_new)
        df = pd.concat([df, df_entity], sort=False)
        list_new = None
        df_entity = None

        # Create list of word tokens
        ux_report.info(f"... filtering text stop words....")
        ux_progress.progress(math.floor(float(task_idx)/task_count*100))
        task_idx += 1
        # from spacy.lang.en.stop_words import STOP_WORDS
        list_new = df[df["tag_type"]=="word"]["tag"].unique()
        map_new = {}
        re_clean = re.compile(r"[^0-9A-Za-z]")
        for idx_sub in range(len(list_new)):
            ux_report.info(f"... filtering text stop words ({idx_sub}/{len(list_new)})....")
            word_new = nlp(list_new[idx_sub])
            map_new[list_new[idx_sub]] = re_clean.sub('', word_new.text.lower()) if not nlp.vocab[word_new.text].is_stop else NLP_STOPWORD
        # now map to single array of mapping
        df.loc[df["tag_type"]=="word", "details"] =  df[df["tag_type"]=="word"]["tag"].apply(lambda x: map_new[x])

    ux_report.info(f"... normalizing time signatures...")
    ux_progress.progress(math.floor(float(task_idx)/task_count*100))
    task_idx += 1
    for tf in ["time_event", "time_begin", "time_end"]:  # convert to pandas time (for easier sampling)
        if False:
            df[tf] = df[tf].apply(lambda x: pd.Timestamp('2010-01-01T00') + pd.Timedelta(x, 'seconds'))
        else:
            df[tf] = pd.to_timedelta(df[tf], unit='s')
            df[tf].fillna(pd.Timedelta(seconds=0), inplace=True)

    ux_report.info(f"... pruning duplicates from {len(df)} events...")
    ux_progress.progress(math.floor(float(task_idx)/task_count*100))
    df.drop_duplicates(inplace=True)
    task_idx += 1
    
    ux_report.info(f"... sorting and indexing {len(df)} events...")
    ux_progress.progress(math.floor(float(task_idx)/task_count*100))
    task_idx += 1
    df.sort_values(["time_begin", "time_end"], inplace=True)
    df.set_index("time_event", drop=True, inplace=True)

    # extract faces (emotion)

    ux_report.info(f"... loaded {len(df)} rows across {len(list_files)} files.")
    ux_report.empty()
    ux_progress.empty()

    # save new data file before returning
    df.to_pickle(path_new)
    return df