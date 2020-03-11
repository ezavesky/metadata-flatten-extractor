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
from os import path, system, unlink
from pathlib import Path
import re
import hashlib
import glob
import math
import json
from time import sleep

import altair as alt
from sklearn.neighbors import BallTree
import streamlit as st

import logging
import warnings
from sys import stdout as STDOUT


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
NLP_TOKENIZE = True
NLP_STOPWORD = "_stopword_"

TOP_HISTOGRAM_N = 15   # max number of elements to show in histogram
TOP_LINE_N = 5   # max number of samples to show in timeseries plot
MIN_INSIGHT_COUNT = 3   # min count for samples in 'insight' viewing (e.g. brand, text, event)
NLP_FILTER = 0.025   # if stop word analysis not ready, what is HEAD/TAIL trim for frequencyy?
SAMPLE_TABLE = 100   # how many samples go in the dataframe table dump
MAX_LOCK_COUNT = 3   # how many lock loops shoudl we wait (for labels)

UPSAMPLE_TIME = 4  # how many map intervals per second? when grouping shots

DEFAULT_REWIND = 2   # how early to start clip from max score (sec)
DEFAULT_CLIPLEN = 5   # length of default cllip (sec)
DEFAULT_REWIND_FRAME = -0.25   # rewind for frame-specific starts

ALTAIR_DEFAULT_WIDTH = 660   # width of charts
ALTAIR_DEFAULT_HEIGHT = 220   # height of charts
ALTAIR_SIDEBAR_WIDTH = 280   # width of charts (in sidebar)
ALTAIR_SIDEBAR_HEIGHT = 180   # height of charts (in sidebar)

LABEL_TEXT = ['Invalid', 'Unverified', 'Valid']  # -1, 0, 1 for labeling interface
LABEL_AS_CSV = False   # save labels as CSV or pkl?

### ------------ dataframe and chart functions ---------------------

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
    df_sub = aggregate_tags(df_live, tag_type, field_group)
    if len(df_sub) == 0:
        st.markdown("*Sorry, the active filters removed all events for this display.*")
        return None
    # unfortunately, a bug currently overrides sort order
    if show_hist:
        quick_sorted_barchart(df_sub, field_group)
    return df_sub


def quick_timeseries(df_live, df_sub, tag_type, graph_type='line'):
    """Helper function to draw a timeseries for a few top selected tags..."""
    if df_sub is None:
        return
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

### ------------ util/format ---------------------

def timedelta_str(timedelta, format="%H:%M:%S"):
    """Create timedelta string with datetime formatting"""
    dt_print = pd.Timestamp("2020-01-01T00:00:00", tz="UTC") + timedelta
    return dt_print.strftime(format)


def data_query(tree_query, tree_shots, shot_input, num_neighbors=5, exclude_input_shots=True):
    """Method to query for samples against a created tree/index

    :param tree_query: (dict): Hot-indexed kNN object (e.g. BallTree from `data_index`) 
    :param tree_shots: (list): The returned index/map from the absolute position in the tree to known/input shots
    :param shot_input: (list): Known shots to use for query (they will be mapped with `tree_shots`)
    :param num_neighbors: (int): Max number of neighbors to return after query
    :param exclude_input_shots: (bool): Should items from `shot_input` be excluded from restuls?
    :return: (DataFrame): dataframe of `shot` and `score` resulting from tree query

    """
    # NOTE: filter by those active shots that have query samples
    #       we have to copy into a new array because memory views don't let us index randomly
    # e.g. tree_query.data[[0,1,4,5], :]
    #   https://www.programiz.com/python-programming/methods/built-in/memoryview
    data_query = None
    for raw_shot in shot_input:
        if raw_shot in tree_shots:  # need to deref to index in memory view
            if data_query is None:
                data_query = tree_query.data[tree_shots.index(raw_shot)]
            else:
                data_query = np.vstack((data_query, tree_query.data[tree_shots.index(raw_shot), :]))
    if len(data_query.shape) == 1:   # fix if we're querying with just a single sample
        data_query = np.vstack((data_query, ))
   
    # execute query, but pad with length of our query
    query_dist, query_ind = tree_query.query(data_query, num_neighbors + data_query.shape[0])
    # massage results into an easy to handle dataframe
    df_expand = pd.DataFrame(np.vstack([np.hstack(query_dist), np.hstack(query_ind)]).T, columns=['distance', 'shot'])
    df_expand["shot"] = df_expand["shot"].apply(lambda x: int(tree_shots[int(x)]))   # map back from query/tree
    # exclude input shots from neighbors (yes, they will always be there otherwise!)
    if exclude_input_shots:
        df_expand = df_expand[~df_expand["shot"].isin(shot_input)]
    # finally, group by shot number, push it to column, sort by best score
    return df_expand.groupby('shot').max() \
                    .reset_index(drop=False) \
                    .sort_values('distance', ascending=True)


### ------------ content functions ---------------------

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


def clip_display(df_live, df, media_file, field_group="tag", label_dir=None, df_label=None):
    """Create visual for video or image with selection of specific instance"""
    list_sel = []
    for idx_r, val_r in df_live.iterrows():   # make it something readable
        time_duration_sec = float(val_r["duration"])  # use shot duration
        str_duration = "" if time_duration_sec < 0.1 else f"({round(time_duration_sec, 2)}s)"
        list_sel.append(f"{len(list_sel)} - (score: {round(val_r['score'],4)}) " \
            f"@ {timedelta_str(val_r['time_begin'])} {str_duration} ({val_r[field_group]})")
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
        f"{timedelta_str(row_first['time_begin'][0])} ({round(time_duration_sec, 2)}s)*"

    _, clip_ext = path.splitext(path.basename(media_file))
    media_clip = path.join(path.dirname(media_file), "".join(["temp_clip", clip_ext]))
    media_image = path.join(path.dirname(media_file), "temp_thumb.jpg")

    if st.button("Play Clip", key=f"{field_group}_{time_begin_sec}"):
        status = clip_video(media_file, media_clip, int(time_begin_sec-DEFAULT_REWIND), time_duration_sec)
        if status == 0: # play clip
            st.video(open(media_clip, 'rb'))
            st.markdown(caption_str)
        elif status == -1:
            st.markdown("**Error:** ffmpeg not found in path. Cannot create video clip.")
        else:
            st.markdown("**Error:** creating video clip.")
    else:       # print thumbnail
        media_data = clip_media(media_file, media_image, time_event_sec-DEFAULT_REWIND_FRAME)
        if media_data is not None:
            st.image(media_data, use_column_width=True, caption=caption_str)

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
def data_load(stem_datafile, data_dir, allow_cache=True, ignore_update=False):
    """Because of repetitive loads in streamlit, a method to read/save cache data according to modify time."""
    # generate a checksum of the input files
    m = hashlib.md5()
    list_files = []
    for filepath in sorted(Path(data_dir).rglob(f'csv_flatten_*.csv*')):
        list_files.append(filepath)
        m.update(str(filepath.stat().st_mtime).encode())
    for filepath in sorted(Path(data_dir).rglob(f'flatten_*.csv*')):   # keep for legacy file discovery  (as of v0.8)
        list_files.append(filepath)
        m.update(str(filepath.stat().st_mtime).encode())

    path_backup = None
    for filepath in Path(data_dir).glob(f'{stem_datafile}.*.pkl.gz'):
        path_backup = filepath
        break

    if not list_files and path_backup is None:
        logger.error(f"Sorry, no flattened or cached files found, check '{data_dir}'...")
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
    shot_timing = range(int(math.ceil(df["time_begin"].max())) * UPSAMPLE_TIME)  # create index
    # for speed, we generate a mapping that can round the time into a reference...
    #   [t0, t1, t2, t3, ....] and use that mapping shot id (update 2/16/20 - don't overwrite "duration" field)
    shot_lookup = None
    for idx_shots, df_shots in df[df["tag_type"]=="shot"].groupby("extractor"):
        if shot_lookup is not None:
            break
        shot_lookup = []
        # logger.info(f"Generating shot mapping from type '{df_shots['extractor'][0]}'...")
        df_shots = df_shots.sort_values("time_begin")
        idx_shot = 0
        for row_idx, row_shot in df_shots.iterrows():
            ref_shot = int(math.floor(row_shot["time_begin"] * UPSAMPLE_TIME))
            # print("check", ref_shot, idx_shot, row_shot["duration"])
            while ref_shot >= len(shot_lookup):
                shot_lookup.append(idx_shot)
            idx_shot += 1
    ref_shot = int(math.floor(df["time_begin"].max() * UPSAMPLE_TIME))
    while ref_shot >= len(shot_lookup):   # extend the mapping array until max time
        shot_lookup.append(shot_lookup[-1])

    df["shot"] = df["time_begin"].apply(lambda x: shot_lookup[int(math.floor(x * UPSAMPLE_TIME))])     # now map the time offset 

    # default with tag for keywords
    df.loc[df["tag_type"]=="word", "details"] = df[df["tag_type"]=="word"]
    if len(df[df["tag_type"]=="word"]) == 0:   # didn't have word source
        df_enhance = df[df["tag_type"]=="transcript"]
        if len(df_enhance) != 0:   # pull it from transcript
            df_enhance = df_enhance.copy()
            df_enhance["details"] = df_enhance["details"].apply(lambda x: json.loads(x)['transcript'])
            ux_report.info(f"Extracting words from transcripts, input samples {len(df_enhance)}, attempting to split")
            pass
        elif len(df[df["tag_type"]=="keyword"]) != 0:   # pull it from keyword
            df_enhance = df[df["tag_type"]=="keyword"].copy()
            df_enhance["details"] = df_enhance["tag"]
            ux_report.info(f"Extracting words from keywords, input samples {len(df_enhance)}, attempting to split")
            pass
        list_append = []
        idx_search = 0
        re_clean = re.compile(r"[^0-9A-Za-z]+")
        for row_idx, row_enhance in df_enhance.iterrows():
            list_text = re_clean.split(row_enhance["details"].lower())
            for cur_word in list_text:
                if len(cur_word) > 1:
                    row_copy = row_enhance.copy()
                    row_copy["details"] = cur_word
                    list_append.append(row_copy)
        if len(list_append):
            df_append = pd.DataFrame(list_append)
            df_append["tag_type"] = "word"
            df_append["tag"] = df_append["details"]
            ux_report.info(f"Extracting {len(df_append)} new words for analysis")
            df = df.append(df_append)

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

@st.cache(suppress_st_warning=True, allow_output_mutation=True)
def data_index(stem_datafile, data_dir, df, allow_cache=True):
    """A method to convert raw dataframe into vectorized features and return a hot index for query.
    Returns:
        [BallTree (sklearn.neighbors.BallTree), [shot0, shot1, shot2]] - indexed tree and list of shot ids that correspond to tree's memory view
    """

    # generate a checksum of the input files
    m = hashlib.md5()
    for data_time in sorted(df["time_begin"].unique()):
        m.update(str(data_time).encode())

    path_backup = None
    for filepath in Path(data_dir).glob(f'{stem_datafile}.*.pkl.gz'):
        path_backup = filepath
        break

    # NOTE: according to this article, we should use 'feather' but it has depedencies, so we use pickle
    # https://towardsdatascience.com/the-best-format-to-save-pandas-data-414dca023e0d
    path_new = path.join(data_dir, f"{stem_datafile}.{m.hexdigest()[:8]}.pkl.gz")
    ux_report = st.empty()
    
    # see if checksum matches the datafile (plus stem)
    if allow_cache and (path.exists(path_new) or path_backup is not None):
        if path.exists(path_new):  # if so, load old datafile, skip reload
            df = pd.read_pickle(path_new)
            ux_report.info(f"... building live index on features...")
            tree = BallTree(df)
            ux_report = st.empty()
            return tree, list(df.index)
        elif len(list_files) == 0 or ignore_update:  # only allow backup if new files weren't found
            st.warning(f"Warning: Using datafile `{path_backup.name}` with no grounded reference.  Version skew may occur.")
            df = pd.read_pickle(path_backup)
            ux_report.info(f"... building live index on features...")
            tree = BallTree(df)
            ux_report = st.empty()
            return tree, list(df.index)
        else:   # otherwise, delete the old backup
            unlink(path_backup.resolve())
    
    # time_init = pd.Timestamp('2010-01-01T00')  # not used any more
    ux_progress = st.empty()
    ux_report.info(f"Data has changed, regenerating core data bundle file {path_new}...")

    # Add a placeholder
    latest_iteration = st.empty()
    ux_progress = st.progress(0)
    task_buffer = 4   # account for pivot, index, duration
    task_idx = 0

    re_encode = re.compile(r"[^0-9a-zA-Z]")

    list_pivot = []
    tuple_groups = df.groupby(["tag", "tag_type"])   # run once but get length for progress bar
    task_count = len(tuple_groups)+task_buffer
    num_group = 0
    ux_progress.progress(math.floor(task_idx/task_buffer*100))

    # average by shot
    # tuple_groups = df.groupby(["shot"])   # run once but get length for progress bar
    # for idx_shot, df_shots in tuple_groups:
    #     ux_report.info(f"... averaging {num_group}/{len(tuple_groups)} tag/tag_type shots...")
    #     # print(df_mean, idx_shot, idx_group)

    #     # group by tag_type, tag
    #     for idx_group, df_group in df_shots.groupby(["tag", "tag_type"]):
    #         list_pivot.append([idx_shot, re_encode.sub('_', '_'.join(idx_group)), df_group["score"].mean()])
    #     num_group += 1

    # group by tag_type, tag
    for idx_group, df_group in tuple_groups:
        if (num_group % 100) == 0:
            ux_report.info(f"... vectorizing {num_group}/{len(tuple_groups)} tag/tag_type shots...")
        # average by shot
        df_mean = df_group.groupby('shot')['score'].mean().reset_index(drop=False)
        df_mean["tag"] = re_encode.sub('_', '_'.join(idx_group))
        list_pivot += list(df_mean.values)
        num_group += 1

    # pivot to make a shot-wise row view
    task_idx += 1
    tuple_groups = None
    ux_progress.progress(math.floor(task_idx/task_buffer*100))
    ux_report.info(f"... pivoting table for score reporting...")
    df_vector_raw = pd.DataFrame(list_pivot, columns=["shot", "score", "tag"])
    df_vector = pd.pivot_table(df_vector_raw, index=["shot"], values="score", columns=["tag"], fill_value=0).sort_index()
    df_vector.index = df_vector.index.astype(int)
    df_vector_raw = None

    # append duration
    task_idx += 1
    ux_progress.progress(math.floor(task_idx/task_buffer*100))
    ux_report.info(f"... linking shot duration to vectors...")
    df_sub = df[df["tag_type"]=="shot"][["shot", "duration", "score"]].set_index("shot", drop=True)
    df_vector = df_vector.join(df_sub["duration"])  # grab duration from original data
    df_sub = None

    # train new hot-index object for fast kNN query
    # https://scikit-learn.org/stable/modules/generated/sklearn.neighbors.BallTree.html#sklearn.neighbors.BallTree.query
    task_idx += 1
    ux_progress.progress(math.floor(task_idx/task_buffer*100))
    ux_report.info(f"... building live index on features...")
    tree = BallTree(df_vector)

    ux_report.empty()
    ux_progress.empty()

    # save new data file before returning
    df_vector.to_pickle(path_new)
    return tree, list(df_vector.index)


def data_label_serialize(data_dir, df_new=None, label_new=None):
    """Method to load labels and append them to the primary data frame

    :param stem_datafile: (str): Stem for active label files
    :param data_dir: (str): Absolute/relative path for label file
    :param label_new: (int): Label for row (-1=false, 1=true, 0=unknown)
    :return bool: True/False on success of save
    """
    if LABEL_AS_CSV:
        path_new = path.join(data_dir, f"data_labels.csv.gz")
        path_lock = path.join(data_dir, f"data_labels.LOCK.csv.gz")
    else:
        path_new = path.join(data_dir, f"data_labels.pkl.gz")
        path_lock = path.join(data_dir, f"data_labels.LOCK.pkl.gz")
    ux_report = st.empty()
    if df_new is None or label_new is None:
        if path.exists(path_new):
            if LABEL_AS_CSV:
                df = pd.read_csv(path_new)
                df["time_begin"] = pd.to_timedelta(df["time_begin"])
                df["label"] = df["label"].astype(int)
            else: 
                df = pd.read_pickle(path_new)
            return df
        ux_report.warning(f"Warning, label file `{path_new}` is not found (ignore this on first runs)!")
        return None
    num_lock = 0
    while path.exists(path_lock):  # if so, load old datafile, skip reload
        num_lock += 1
        if num_lock > MAX_LOCK_COUNT:
            ux_report.error(f"Label file `{path_new}` is permanently locked, please clear the file or ask for help!")
            logger.error(f"Label file `{path_new}` is permanently locked, please clear the file or ask for help!")
            return False
        ux_report.warning(f"Label file `{path_new}` is temporarily locked, retry {num_lock} momentarily...")
        sleep(2)  # sleep a couple of seconds...
    ux_report.info("Writing new label...")
    ts_now = pd.Timestamp.now()
    with open(path_lock, 'wt') as f:
        f.write(str(ts_now))
    col_primary = ["time_begin", "tag_type", "tag", "extractor"]
    df = pd.DataFrame([], columns=col_primary)
    if path.exists(path_new):
        if LABEL_AS_CSV:
            df = pd.read_csv(path_new)
            df["time_begin"] = pd.to_timedelta(df["time_begin"])
            df["label"] = df["label"].astype(int)
        else: 
            df = pd.read_pickle(path_new)
    df_new = df_new[col_primary].copy()
    df_new["timestamp"] = ts_now  # add new timestamp (now)
    df_new["label"] = int(label_new)   # add new label
    df = pd.concat([df_new, df], sort=False, ignore_index=True).drop_duplicates(col_primary)  # drop duplicate labels
    if LABEL_AS_CSV:
        df.to_csv(path_new, index=False)
    else:
        df.to_pickle(path_new)
    ux_report.empty()
    unlink(path_lock)
    return True
