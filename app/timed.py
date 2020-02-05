#! python
# ===============LICENSE_START=======================================================
# vinyl-tools Apache-2.0
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
import ast
from os import path, system, unlink
from pathlib import Path
import re
import hashlib
import glob
import math
import json

import altair as alt

version_path = path.join("..", "_version.py")
re_issue = re.compile(r"[^0-9A-Za-z]+")
presence_bars = False  # toggle to show presence indicators as a graph

import logging
import warnings
from sys import stdout as STDOUT

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
NLP_TOKENIZE = True
NLP_STOPWORD = "_stopword_"

TOP_HISTOGRAM_N = 15
TOP_LINE_N = 5
NLP_FILTER = 0.025
SAMPLE_N = 250

DEFAULT_REWIND = 5 # how early to start clip from max score (sec)
DEFAULT_CLIPLEN = 10 # length of default cllip (sec)

ALTAIR_DEFAULT_WIDTH = 660   # width of charts
ALTAIR_DEFAULT_HEIGHT = 300   # height of charts

def main_page(data_dir=None, media_file=None):
    """Main page for execution"""
    # read in version information
    version_dict = {}
    with open(version_path) as file:
        exec(file.read(), version_dict)   
    st.title(version_dict['__description__']+" Explorer")
    ux_report = st.empty()
    ux_progress = st.empty()

    if data_dir is None:
        data_dir = path.join(path.dirname(version_path), "results")
    if media_file is None:
        media_file = path.join(data_dir, "videohd.mp4")

    df = data_load("data_bundle", data_dir, True)
    if df is None:
        st.error("No data could be loaded, please check configuration options.")
        return
    df_live = draw_sidebar(df)

    # Create the runtime info
    st.markdown(f"""<div style="text-align:left; font-size:small; color:#a1a1a1; width=100%;">
                     <span >{version_dict['__package__']} (v {version_dict['__version__']})</span>
                     <span > - {len(df_live)} events</span></div>""", unsafe_allow_html=True)
    if len(df_live) < SAMPLE_N:
        st.markdown("## Too few samples")
        st.markdown("The specified filter criterion are too rigid. Please modify your exploration and try again.")
        return

    # logger.info(list(df.columns))

    @st.cache(suppress_st_warning=False)
    def _aggregate_tags(df_live, tag_type, field_group="tag"):
        df_sub = df_live[df_live["tag_type"]==tag_type].groupby(field_group)["score"] \
                    .agg(['count', 'mean', 'max', 'min']).reset_index(drop=False) \
                    .sort_values(["count", "mean"], ascending=False)
        df_sub[["mean", "min", "max"]] = df_sub[["mean", "min", "max"]].apply(lambda x: round(x, 3))
        return df_sub

    def _quick_sorted_barchart(df_sub):
        """Create bar chart (e.g. histogram) with no axis sorting..."""
        if False:   #  https://github.com/streamlit/streamlit/issues/385
            st.bar_chart(df_sub["tag"].head(TOP_HISTOGRAM_N))
        else:
            # https://github.com/streamlit/streamlit/blob/5e8e0ec1b46ac0b322dc48d27494be674ad238fa/lib/streamlit/DeltaGenerator.py
            st.write(alt.Chart(df_sub.head(TOP_HISTOGRAM_N), width=ALTAIR_DEFAULT_WIDTH, height=ALTAIR_DEFAULT_HEIGHT).mark_bar().encode(
                x=alt.X('count', sort=None),
                y=alt.Y('tag', sort=None),
                tooltip=['tag', 'count', 'mean', 'min']
            ))


    def quick_hist(df_live, tag_type, show_hist=True, field_group="tag"):
        """Helper function to draw aggregate histograms of tags"""
        df_sub = _aggregate_tags(df_live, tag_type, field_group)
        if len(df_sub) == 0:
            st.markdown("*Sorry, the active filters removed all events for this display.*")
            return None
        # unfortunately, a bug currently overrides sort order
        if show_hist:
            _quick_sorted_barchart(df_sub)
        return df_sub

    def quick_timeseries(df_live, df_sub, tag_type, use_line=True):
        """Helper function to draw a timeseries for a few top selected tags..."""
        if df_sub is None:
            return
        add_tag = st.selectbox("Additional Timeline Tag", list(df_live[df_live["tag_type"]==tag_type]["tag"].unique()))
        tag_top = list(df_sub["tag"].head(TOP_LINE_N)) + [add_tag]

        df_sub = df_live[(df_live["tag_type"]==tag_type) & (df_live["tag"].isin(tag_top))]    # filter top
        df_sub = df_sub[["tag", "score"]]   # select only score and tag name
        df_sub.index = df_sub.index.round('1T')
        list_resampled = [df_sub[df_sub["tag"] == n].resample('1T', base=0).mean()["score"] for n in tag_top]   # resample each top tag
        df_scored = pd.concat(list_resampled, axis=1).fillna(0)
        df_scored.columns = tag_top
        df_scored.index = df_scored.index.seconds // 60
        if use_line:
            st.line_chart(df_scored)
        else:
            st.area_chart(df_scored)

    def clip_video(media_file, media_output, start, duration=1, image_only=False):
        """Helper function to create video clip"""
        if path.exists(media_output):
            unlink(media_output)
        if (system("which ffmpeg")==0):  # check if ffmpeg is in path
            if not image_only:
                return system(f"ffmpeg -ss {start} -i {media_file} -t {duration} -c copy {media_output}")
            else: 
                # TODO: do we allow force of an aspect ratio for bad video transcode?  e.g. -vf 'scale=640:360' 
                return system(f"ffmpeg  -ss {start} -i {media_file} -r 1 -t 1 -f image2 {media_output}")  
        else:
            return -1

    st.markdown("## high-frequency content")

    # frequency bar chart for found labels / tags
    st.markdown("### popular visual tags")
    df_sub = quick_hist(df_live, "tag")  # quick tag hist
    quick_timeseries(df_live, df_sub, "tag")      # time chart of top N 

    # frequency bar chart for types of faces

    # frequency bar chart for keywords
    st.markdown("### popular textual keywords")
    df_sub = _aggregate_tags(df_live, "word", "details")
    if not NLP_TOKENIZE or len(df_sub) < 5:   # old method before NLP stop word removal
        df_sub = _aggregate_tags(df_live, "word", "tag")
        df_sub = df_sub.iloc[math.floor(len(df_sub) * NLP_FILTER):]
        num_clip = list(df_sub["count"])[0]
        st.markdown(f"*Note: The top {round(NLP_FILTER * 100, 1)}% of most frequent events (more than {num_clip} instances) have been dropped.*")
    else:
        st.markdown(f"*Note: Results after stop word removal.*")
        df_sub.rename(columns={"details":"tag"}, inplace=True)
        df_sub = df_sub[(df_sub["tag"] != NLP_STOPWORD) & (df_sub["tag"] != "")]
    _quick_sorted_barchart(df_sub)

    # frequency bar chart for found labels / tags
    st.markdown("### popular textual named entities")
    df_sub = quick_hist(df_live, "entity")  # quick tag hist

    # frequency bar chart for brands
    st.markdown("### popular brands")
    df_sub = quick_hist(df_live, "brand")
    quick_timeseries(df_live, df_sub, "brand", False)      # time chart of top N 

    # frequency bar chart for emotions

    # frequency bar chart for celebrities
    st.markdown("### popular celebrities")
    df_sub = quick_hist(df_live, "identity")
    quick_timeseries(df_live, df_sub, "identity", False)      # time chart of top N 
    
    # frequency bar chart for celebrities
    st.markdown("### moderation events timeline")
    df_sub = quick_hist(df_live, "moderation", False)
    quick_timeseries(df_live, df_sub, "moderation", False)      # time chart of top N 
    
    # TODO: shot length distribution?

    # plunk down a dataframe for people to explore as they want
    st.markdown(f"## filtered exploration ({SAMPLE_N} events)")
    filter_tag = st.selectbox("Tag Type for Exploration", ["All"] + list(df_live["tag_type"].unique()))
    order_tag = st.selectbox("Sort Metric", ["random", "score - descending", "score - ascending", "time_begin", "time_end", 
                                         "duration - ascending", "duration - descending"])
    order_ascend = order_tag.split('-')[-1].strip() == "ascending"  # eval to true/false
    order_sort = order_tag.split('-')[0].strip()
    df_sub = df_live
    if filter_tag != "All":
        df_sub = df_live[df_live["tag_type"]==filter_tag]
    if order_tag == "random":
        df_sub = df_sub.sample(SAMPLE_N)
    else:
        df_sub = df_sub.sort_values(order_sort, ascending=order_ascend).head(SAMPLE_N)
    st.write(df_sub)

    st.markdown(f"## clip replay")

    if media_file is None or not path.exists(media_file):
        st.markdown(f"*Media file `{media_file}` not found or readable, can not generate clips.*")
    else:        
        st.markdown(f"### celebrity clips")
        _, clip_ext = path.splitext(path.basename(media_file))
        media_clip = path.join(path.dirname(media_file), "".join(["temp_clip", clip_ext]))
        media_image = path.join(path.dirname(media_file), "temp_thumb.jpg")

        df_celeb = df_live[df_live["tag_type"]=="identity"] 
        celebrity_tag = st.selectbox("Celebrity", list(df_celeb["tag"].unique())) 
        # sor to find the best scoring, shortest duration clip
        df_celeb_sel = df_celeb[df_celeb["tag"]==celebrity_tag].sort_values(["score", "duration"], ascending=[False, True])
        # get begin_time with max score for selected celeb, convert to seconds
        row_first = df_celeb_sel.head(1)
        time_begin_sec = int(row_first['time_begin'] / np.timedelta64(1, 's'))
        time_duration_sec = int(row_first["duration"])  # use shot duration
        if time_duration_sec < DEFAULT_CLIPLEN:   # min clip length
            time_duration_sec = DEFAULT_CLIPLEN
        time_str = str(row_first['time_begin'][0])

        if st.button("Play Clip"):
            status = clip_video(media_file, media_clip, int(time_begin_sec-DEFAULT_REWIND), time_duration_sec)
            if status == 0: # play clip
                st.video(open(media_clip, 'rb'))
                st.markdown(f"*Celebrity: {celebrity_tag} (score: {row_first['score'][0]}) @ {time_str} ({round(time_duration_sec, 2)}s)*")
            elif status == -1:
                st.markdown("**Error:** ffmpeg not found in path. Cannot create video clip.")
            else:
                st.markdown("**Error:** creating video clip.")
        else:       # print thumbnail
            status = clip_video(media_file, media_image, time_begin_sec, image_only=True)
            if status == 0:
                st.image(media_image, use_column_width=True,
                        caption=f"Celebrity: {celebrity_tag} (score: {row_first['score'][0]}) @ {time_str}")


@st.cache(suppress_st_warning=True, allow_output_mutation=True)
def data_load(stem_datafile, data_dir, allow_cache=True):
    """Because of repetitive loads in streamlit, a method to read/save cache data according to modify time."""

    # generate a checksum of the input files
    m = hashlib.md5()
    list_files = []
    for filepath in Path(data_dir).rglob(f'flatten_*.csv*'):
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
        else:
            st.warning(f"Warning: Using datafile `{path_backup.name}` with no grounded reference.  Version skew may occur.")
            return pd.read_pickle(path_backup)
    
    # time_init = pd.Timestamp('2010-01-01T00')  # not used any more
    ux_report = st.empty()
    ux_progress = st.empty()
    ux_report.info(f"Data has changed, regenerating core data bundle file {path_new}...")

    # Add a placeholder
    latest_iteration = st.empty()
    ux_progress = st.progress(0)
    task_buffer = 5   # account for time-norm, sorting, shot-mapping, named entity
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
    for idx_shots, df_shots in df[df["tag"]=="shot"].groupby("extractor"):
        if shot_lookup is not None:
            break
        shot_lookup = []
        shot_duration = []
        print(df_shots.head(0))
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
        df = pd.concat([df, df_entity])
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

    ux_report.info(f"... sorting and indexing....")
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



def draw_sidebar(df, sort_list=None):
    # Generate the slider filters based on the data available in this subset of titles
    # Only show the slider if there is more than one value for that slider, otherwise, don't filter
    st.sidebar.title('Discovery Filters')
    st.sidebar.markdown("<br>", unsafe_allow_html=True)

    # strict timeline slider
    value = (int(df.index.min().seconds // 60), int(df.index.max().seconds // 60))
    time_bound = st.sidebar.slider("Time Range (min)", min_value=value[0], max_value=value[1], value=value)
    st.sidebar.markdown("<br>", unsafe_allow_html=True)

    # extract shot extents (shot length)
    value = (int(df["duration"].min()), int(df["duration"].max()))
    duration_bound = st.sidebar.slider("Shot Duration (sec)", min_value=value[0], max_value=value[1], value=value, step=1)
    st.sidebar.markdown("<br>", unsafe_allow_html=True)

    # confidence measure
    value = (df["score"].min(), df["score"].max())
    score_bound = st.sidebar.slider("Insight Score", min_value=value[0], max_value=value[1], value=value, step=0.01)
    st.sidebar.markdown("<br>", unsafe_allow_html=True)


    # extract faces (emotion)

    # Filter by slider inputs to only show relevant events
    df_filter = df[(df['time_begin'] >= pd.to_timedelta(time_bound[0], unit='min')) 
                    & (df['time_end'] <= pd.to_timedelta(time_bound[1], unit='min'))
                    & (df['duration'] >= duration_bound[0]) 
                    & (df['duration'] <= duration_bound[1])
                    & (df['score'] >= score_bound[0]) 
                    & (df['score'] <= score_bound[1])
    ]


    # hard work done, return the trends!
    if sort_list is None:
        return df_filter
    # otherwise apply sorting right now
    return df_filter.sort_values(by=[v[0] for v in sort_list], 
                                       ascending=[v[1] for v in sort_list])

def main(args=None):
    import argparse
    
    parser = argparse.ArgumentParser(
        description="""A script run the data explorer.""",
        epilog="""Application examples
            # specify the input media file 
            streamlit run timed.py -- -m video.mp4
    """, formatter_class=argparse.RawTextHelpFormatter)
    submain = parser.add_argument_group('main execution')
    submain.add_argument('-d', '--data_dir', dest='data_dir', type=str, default='../results', help='specify the source directory for flattened metadata')
    submain.add_argument('-m', '--media_file', dest='media_file', type=str, default=None, help='specific media file for extracting clips (empty=no clips)')

    if args is None:
        config_defaults = vars(parser.parse_args())
    else:
        config_defaults = vars(parser.parse_args(args))
    print(f"Runtime Configuration {config_defaults}")

    main_page(**config_defaults)


# main block run by code
if __name__ == '__main__':
    main()
