# Imports
import streamlit as st
import pandas as pd
import numpy as np
import ast
from os import path
from pathlib import Path
import re
import hashlib
import glob
import math
import json

data_dir = path.join("..", "results")
version_path = path.join("..", "_version.py")
re_issue = re.compile(r"[^0-9A-Za-z]+")
presence_bars = False  # toggle to show presence indicators as a graph

import logging
import warnings
from sys import stdout as STDOUT

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
NLP_TOKENIZE = True
TOP_HISTOGRAM_N = 20
SAMPLE_N = 100

def main_page():
    # read in version information
    version_dict = {}
    with open(version_path) as file:
        exec(file.read(), version_dict)

    st.title(version_dict['__description__']+" Explorer")
    ux_report = st.empty()
    ux_progress = st.empty()

    df = data_load("data_bundle", True)
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

    # frequency bar chart for found labels / tags
    st.markdown("### popular visual tags")
    df_sub = df_live[df_live["tag_type"]=="tag"].groupby("tag").count().reset_index(drop=False).set_index("shot").sort_index(ascending=False)
    df_sub.index.name = "count"
    st.bar_chart(df_sub["tag"].head(TOP_HISTOGRAM_N))

    # time chart of top N 
    # tag_top = list(df_sub["tag"].head(5))
    # df_sub = df_live[(df_live["tag_type"]=="tag") & (df_live["tag"].isin(tag_top))]

    # frequency bar chart for types of faces

    # frequency bar chart for keywords
    st.markdown("### popular textual keywords")
    df_sub = df_live[df_live["tag_type"]=="word"].groupby("details").count().reset_index(drop=False).set_index("shot").sort_index(ascending=False)
    if not NLP_TOKENIZE or len(df_sub) < 5:   # old method before NLP stop word removal
        df_sub = df_live[df_live["tag_type"]=="word"].groupby("tag").count().reset_index(drop=False).set_index("shot").sort_index(ascending=False)
        df_sub = df_sub.iloc[math.floor(len(df_sub) * 0.03):]
        num_clip = df_sub.head(1).index[0]
        st.markdown(f"*Note: The top 3% of scoring events (more than {num_clip} instances) have been dropped.*")
    else:
        st.markdown(f"*Note: Results after stop word removal.*")
        df_sub.rename({"details":"tag"})
    st.bar_chart(df_sub["tag"].head(TOP_HISTOGRAM_N))

    # frequency bar chart for logos
    st.markdown("### popular logos")
    df_sub = df_live[df_live["tag_type"]=="logo"].groupby("tag").count().reset_index(drop=False).set_index("shot").sort_index(ascending=False)
    df_sub.index.name = "count"
    st.bar_chart(df_sub["tag"].head(TOP_HISTOGRAM_N))

    # frequency bar chart for emotions

    # frequency bar chart for celebrities
    st.markdown("### popular celebrities")
    df_sub = df_live[df_live["tag_type"]=="identity"].groupby("tag").count().reset_index(drop=False).set_index("shot").sort_index(ascending=False)
    df_sub.index.name = "count"
    st.bar_chart(df_sub["tag"].head(TOP_HISTOGRAM_N))
    

    # df_poptag = df_live.groupby(["tag", "source_event"]).count().sort_values(by="source_event", ascending=False)
    # logger.info(df_poptag.head(10))


    # st.write(df_live.groupby("tag")["source_event"].count())
    # st.write(df_live.groupby("source_event").count())
    # st.write(df_live.resample("30S").groupby("tag").count())

    # hist_event = df.hist(column="tim  e_event", bins=100)
    # st.write(hist_event)
    st.markdown("### filtered exploration (100 events)")
    filter_tag = st.selectbox("Inspect Tag Type", ["All"] + list(df_live["tag_type"].unique()))
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


@st.cache(suppress_st_warning=True, allow_output_mutation=True)
def data_load(stem_datafile, allow_cache=True):
    """Because of repetitive loads in streamlit, a method to read/save cache data according to modify time."""

    # generate a checksum of the input files
    m = hashlib.md5()
    list_files = []
    for filepath in Path(data_dir).rglob(f'*.csv*'):
        list_files.append(filepath)
        m.update(str(filepath.stat().st_mtime).encode())

    # NOTE: according to this article, we should use 'feather' but it has depedencies, so we use pickle
    # https://towardsdatascience.com/the-best-format-to-save-pandas-data-414dca023e0d
    path_new = path.join(data_dir, f"{stem_datafile}.{m.hexdigest()[:8]}.pkl.gz")

    # see if checksum matches the datafile (plus stem)
    if allow_cache and path.exists(path_new):
        # if so, load old datafile, skip reload
        return pd.read_pickle(path_new)
    
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
        df_sub = df[df["tag_type"]=="word"]
        list_new = df_sub["details"]
        idx_sub = 0
        re_clean = re.compile(r"[^0-9A-Za-z]")
        for row_idx, row_word in df_sub.iterrows():
            word_new = nlp(row_word["tag"])
            list_new[idx_sub] = re_clean.sub('', word_new.text.lower()) if not nlp.vocab[word_new.text].is_stop else "_stopword_"
            idx_sub += 1
        df.loc[df["tag_type"]=="word", "details"] = list_new

    # extract shot extents
    ux_report.info(f"... mapping shot id to all events....")
    ux_progress.progress(math.floor(float(task_idx)/task_count*100))
    task_idx += 1
    df["duration"] = df["time_end"] - df["time_begin"]
    df["shot"] = 0
    df_sub = df[df["tag"]=="shot"]
    idx_sub = 0
    for row_idx, row_shot in df_sub.iterrows():
        ux_report.info(f"... mapping shot id to all events ({idx_sub}/{len(df_sub)}) for {len(df)} samples....")
        # idx_match = df.loc[row_shot["time_begin"]:row_shot["time_end"]].index   # find events to update
        idx_match = df.loc[(df["time_begin"] >= row_shot["time_begin"])
                            & (df["time_end"] < row_shot["time_end"])].index   # find events to update
        df.loc[idx_match, "duration"] = row_shot["duration"]
        df.loc[idx_match, "shot"] = idx_sub
        idx_sub += 1

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
    delta_range = (pd.to_timedelta(time_bound[0], unit='min'),
                    pd.to_timedelta(time_bound[1], unit='min'))
    df_filter_time = df.loc[delta_range[0]:delta_range[1]]
    df_filter = df_filter_time[
                    (df_filter_time['duration'] >= duration_bound[0]) 
                    & (df_filter_time['duration'] <= duration_bound[1])
                    & (df_filter_time['score'] >= score_bound[0]) 
                    & (df_filter_time['score'] <= score_bound[1])
    ]


    # hard work done, return the trends!
    if sort_list is None:
        return df_filter
    # otherwise apply sorting right now
    return df_filter.sort_values(by=[v[0] for v in sort_list], 
                                       ascending=[v[1] for v in sort_list])
    



# main block run by code
if __name__ == '__main__':
    main_page()
