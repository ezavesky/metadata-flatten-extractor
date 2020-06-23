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
import re
import hashlib
import glob
import math
import json
from time import sleep

from sklearn.neighbors import BallTree

import logging

from metadata_flatten import parsers

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
NLP_TOKENIZE = True
NLP_STOPWORD = "_stopword_"

PARSER_BASE_PREFIX = "flatten_"

PATH_BASE_PREFIX = "data_"
PATH_BASE_BUNDLE = f"{PATH_BASE_PREFIX}bundle"
PATH_BASE_LABELS = f"{PATH_BASE_PREFIX}labels"
PATH_BASE_VECTORS = f"{PATH_BASE_PREFIX}vectors"

TOP_HISTOGRAM_N = 15   # max number of elements to show in histogram
TOP_LINE_N = 5   # max number of samples to show in timeseries plot
MIN_INSIGHT_COUNT = 3   # min count for samples in 'insight' viewing (e.g. brand, text, event)
NLP_FILTER = 0.025   # if stop word analysis not ready, what is HEAD/TAIL trim for frequencyy?
SAMPLE_TABLE = 100   # how many samples go in the dataframe table dump
MAX_LOCK_COUNT = 3   # how many lock loops shoudl we wait (for labels)

UPSAMPLE_TIME = 4  # how many map intervals per second? when grouping shots
DEFAULT_SHOT_LEN = 10  # if simulating shots (e.g. none found), what is duration?

DEFAULT_REWIND = 2   # how early to start clip from max score (sec)
DEFAULT_CLIPLEN = 5   # length of default cllip (sec)
DEFAULT_REWIND_FRAME = -0.25   # rewind for frame-specific starts

LABEL_TEXT = ['Invalid', 'Unverified', 'Valid']  # -1, 0, 1 for labeling interface
LABEL_AS_CSV = False   # save labels as CSV or pkl?

### ------------ dataframe and chart functions ---------------------


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


## ---- data load functions --- 

def data_discover_raw(stem_datafile, data_dir, bundle_files=False):
    # generate a checksum of the input files
    m = hashlib.md5()
    list_files = []
    for filepath in sorted(Path(data_dir).rglob(f'csv_{PARSER_BASE_PREFIX}*.csv*')):
        list_files.append(filepath)
        m.update(str(filepath.stat().st_mtime).encode())
    for filepath in sorted(Path(data_dir).rglob(f'{PARSER_BASE_PREFIX}*.csv*')):   # keep for legacy file discovery  (as of v0.8)
        list_files.append(filepath)
        m.update(str(filepath.stat().st_mtime).encode())
    if bundle_files:
        for filepath in sorted(Path(data_dir).rglob(f'{PATH_BASE_PREFIX}*.gz')):   # keep for bundle and data file (as of v0.9.6)
            list_files.append(filepath)
            m.update(str(filepath.stat().st_mtime).encode())

    # NOTE: according to this article, we should use 'feather' but it has depedencies, so we use pickle
    # https://towardsdatascience.com/the-best-format-to-save-pandas-data-414dca023e0d
    path_new = Path(data_dir).joinpath(f"{stem_datafile}.{m.hexdigest()[:8]}.pkl.gz")
    return list_files, path_new


def data_parser_list():
    re_sub = re.compile(f".*{PARSER_BASE_PREFIX}")
    return {re_sub.sub('', x["name"]):x["types"] for x in parsers.get_by_name()}


def data_parse_callback(path_input, extractor_name, fn_callback=None, verbose=True):
    list_parser_modules = parsers.get_by_name(extractor_name)

    run_options = {"verbose": verbose}
    df_output = None

    for parser_obj in list_parser_modules:  # iterate through auto-discovered packages
        parser_instance = parser_obj['obj'](path_input)   # create instance
        df = parser_instance.parse(run_options)  # attempt to process
        if df_output is None:
            df_output = df
        else:
            df_output = pd.concat([df, df_output]).drop_duplicates()
    return df_output


def data_load_callback(stem_datafile, data_dir, allow_cache=True, ignore_update=False, fn_callback=None):
    """Because of repetitive loads in streamlit, a method to read/save cache data according to modify time."""
    list_files, path_new = data_discover_raw(stem_datafile, data_dir)

    path_backup = None
    for filepath in Path(data_dir).glob(f'{stem_datafile}.*.pkl.gz'):
        path_backup = filepath
        break

    if not list_files and path_backup is None:
        str_fail = f"Sorry, no flattened or cached files found, check '{data_dir}'..."
        if fn_callback is not None:
            fn_callback(str_fail)
        logger.error(str_fail)
        return None 

    # see if checksum matches the datafile (plus stem)
    if allow_cache and (path_new.exists() or path_backup is not None):
        if path_new.exists():  # if so, load old datafile, skip reload
            return pd.read_pickle(str(path_new.resolve()))
        elif len(list_files) == 0 or ignore_update:  # only allow backup if new files weren't found
            if fn_callback is not None:
                fn_callback(f"Warning: Using datafile `{path_backup.name}` with no grounded reference.  Version skew may occur.", 0, True)
            return pd.read_pickle(path_backup)
        else:   # otherwise, delete the old backup
            path_backup.unlink()
    
    # time_init = pd.Timestamp('2010-01-01T00')  # not used any more
    if fn_callback is not None:
        fn_callback(f"Data has changed, regenerating core data bundle file {path_new}...")

    # Add a placeholder
    if fn_callback is not None:
        fn_callback(f"", 0)
    task_buffer = 6   # account for time-norm, sorting, shot-mapping, named entity, dup-dropping
    task_count = len(list_files)+task_buffer

    df = None
    for task_idx in range(len(list_files)):
        f = list_files[task_idx]
        if fn_callback is not None:
            fn_callback(f"Loading file '{f.name}'...", math.floor(float(task_idx)/task_count*100))
        df_new = pd.read_csv(str(f.resolve()))
        df = df_new if df is None else pd.concat([df, df_new], axis=0, sort=False)
    df["details"].fillna("", inplace=True)

    logger.info(f"Known columns: {list(df.columns)}")
    logger.info(f"Known types: {list(df['tag_type'].unique())}")

    # extract shot extents
    if fn_callback is not None:
        fn_callback(f"", math.floor(float(task_idx)/task_count*100))
    task_idx += 1

    if len(df[df["tag_type"]=="shot"]) == 0:   # warning, no shots detected!
        time_max = int(math.floor(df["time_end"].max() * UPSAMPLE_TIME))
        time_min = int(math.floor(df["time_begin"].min() * UPSAMPLE_TIME))
        str_print = f"... generating shots at interval {DEFAULT_SHOT_LEN}s [{time_min} - {time_max}]"
        if fn_callback is not None:
            fn_callback(str_print)
        logger.info(str_print)
        list_new = [{"time_begin":x * DEFAULT_SHOT_LEN, "time_end": (x+1) * DEFAULT_SHOT_LEN,
                     "extractor": "simulated", "tag_type":"shot", "score": 1.0,
                     "time_event":(x+0.5) * DEFAULT_SHOT_LEN, "details":"", "source_event": "video" } 
                     for x in range(math.floor(time_min/DEFAULT_SHOT_LEN), math.floor(time_max/DEFAULT_SHOT_LEN)) ]
        df = df.append(pd.DataFrame(list_new), sort=False)

    if fn_callback is not None:
        fn_callback(f"... mapping shot id to all events....")
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
            if fn_callback is not None:
                fn_callback(f"Extracting words from transcripts, input samples {len(df_enhance)}, attempting to split")
            pass
        elif len(df[df["tag_type"]=="keyword"]) != 0:   # pull it from keyword
            df_enhance = df[df["tag_type"]=="keyword"].copy()
            df_enhance["details"] = df_enhance["tag"]
            if fn_callback is not None:
                fn_callback(f"Extracting words from keywords, input samples {len(df_enhance)}, attempting to split")
            pass
        list_append = []
        idx_search = 0
        re_clean = re.compile(r"[^0-9A-Za-z]+")
        for row_idx, row_enhance in df_enhance.iterrows():
            list_text = re_clean.split(row_enhance["details"].lower())
            for cur_word in list_text:
                if len(cur_word) > 1:
                    row_copy = row_enhance.copy()
                    row_copy["details"] = cur_word.capitalize()
                    list_append.append(row_copy)
        if len(list_append):
            df_append = pd.DataFrame(list_append)
            df_append["tag_type"] = "word"
            df_append["tag"] = df_append["details"]
            if fn_callback is not None:
                fn_callback(f"Extracting {len(df_append)} new words for analysis")
            df = df.append(df_append)

    if NLP_TOKENIZE:
        # extract/add NLP tags from transcripts
        if fn_callback is not None:
            fn_callback(f"... detecting NLP-based textual entities....", math.floor(float(task_idx)/task_count*100))
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
            if (idx_sub % 20) == 0:
                if fn_callback is not None:
                    fn_callback(f"... detecting NLP-based textual entities ({idx_sub}/{len(df_sub)})....")
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
        if fn_callback is not None:
            fn_callback(f"... integrating {len(list_new)} new text entities....")
        df_entity = pd.DataFrame(list_new)
        df = pd.concat([df, df_entity], sort=False)
        list_new = None
        df_entity = None

        # Create list of word tokens
        if fn_callback is not None:
            fn_callback(f"... filtering text stop words....", math.floor(float(task_idx)/task_count*100))
        task_idx += 1
        # from spacy.lang.en.stop_words import STOP_WORDS
        list_new = df[df["tag_type"]=="word"]["tag"].unique()
        map_new = {}
        re_clean = re.compile(r"[^0-9A-Za-z]")
        for idx_sub in range(len(list_new)):
            if (idx_sub % 20) == 0:
                if fn_callback is not None:
                    fn_callback(f"... filtering text stop words ({idx_sub}/{len(list_new)})....")
            word_new = nlp(list_new[idx_sub])
            map_new[list_new[idx_sub]] = re_clean.sub('', word_new.text.lower()) if not nlp.vocab[word_new.text].is_stop else NLP_STOPWORD
        # now map to single array of mapping
        df.loc[df["tag_type"]=="word", "details"] =  df[df["tag_type"]=="word"]["tag"].apply(lambda x: map_new[x])

    if fn_callback is not None:
        fn_callback(f"... normalizing time signatures...", math.floor(float(task_idx)/task_count*100))
    task_idx += 1
    for tf in ["time_event", "time_begin", "time_end"]:  # convert to pandas time (for easier sampling)
        if False:
            df[tf] = df[tf].apply(lambda x: pd.Timestamp('2010-01-01T00') + pd.Timedelta(x, 'seconds'))
        else:
            df[tf] = pd.to_timedelta(df[tf], unit='s')
            df[tf].fillna(pd.Timedelta(seconds=0), inplace=True)

    if fn_callback is not None:
        fn_callback(f"... pruning duplicates from {len(df)} events...", math.floor(float(task_idx)/task_count*100))
    df.drop_duplicates(inplace=True)
    task_idx += 1
    
    if fn_callback is not None:
        fn_callback(f"... sorting and indexing {len(df)} events...", math.floor(float(task_idx)/task_count*100))
    task_idx += 1
    df.sort_values(["time_begin", "time_end"], inplace=True)
    df.set_index("time_event", drop=True, inplace=True)

    # extract faces (emotion)

    if fn_callback is not None:
        fn_callback(f"... loaded {len(df)} rows across {len(list_files)} files...")
        fn_callback("", 0)

    # save new data file before returning
    df.to_pickle(str(path_new.resolve()))
    return df


def data_index_callback(stem_datafile, data_dir, df, allow_cache=True, ignore_update=False, fn_callback=None):
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
    path_new = Path(data_dir).joinpath(f"{stem_datafile}.{m.hexdigest()[:8]}.pkl.gz")
    
    # see if checksum matches the datafile (plus stem)
    if allow_cache and (path_new.exists() or path_backup is not None):
        if path_new.exists():  # if so, load old datafile, skip reload
            df = pd.read_pickle(str(path_new.resolve()))
            tree = BallTree(df)
            if fn_callback is not None:
                fn_callback("")
            return tree, list(df.index)
        elif df is None or len(df) == 0 or ignore_update:  # only allow backup if new files weren't found
            if fn_callback is not None:
                fn_callback(f"Warning: Using datafile `{path_backup.name}` with no grounded reference.  Version skew may occur.", 0, True)
            df = pd.read_pickle(path_backup)
            if fn_callback is not None:
                fn_callback(f"... building live index on features...")
            tree = BallTree(df)
            if fn_callback is not None:
                fn_callback("")
            return tree, list(df.index)
        else:   # otherwise, delete the old backup
            path_backup.unlink()
    
    # time_init = pd.Timestamp('2010-01-01T00')  # not used any more
    if fn_callback is not None:
        fn_callback(f"Data has changed, regenerating core data bundle file {path_new}...")

    # Add a placeholder
    task_buffer = 4   # account for pivot, index, duration
    task_idx = 0

    re_encode = re.compile(r"[^0-9a-zA-Z]")

    list_pivot = []
    tuple_groups = df.groupby(["tag", "tag_type"])   # run once but get length for progress bar
    task_count = len(tuple_groups)+task_buffer
    num_group = 0
    if fn_callback is not None:
        fn_callback(f"... preprocessing data...", math.floor(task_idx/task_buffer*100))

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
            if fn_callback is not None:
                fn_callback(f"... vectorizing {num_group}/{len(tuple_groups)} tag/tag_type shots...")
        # average by shot
        df_mean = df_group.groupby('shot')['score'].mean().reset_index(drop=False)
        df_mean["tag"] = re_encode.sub('_', '_'.join(idx_group))
        list_pivot += list(df_mean.values)
        num_group += 1

    # pivot to make a shot-wise row view
    task_idx += 1
    tuple_groups = None
    if fn_callback is not None:
        fn_callback(f"... pivoting table for score reporting...", math.floor(task_idx/task_buffer*100))
    df_vector_raw = pd.DataFrame(list_pivot, columns=["shot", "score", "tag"])
    df_vector = pd.pivot_table(df_vector_raw, index=["shot"], values="score", columns=["tag"], fill_value=0).sort_index()
    df_vector.index = df_vector.index.astype(int)
    df_vector_raw = None

    # append duration
    task_idx += 1
    if fn_callback is not None:
        fn_callback(f"... linking shot duration to vectors...", math.floor(task_idx/task_buffer*100))
    df_sub = df[df["tag_type"]=="shot"][["shot", "duration", "score"]].set_index("shot", drop=True)
    df_vector = df_vector.join(df_sub["duration"])  # grab duration from original data
    df_sub = None

    # train new hot-index object for fast kNN query
    # https://scikit-learn.org/stable/modules/generated/sklearn.neighbors.BallTree.html#sklearn.neighbors.BallTree.query
    task_idx += 1
    if fn_callback is not None:
        fn_callback(f"... building live index on features...", math.floor(task_idx/task_buffer*100))
    tree = BallTree(df_vector)

    if fn_callback is not None:
        fn_callback("", 0)

    # save new data file before returning
    df_vector.to_pickle(str(path_new.resolve()))
    return tree, list(df_vector.index)


def data_label_serialize_callback(data_dir, df_new=None, label_new=None, fn_callback=None):
    """Method to load labels and append them to the primary data frame

    :param stem_datafile: (str): Stem for active label files
    :param data_dir: (str): Absolute/relative path for label file
    :param label_new: (int): Label for row (-1=false, 1=true, 0=unknown)
    :return bool: True/False on success of save
    """
    if LABEL_AS_CSV:
        path_new = Path(data_dir).joinpath(f"{PATH_BASE_LABELS}.csv.gz")
        path_lock = Path(data_dir).joinpath(f"{PATH_BASE_LABELS}.LOCK.csv.gz")
    else:
        path_new = Path(data_dir).joinpath(f"{PATH_BASE_LABELS}.pkl.gz")
        path_lock = Path(data_dir).joinpath( f"{PATH_BASE_LABELS}.LOCK.pkl.gz")
    if fn_callback is not None:
        fn_callback("")
    if df_new is None or label_new is None:
        if path_new.exists():
            if LABEL_AS_CSV:
                df = pd.read_csv(str(path_new.resolve()))
                df["time_begin"] = pd.to_timedelta(df["time_begin"])
                df["label"] = df["label"].astype(int)
            else: 
                df = pd.read_pickle(str(path_new.resolve()))
            return df
        if fn_callback is not None:
            fn_callback(f"Warning, label file `{path_new}` is not found (ignore this on first runs)!")
        return None
    num_lock = 0
    while path_lock.exists():  # if so, load old datafile, skip reload
        num_lock += 1
        if num_lock > MAX_LOCK_COUNT:
            if fn_callback is not None:
                fn_callback(f"Label file `{path_new}` is permanently locked, please clear the file or ask for help!")
            logger.error(f"Label file `{path_new}` is permanently locked, please clear the file or ask for help!")
            return False
        if fn_callback is not None:
            fn_callback(f"Label file `{path_new}` is temporarily locked, retry {num_lock} momentarily...")
        sleep(2)  # sleep a couple of seconds...
    if fn_callback is not None:
        fn_callback(f"Writing new label...")
    ts_now = pd.Timestamp.now()
    with path_lock.open('wt') as f:
        f.write(str(ts_now))
    col_primary = ["time_begin", "tag_type", "tag", "extractor"]
    df = pd.DataFrame([], columns=col_primary)
    if path_new.exists():
        if LABEL_AS_CSV:
            df = pd.read_csv(str(path_new.resolve()))
            df["time_begin"] = pd.to_timedelta(df["time_begin"])
            df["label"] = df["label"].astype(int)
        else: 
            df = pd.read_pickle(str(path_new.resolve()))
    df_new = df_new[col_primary].copy()
    df_new["timestamp"] = ts_now  # add new timestamp (now)
    df_new["label"] = int(label_new)   # add new label
    df = pd.concat([df_new, df], sort=False, ignore_index=True).drop_duplicates(col_primary)  # drop duplicate labels
    if LABEL_AS_CSV:
        df.to_csv(str(path_new.resolve()), index=False)
    else:
        df.to_pickle(str(path_new.resolve()))
    if fn_callback is not None:
        fn_callback(f"")
    if path_lock.exists():
        path_lock.unlink()
    return True
