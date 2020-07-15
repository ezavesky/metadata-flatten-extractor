# -*- coding: utf-8 -*-
#! python
# ===============LICENSE_START=======================================================
# scene-me Apache-2.0
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


import re
import pandas as pd
# import os
import numpy as np
import json
from pathlib import Path
import hashlib

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

import spacy
from spacy.vocab import Vocab
from spacy.tokenizer import Tokenizer
from scipy import spatial

def list2vect(nlp, list_tags, output_file):
    """Given a specific model, map the file set (single line of text) into an output embedding space"""
    vocab = Vocab()
    for tag_raw in list_tags:   # for each tag input
        tag_raw = tag_raw.lower()
        vocab.set_vector(tag_raw, tag2vect(nlp, tag_raw, vocab))
        tag_id = nlp.vocab.strings[tag_raw]
        if tag_id not in nlp.vocab:   # search existing one
            tag_doc = nlp(tag_raw)
            vocab.set_vector(tag_raw, tag_doc.vector)
        else:
            vocab.set_vector(tag_raw, nlp.vocab[tag_id].vector)

    # write a w2v file
    path_target = Path(output_file).resolve()
    vocab.to_disk(str(path_target))
    return vocab


def tag2vect(nlp, tag_raw, target_domain):
    """Given a specific model, map the file set (single line of text) into an output embedding space"""
    tag_id = target_domain.strings[tag_raw]
    if tag_id in target_domain.vectors:   # search existing one
        return target_domain.vectors[tag_id]
    tag_doc = nlp(tag_raw)
    return tag_doc.vector


def domain_map(nlp, qry_tag, target_domain, k=20, threshold=0.0):
    """domain map/query given a tag and a model..."""
    # query with a tag, preprocessing and find top ranked iab tags.
    #    e.g. domain_map('armchair', contentai_aws)
    # print(f"QUERY: [{qry_tag}]")
    
    # qry_tag = qry_tag.strip().lower().replace('(','').replace(')','').replace('\"','').replace('\'s','').replace('-','').replace('/',' ').strip().replace(' ','_')

    results = []
    count = 0
    qry_tag = qry_tag.lower()

    query_vect = tag2vect(nlp, qry_tag, target_domain)
    query_vect = np.array([query_vect])
    # print("TAG", query_vect)
    keys, rows, score = target_domain.vectors.most_similar(query_vect, batch_size=2048, n=k*5)
   
    dict_seen = {}
    for idx in range(len(keys[0])):   # just search our new domain for similarity
        score_new = score[0][idx]
        text_new = target_domain[keys[0][idx]].text.lower()
        if score_new >= threshold and text_new not in dict_seen: 
            dict_seen[text_new] = True
            results.append({'tag':text_new, 'score':score_new})
            if len(results) >= k:
                return results
    return results


def model_load(model_name="en_core_web_lg"):
    # suggested datasets...
    # en_core_web_lg
    # en_vectors_web_lg
    # en_trf_distilbertbaseuncased_lg

    # can also use this method...
    # python -m spacy download en_core_web_lg
    nlp = spacy.load(model_name) 

    # consider more advanced transformers to learn classifiers? - https://explosion.ai/blog/spacy-transformers
       
    logger.info(f'Found {len(nlp.vocab.strings)} word vectors of nlp with dim {nlp.vocab.vectors_length}')
    return nlp


def vocab_load(path_vector):
    vocab = Vocab().from_disk(path_vector)
    logger.info(f'Found {len(vocab.strings)} word vectors of nlp with dim {vocab.vectors_length}')
    return vocab


def quick_parse(args=[], config={}):
    import argparse
    _ROOT = Path(__file__).parent

    parser = argparse.ArgumentParser(
        description="""
        Utility to quickly map input query strigns to a known dataset
            python -u /src/app/lexicon_map/mapping.py -i /data/inputs.txt \ 
                -t /data/domain_aws.csv /data/domain_gcp.csv /data/domain_gcp_logo.csv /data/domain_gcp_object.csv /data/domain_scene.csv \ 
                -c 0 1 1 1 0 -m word2vec-google-news-300 -o /data/mapping.json                
        """
    )
    subparse = parser.add_argument_group('data configuration')
    subparse.add_argument("-d", "--data_dir", type=str, default=str(_ROOT.joinpath('model').resolve()), 
        help="specify the source directory for ingested domains")
    subparse.add_argument("-m", "--mapping_model", type=str, default='en_core_web_lg', 
        help="embedding imodel (suggest 'en_core_web_lg', 'en_vectors_web_lg')")
    subparse.add_argument("-n", "--model_name", type=str, default='', 
        help="name to save the embedding model; if not provdided will be from hashed target file paths")
    subparse.add_argument("-t", "--target_domain", nargs='+', default='', required=True, 
        help="target domain file, add multiple input domain files")
    subparse.add_argument("-c", "--csv_column", nargs='+', default=None, 
        help="if csv, only use this specific column (and also skip the header); -1=disabled")    
    subparse = parser.add_argument_group('query configuration')
    subparse.add_argument("-i", "--input_query", type=str, default='', required=True, 
        help="input queries file")
    subparse.add_argument("-o", "--output_mapped", type=str, default='', 
        help="output mapped file")
    subparse.add_argument("-k", "--result_limit", type=int, default=10, 
        help="Max results per mapping")
    subparse.add_argument("-s", "--result_threshold", type=float, default=0.0, 
        help="Min score for reporting threshold of mapping")

    run_settings = vars(parser.parse_args(args))
    run_settings.update(config)
    logger.info(f"Run Settings: {run_settings}")

    if len(run_settings['csv_column']):
        if len(run_settings['csv_column']) != len(run_settings['target_domain']):
            logger.critical(f"Mismatch of CSV columns ({len(run_settings['csv_column'])}) and domains ({len(run_settings['target_domain'])}), please omit columns or match inputs: {run_settings['csv_column']} vs. {run_settings['target_domain']}")
            return None
        run_settings['csv_column'] = [int(x) for x in run_settings['csv_column']]

    path_input = Path(run_settings['input_query'])
    if not path_input.exists():
        logger.error(f"Input query file '{str(path_input.resolve())}' not found...'")
        return None

    path_data = Path(run_settings["data_dir"])
    if not path_data.exists():
        path_data.mkdir(parents=True)
        if not path_data.exists():
            logger.fatal(f"Attempt to create {str(path_data)} failed, please check filesystem permissions")
            return None

    # load word vector
    logger.info(f"Retrieving model data for {run_settings['mapping_model']}")
    nlp = model_load(run_settings['mapping_model'])
    
    # hash all inputs
    if len(run_settings['model_name']) > 0:
        path_target_parsed = path_data.joinpath(run_settings['model_name'] + ".w2v")
    else:
        hash_digest = hashlib.md5()
        list_files = [Path(x).resolve() for x in run_settings['target_domain']]
        for name in list_files:
            if not name.exists():
                logger.critical(f"Unable to find input file '{str(name)}', warning...")
            hash_digest.update(str(name).encode())
        path_target_parsed = path_data.joinpath(hash_digest.hexdigest()[:8] + ".w2v")
    logger.info(f"Expecting target domain vector data for {str(path_target_parsed.resolve())}")

    # make sure we have the target
    if not path_target_parsed.exists():
        list_targets = []
        for idx_file in range(len(list_files)):
            path_target = list_files[idx_file]
            logger.info(f"Loading the target domain file... '{str(path_target)}'")
            if len(run_settings['csv_column']) and run_settings['csv_column'][idx_file] > -1:
                df_input = pd.read_csv(str(path_target))
                # print(path_target, run_settings['csv_column'][idx_file],  df_input[list(df_input.columns)[run_settings['csv_column'][idx_file]]])
                target_column = list(df_input.columns)[run_settings['csv_column'][idx_file]]
                list_new = list(df_input[target_column].dropna().unique())
                logger.info(f"... pulled {(len(list_new))} items for column '{target_column}' ({run_settings['csv_column'][idx_file]})")
                list_targets += list_new
            else:     # read line by line of the input
                with path_target.open('r') as f:
                    list_targets += f.read()
        sub2vec = list2vect(nlp, list_targets, str(path_target_parsed.resolve())) 
        logger.info(f"Generated new target mapping for {len(list_targets)} terms in file '{str(path_target_parsed.resolve())}")
    else:
        sub2vec = vocab_load(str(path_target_parsed.resolve()))
        logger.info(f"Loaded existing target mapping for {len(sub2vec.strings)} terms in file '{str(path_target_parsed.resolve())}")

    # parse a single line at a time
    dict_results = {}
    with path_input.open('r') as f:
        for line in f:
            line = line.strip()
            keyword_results = domain_map(nlp, line, sub2vec, 
                k=run_settings['result_limit'], threshold=run_settings['result_threshold'])
            dict_results[line] = keyword_results

    # write to output
    if len(run_settings['output_mapped']):
        path_output = Path(run_settings['output_mapped']).resolve()
        if not path_output.parent.exists():
            path_output.mkdir(parents=True)
        with path_output.open('wt') as f:
            json.dump(dict_results, f)
        logger.info(f"Wrote {len(dict_results)} terms to file '{path_output}'...")
    return dict_results


if __name__ == "__main__":
    import sys
    quick_parse(args=sys.argv[1:])
