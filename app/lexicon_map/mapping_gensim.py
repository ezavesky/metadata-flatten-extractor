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

from gensim.models import KeyedVectors

_RE_CLEAN = re.compile(r"([\)\(\"-]|\'s)+")

def term_clean(query):
    query = _RE_CLEAN.sub('', query.strip().lower())
    query = query.replace('/',' ').strip().replace(' ','_')
    return query


def tag2vect(word2vec, list_tags, output_file):
    """Given a specific model, map the file set (single line of text) into an output embedding space"""
    word_embedding = {}
    dim = word2vec.vector_size

    # TODO: set to None for older method instead... KeyedVectors(dim)
    sub2vec = KeyedVectors(dim) 
    for tag in list_tags:
        if (tag == ''): continue
        tag = term_clean(tag)
        if tag not in word2vec.vocab:  # not found so average several
            tag_vect = np.mean(list(map(lambda w: word2vec[w] if w in word2vec.vocab else np.zeros(dim),tag.split())),axis = 0)
            tag_new = tag.replace(' ','_')
            if sub2vec is not None:
                sub2vec.add(tag_new, tag_vect)
            else:
                word_embedding[tag_new] = tag_vect
        else:   # found, so just copy lexicon...
            if sub2vec is not None:
                sub2vec.add(tag, word2vec[tag])
            else:
                word_embedding[tag] = word2vec[tag]
        
    # write a w2v file
    path_target = Path(output_file).resolve()
    if sub2vec is None:
        with path_target.open('wt') as f:
            f.write(f"{len(word_embedding)} {word2vec.vector_size}\n")
            for w in word_embedding.keys():
                f.write(w+' '+' '.join(str(_) for _ in word_embedding[w])+"\n")
        
        # if(os.path.exists(filename_out)): os.system("rm %s"%filename_out)
        # output = open(output_file, 'a')
        # for w in word_embedding.keys():
        #     output.write(w+' '+' '.join(str(_) for _ in word_embedding[w])+"\n")
        # output.flush()
        # output.close()

        # add header to sub w2v file
        # line_count = int(subprocess.getoutput("wc -l %s" %sub_vec_file).strip().split()[0])
        # os.system("echo \'%d %d\' | cat - %s > temp && mv temp %s" % (line_count, word2vec.vector_size, sub_vec_file, sub_vec_file))
    else:
        sub2vec.save_word2vec_format(str(path_target), binary=False)
    
    return word_embedding if sub2vec is None else sub2vec


def domain_map(word2vec, qry_tag, target_domain, k=10, threshold=0.0, query_limit=500):
    """domain map/query given a tag and a model..."""
    # query with a tag, preprocessing and find top ranked iab tags.
    #    e.g. domain_map('armchair', contentai_aws)
    qry_tag = term_clean(qry_tag)
    print(f"QUERY: [{qry_tag}]")
    
    # qry_tag = qry_tag.strip().lower().replace('(','').replace(')','').replace('\"','').replace('\'s','').replace('-','').replace('/',' ').strip().replace(' ','_')

    results = []
    count = 0
    try:
        if qry_tag in target_domain:
            results.append({'tag':qry_tag, 'score':1.0})
            count += 1
        for w in word2vec.most_similar(positive=[qry_tag], topn=query_limit):
            if count >= k: break
            if (w[0] in target_domain and w[1]>=threshold): 
                results.append({'tag':w[0], 'score':w[1]})
                count += 1
    except KeyError:
        logger.error(qry_tag+" not found.")

    if len(results) == 0:
        logger.info("No mapping found.")
    return results


def model_load(model_name="word2vec-google-news-300"):
    # suggested datasets...
    # conceptnet-numberbatch-17-06-300
    # word2vec-google-news-300
    # fasttext-wiki-news-subwords-300

    # can also use this method...
    # python -m gensim.downloader --download glove-wiki-gigaword-50

    import gensim.downloader as api

    info = api.info()  # show info about available models/datasets
    if info is not None:
        logger.info(f"Found NLP models... {info['models'].keys()}")
    model = api.load(model_name)  # download the model and return as object ready for use
       
    # LEGACY CODE FOR LOADING .....
    # path_data = Path(path_data)
    # path_vector = path_data.joinpath("GoogleNews-vectors-negative300.bin")   # load word vector
    # if not path_vector.exists():
    #     request from  https://raw.githubusercontent.com/mmihaltz/word2vec-GoogleNews-vectors/master/GoogleNews-vectors-negative300.bin.gz
    # model = KeyedVectors.load_word2vec_format(str(path_vector), binary=True)
    
    logger.info(f'Found {len(model.vocab)} word vectors of word2vec with dim {model.vector_size}')
    return model


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
    subparse.add_argument("-m", "--mapping_model", type=str, default='glove-wiki-gigaword-300', 
        help="embedding imodel (suggest 'fasttext-wiki-news-subwords-300', 'glove-wiki-gigaword-300', 'word2vec-google-news-300', or 'conceptnet-numberbatch-17-06-300')")
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
    word2vec = model_load(run_settings['mapping_model'])
    
    # hash all inputs
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
        sub2vec = tag2vect(word2vec, list_targets, str(path_target_parsed.resolve())) 
        logger.info(f"Generated new target mapping for {len(list_targets)} terms in file '{str(path_target_parsed.resolve())}")
    else:
        sub2vec = KeyedVectors.load_word2vec_format(str(path_target_parsed.resolve()))
        logger.info(f"Loaded existing target mapping for {len(sub2vec.vocab)} terms in file '{str(path_target_parsed.resolve())}")

    # parse a single line at a time
    dict_results = {}
    with path_input.open('r') as f:
        for line in f:
            line = line.strip()
            keyword_results = domain_map(word2vec, line, sub2vec, 
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
