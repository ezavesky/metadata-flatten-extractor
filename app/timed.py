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

import importlib
from os import path
import json

import streamlit as st

version_path = path.join("..", "metadata_flatten", "_version.py")

import modes

@st.cache(suppress_st_warning=False)
def manifest_parse(manifest_file):
    """Attempt to parse a manifest file, return list of processing directory and file if valid. (added v0.8.3)"""
    if manifest_file is None or len(manifest_file)==0 or not path.exists(manifest_file):
        print(f"Specified manifest file '{manifest_file}' does not exist, skipping.")
        return []
    try:
        with open(manifest_file, 'rt') as f:
            manifest_obj = json.load(f)  # parse the manifest directly
            if manifest_obj is None or len(manifest_obj) == 0:
                print(f"Specified manifest file '{manifest_file}' contained no valid entries, skipping.")
                return []
    except Exception as e:
        print(f"Failed to load requested manifest file {manifest_file}, skipping. ({e})")
        return []

    # validate columns (name, asset, results)
    if 'manifest' not in manifest_obj:
        print(f"Specified manifest file '{manifest_file}' syntax error (missing 'manifest' array), skipping.")
        return []
    # return only those rows that are valid
    list_return = []
    for result_obj in manifest_obj['manifest']:
        if "name" in result_obj and "video" in result_obj and "results" in result_obj:  # validate objects
            if path.exists(result_obj['video']) and path.exists(result_obj['results']):  # validate results directory
                list_return.append(result_obj)
    return list_return


def main_page(data_dir=None, media_file=None, ignore_update=False, manifest="", symlink=""):
    """Main page for execution"""
    # read in version information
    version_dict = {}
    with open(version_path) as file:
        exec(file.read(), version_dict)   
    st.title(version_dict['__description__']+" Explorer")

    ux_report = st.empty()
    ux_progress = st.empty()

    st.sidebar.markdown('### Discovery Filters')
    list_assets = manifest_parse(manifest)
    if len(list_assets):
        dict_assets = { v['name']: v for v in list_assets }
        names_assets = [v['name'] for v in list_assets]
        sel_asset = st.sidebar.selectbox("Asset", names_assets)
        # with a specific asset chosen, override the data directory and media file
        data_dir = dict_assets[sel_asset]['results']
        media_file = dict_assets[sel_asset]['video']

    # resume normal operation for the specific insight
    sel_mode = st.sidebar.selectbox("Insight Mode", modes.modules, index=modes.modules.index("overview"))
    page_module = importlib.import_module(f"modes.{sel_mode}")  # load module
    func_page = getattr(page_module, "main_page")   # get class template
    df_live = func_page(data_dir, media_file, ignore_update, symlink)  # attempt to process
    num_events = f"{len(df_live)} events" if df_live is not None else "(no events detected)"
    ux_report.markdown(f"""<div style="text-align:left; font-size:small; color:#a1a1a1; width=100%;">
                     <span >{version_dict['__package__']} (v {version_dict['__version__']})</span>
                     <span > - {num_events}</span></div>""", unsafe_allow_html=True)


def main(args=None):
    import argparse
    
    parser = argparse.ArgumentParser(
        description="""A script to run the data explorer.""",
        epilog="""Application examples
            # specify the input media file 
            streamlit run timed.py -- -m video.mp4
    """, formatter_class=argparse.RawTextHelpFormatter)
    submain = parser.add_argument_group('main execution')
    submain.add_argument('-d', '--data_dir', dest='data_dir', type=str, default='../results', help='specify the source directory for flattened metadata')
    submain.add_argument('-m', '--media_file', dest='media_file', type=str, default=None, help='specific media file for extracting clips (empty=no clips)')
    submain.add_argument('-i', '--ignore_update', dest='ignore_update', default=False, action='store_true', help="Ignore update files and use bundle directly")
    submain.add_argument('-l', '--manifest', dest='manifest', type=str, default='', help='specify a manifest file for multiple asset analysis')
    submain.add_argument('-s', '--symlink', dest='symlink', type=str, default='', help='specify a symlink directory for serving static assets (empty=disabled)')

    if args is None:
        config_defaults = vars(parser.parse_args())
    else:
        config_defaults = vars(parser.parse_args(args))
    print(f"Runtime Configuration {config_defaults}")

    main_page(**config_defaults)


# main block run by code
if __name__ == '__main__':
    main()
