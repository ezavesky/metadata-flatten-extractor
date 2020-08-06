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


from queue import Queue
import atexit

import pandas as pd
import numpy as np
from pathlib import Path
import re
import math

import datetime as dt
import json

import dash
import dash_table
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State, ALL
import plotly.graph_objects as go
from dash.exceptions import PreventUpdate
from dash_table.Format import Format

import plotly.graph_objects as go
import plotly.express as px

import logging
logger = logging.getLogger()

import mapping_spacy as mapping    # TODO: configure this in another way
from common import preprocessing, queueproc
from common.media import manifest_parse

MAPPING_PRIMARY = "__primary"
MAPPING_LEXICON = "lexicon"
BACKEND_INTERVAL_WARN = 10   # how many spins until warning message?
MAX_RESULTS = 20   # max results for the mapping
MAX_AUTO_ENABLED = 5   # max results that are auto-enabled after mapping
HEATMAP_INTERVAL_SECONDS = 60

ALTAIR_DEFAULT_HEIGHT = 320   # height of charts

_app_obj = None   # our one dash app instance


def get_dash_app():
    """Return instance of dash app..."""
    global _app_obj
    return _app_obj


def create_dash_app(name, server, run_settings):
    """Create dash app..."""
    global _app_obj
    _app_obj = dash.Dash(name, 
        meta_tags=[{"name": "viewport", "content": "width=device-width"}], server=server,
        external_stylesheets=[dbc.themes.BOOTSTRAP]
    )

    # init app objects
    _app_obj.models = models_load(run_settings['mapping_model'], run_settings['data_dir'])
    _app_obj.dataset = dataset_load(None, run_settings['mapping_model'])

    class DatasetOp(queueproc.BaseProcess):
        def callback(self, str_log):
            self.cascade("progress", str_log)

        def do_discover(self, data_dir, manifest_path):
            # discover the dataframes for each asset
            dict_stems = dataset_discover(data_dir, manifest_path)
            self.send('load', dict_stems)
            return None

        def do_load(self, dict_stems):
            # load the dataframes for each asset
            result = dataset_load(dict_stems, run_settings['mapping_model'], fn_callback=self.callback)
            if result['data'] is not None:
                self.cascade("progress", f"Scheduling mapping to target {run_settings['model_target']}...")
                self.send('map', run_settings['model_target'], _app_obj.models, 
                    run_settings['data_dir'], result['data']) #, exclude_type=[], include_extractor=[])
            else:
                self.cascade("progress", f"No targets found, entering map-only mode...")
            return result

        def do_map(self, target, models, data_dir, df):
            if len(target) > 0:
                dataset_map(models, target,  data_dir, df, fn_callback=self.callback) #, exclude_type=[], include_extractor=[])
                self.cascade("progress", f"Mapped data from {target}...")
                return target
            self.cascade("progress", f"Skipping data map from {target}...")


    class DatasetProgress(queueproc.BaseProcess):
        def do_progress(self, status_msg):
            # do some other fancy update?
            # print("PROGRESS", status_msg)
            return status_msg

        def do_load(self, dict_update):
            _app_obj.dataset = dict_update
            return len(dict_update['data']) if dict_update['data'] is not None else 0

        def do_map(self, status_msg):
            return status_msg

    process2 = DatasetProgress()
    process1 = DatasetOp(recv=process2)
    process1.start()
    _app_obj.processing = {'scheduler':process1, 'progress':process2}

    # start the first process, data discovery!
    process1.send('discover', run_settings['data_dir'], run_settings['manifest'])

    return _app_obj

### ---------------- data and nlp mapping ---------------------------------------


def models_load(model_name, data_dir, models_dict=None):
    if models_dict is None:
        models_dict = {}

    # read the individual vocab labels
    path_data = Path(data_dir)
    for path_vocab in path_data.rglob("*.w2v"):   # scan data dir for secondary vocabularies
        models_dict[path_vocab.stem] = {"vocab": mapping.vocab_load(path_vocab.resolve()), "idx": len(models_dict)}
    # important for this to be at the end of the list
    if MAPPING_PRIMARY not in models_dict:   # first, load the primary model
        models_dict[MAPPING_PRIMARY] = {'vocab': mapping.model_load(model_name), 'idx': -1 }
    return models_dict


def dataset_map(models_dict, model_name, data_dir, df, exclude_type=[], include_extractor=[], fn_callback=None):
    # read the individual vocab labels, validate that the model is there
    if MAPPING_PRIMARY not in models_dict:
        return False
    if model_name in models_dict:
        return True
    def callback_echo(str_new):
        print(str_new)
    if fn_callback is None:
        fn_callback = callback_echo

    # pull out list of assets
    if df is None or len(df) < 1:
        return False
    if len(exclude_type) > 0:
        df = df[~df["tag_type"].isin(exclude_type)]
    if len(include_extractor) > 0:
        df = df[df["extractor"].isin(include_extractor)]

    # map into subvec
    path_vocab = Path(data_dir).joinpath(model_name + ".w2v")
    logger.info(f"Mapping assets from '{model_name}' from {len(df)} tags into model {str(path_vocab)}.")
    sub2vec = mapping.list2vect(models_dict[MAPPING_PRIMARY]['vocab'], list(df["tag"].astype(str)), 
                                str(path_vocab.resolve()), fn_callback=fn_callback) 

    # update model in local dict
    models_dict[path_vocab.stem] = {"vocab": sub2vec, "idx": len(models_dict)}
    return True


def dataset_discover(data_dir, manifest_file=''):
    path_data = Path(data_dir)
    dict_stems = {}

    list_manifest = []
    if manifest_file is not None and len(manifest_file):
        list_manifest = manifest_parse(manifest_file)
    if list_manifest:
        re_clean = re.compile(r"[^a-zA-z0-9]+")
        for dict_manifest in list_manifest:  # consolidate manifest entries
            # { "name": "Parking Spots on Mars", "video": "/video/park_marks.mp4", "results": "/results/park_mars" },
            parent_name = dict_manifest['name']
            path_scan = Path(dict_manifest['results'])
            list_files, path_new = preprocessing.data_discover_raw("lexicon", str(dict_manifest['results']), bundle_files=False)
            dict_stems[parent_name] = {'data':None, 'files':list_files, 'parent':parent_name,
                                    'base':path_data.joinpath(re_clean.sub('', parent_name) + ".pkl.gz"),
                                    'abs':parent_name.lower()}

    else:
        logger.info(f"Failed to load or parse the manifest file '{manifest_file}', scanning result dir '{data_dir}' instead")

        list_files, path_new = preprocessing.data_discover_raw("lexicon", str(data_dir), bundle_files=False)
        for path_test in list_files:  # consolidate inputs by the parent directory
            str_parent = f"{path_test.parent.name} ({path_test.parent.parent.name})"  # use two parent depths
            if str_parent not in dict_stems:
                dict_stems[str_parent] = {'data':None, 'files':[], 'parent':str(path_test.parent), 
                                        'base':path_data.joinpath(path_test.parent.name + ".pkl.gz"),
                                        'abs':str(path_test).lower()}
            dict_stems[str_parent]['files'].append(path_test)
    return dict_stems


def dataset_load(dict_stems, nlp_model, df=None, fn_callback=None):
    def callback_echo(str_new):
        print(str_new)
    if fn_callback is None:
        fn_callback = callback_echo
    if dict_stems is None:
        return {'data':None, 'assets':[]}
    
    sorted_stems = [dict_stems[x]['abs'] for x in dict_stems]
    sorted_stems.sort()

    for str_parent in dict_stems:  # consolidate inputs by the parent directory
        def callback_load(str_new="", progress=0, is_warning=False):
            str_log = f"[{str_parent} / {progress}%] {str_new}"
            fn_callback(str_log)
        df_new = preprocessing.data_load_callback(dict_stems[str_parent]['base'], nlp_model=nlp_model,
            data_dir=dict_stems[str_parent]['files'], map_shots=False, fn_callback=callback_load)
        df_new['asset'] = str_parent   # assign asset link back
        df_new['tag'] = df_new['tag'].str.lower()   # push all tags to lower case
        df_new['path'] = str_parent   # assign asset link back
        df_new['asset_idx'] = sorted_stems.index(dict_stems[str_parent]['abs'])   # assign asset link back
        df = df_new if df is None else pd.concat([df, df_new], ignore_index=True)
        str_log = f"Loaded assets '{str_parent}' from {str(dict_stems[str_parent]['base'])}... ({len(df_new)} rows)"
        fn_callback(str_log)
    return {'data':df, 'assets':dict_stems}


def generate_mapping(app, query=None, target_dataset=None, limit=MAX_RESULTS):
    list_return = []

    if query is not None:
        if target_dataset in app.models and valid_mapping(target_dataset):
            return mapping.domain_map(app.models[MAPPING_PRIMARY]['vocab'], query, 
                                        app.models[target_dataset]['vocab'], k=limit)
        else:
            return mapping.domain_map(app.models[MAPPING_PRIMARY]['vocab'], query, 
                                        app.models[MAPPING_PRIMARY]['vocab'].vocab, k=limit)
    return [ ]



### ---------------- layout and UX interactions ---------------------------------------

def valid_mapping(asset_label):
    return asset_label != MAPPING_PRIMARY


def targets_refresh(app):
    list_datasets = [{"label": k, "value": k} for k in app.models if valid_mapping(k)]
    if not list_datasets:
        list_datasets = [{"label":"(no datsets found)", "value":MAPPING_PRIMARY}]
    return list_datasets


def assets_refresh(app):
    list_assets = [{"label":x, "value":x, "sort":str(app.dataset['assets'][x]['parent'])} for x in app.dataset['assets']]
    if not list_assets:
        list_assets = [{"label":"(no assets found)", "value":MAPPING_PRIMARY, "disabled":True, "sort":'x'}]
    list_assets.sort(key=lambda x: x['sort'])
    return list_assets


def layout_generate():
    app = get_dash_app()

    # https://dash.plot.ly/dash-core-components/store
    local_store = dcc.Store(id='session', storage_type='memory', data={})   # use store

    list_datasets = targets_refresh(app)
    list_assets = assets_refresh(app)
    num_assets = len([x for x in list_assets if valid_mapping(x['value'])])

    return html.Div([
        dbc.Navbar([
            dbc.Col([ 
                dbc.Button([
                    html.Span(className="fas fa-bars", title='Toggle Filters')
                    ], id="button_filters", size="sm", color="primary", className="mt-2 mr-2"),
                html.H2(app.title, className="text-left d-inline align-text-top"),
            ], width=3),
            dbc.Col([ 
                dbc.Input(id="search_text", placeholder="(e.g. car truck not motorcycle, return to evaluate)", 
                    style={'width': '100%'}, debounce=False, bs_size="lg", persistence=True, className="rounded align-middle")
            ], width=7, md=7, sm=6, className=' '),
            # dbc.Col([ 
            #     # icon gallery - https://fontawesome.com/icons?d=gallery
            #     dbc.Button([
            #         html.I(className="fas fa-external-link-alt", title='Snapshot current state...')
            #         ], id="button_generate", className="mb-1", size="sm", color="primary"),
            #     ],
            #     width=2, md=2, sm=1, className='pt-2 text-right')
            ],
            color="primary", dark=True, className="row app-header bg-dark text-capitalize text-light"),
        dbc.Row([
            dbc.Collapse([
                dbc.Row(dbc.Col([
                    html.Div([
                        html.Span("Mapped Results", className="h4"),
                        html.Span(" (0)", className="text-dark smalls", id="mapped_count"),
                    ]),
                    dbc.Checklist(options=[], id="mapped_tags", className="itemlist border border-1 border-dark pl-1 pr-1", inline=False),
                    ], width=12)),
                dbc.Row(dbc.Col([
                    dbc.FormGroup([
                        html.Span("Target Dataset", className="h4"),
                        dbc.RadioItems(options=list_datasets, value=list_datasets[0]['value'], persistence=True, id="mapped_datasets", inline=True),
                        ])
                    ], width=12), className="mt-2"),
                html.Div([
                    dbc.Row(dbc.Col([
                        dbc.FormGroup([
                            html.Span("Tag Type", className="h4"),
                            dbc.Checklist(options=[], id="exclude_types", inline=True, persistence=True),
                            ])
                        ], width=12)),
                    dbc.Row(dbc.Col([
                        html.Div("Score Limiter", className="h4"),
                        dcc.RangeSlider(min=0, step=5, max=100, value=[50,100], persistence=True, id="filter_scores",
                            marks={0: {"label":"0.0"}, 50: {"label":"0.5"}, 100: {"label":"1.0"} })
                        ], width=12)),
                    dbc.Row(dbc.Col([
                        html.Div([
                            html.Span("Asset Filter", className="h4"),
                            html.Span([html.Span(f" ("), html.Span(json.dumps(num_assets), id="asset_list_count"), html.Span(")")], className="text-dark smalls"),
                        ]),
                        dbc.Checklist(id="asset_list", className="itemlist border border-1 border-dark pl-1 pr-1",
                            options=list_assets, value=[x['value'] for x in list_assets], persistence=True),
                        ], width=12)),
                    ], id="filter_asset_block", style={'display':'none'})
                ], id="core_filter", className="col-md-3 col-sm-12 border border-1 dark rounded p-2 mr-1 ml-1 border-dark"),
            dbc.Col([
                html.Div([
                    dbc.Row([
                        dbc.Col("", className="text-left", id="search_update"),
                        dbc.Col(f"({app.version['__project__']} v{app.version['__version__']}, {app.version['__copyright__']})", 
                                    className="text-right col-8")
                        ], className="text-muted small "),
                    dbc.Row(dbc.Col([
                        html.Div("Overall Event Histogram", className="h4"),
                        dcc.Graph(id="graph_histogram"),
                        ])),
                    dbc.Row([
                        dbc.Col([
                            html.Div("Asset Inventory Estimates", className="h4 mb-0"),
                            html.Div("(click an interval for tabular event view)", className="small text-muted"),
                            ]),
                        dbc.Col([
                            html.Div([
                                dbc.Label("Estimator"),
                                dbc.RadioItems(
                                    options=[
                                        {"label": "mean", "value": "mean"},
                                        {"label": "max", "value": "max"},
                                        {"label": "count", "value": "count"},
                                        ],
                                    value="mean", id="inventory_estimator", persistence=True, inline=True),
                                ])
                            ], className="col-3 border border-dark rounded pb-0 border-1 small"),
                        ], className="w-100"),
                    dbc.Row(dbc.Col([
                        dcc.Graph(id="graph_inventory"),
                        ])),
                    dbc.Row(dbc.Col([
                        html.Div("", id="primary_item")
                        ]), className="w-100"),
                    ], id="core_results", style={"display":"none"}),
                html.Div([                    
                    html.Div("Sorry, no assets found. Try typing above or specifing an alternate target directory.")
                    ], id="core_empty", className="h3", style={"display":"none"}),
                dbc.Row(dbc.Col([
                    html.Div("", id="callback_progress_note", className="text-center "),
                    dbc.Progress(value=0, id="callback_progress_animated", style={"height": "4em", 'display':'none'}, 
                                striped=True, animated=True),
                    html.Div([
                        dbc.Button([
                            dbc.Spinner(size="sm"), 
                            " Concurrent processing is active in background..."
                            ], color="primary", disabled=True, className="text-center m-2 text-light"),
                        ], className="d-flex justify-content-center"),
                    dcc.Interval(id="callback_interval", n_intervals=0, interval=2000, disabled=False),
                    html.Div("0", id="callback_count_last", style={"display":"none"}),
                    ], className="p-0 pt-2"), className="bg-light rounded border border-1 border-dark m-0 mx-auto", id="callback_progress"), 
                    # style={"display":"none"}),
                ], id="core_tabs", className="border border-1 rounded mr-1 ml-1 p-1 border-dark")
        ], className="rounded h-100 mt-1"),
        
        # Hidden div inside the app that stores the intermediate value
        local_store,   # use store
        # dcc.Interval(
        #     id='interval_component',
        #     interval=_app_obj.settings['refresh_interval'], # in milliseconds
        #     n_intervals=0
        # ),
        # html.Div(id='intermediate_value', style={'display': 'none'})   # use hidden div for data
    ], id="mainContainer", className="container-fluid h-100")


def num_simplify(num_raw):
    unit = ['', 'K', 'M', 'B']
    idx_unit = 0
    while num_raw > 1000:
        num_raw = round(num_raw/1000, 1)
        idx_unit += 1
    return f"{num_raw}{unit[idx_unit]}"


def callback_create(app):
    re_space = re.compile(r"\s+$")

    @app.callback(
        [Output('mapped_tags', 'options'), Output('mapped_tags', 'value'), Output('mapped_count', 'children'), 
         Output('exclude_types', 'options'), Output('exclude_types', 'value'), Output('session', 'data')],
        [Input('search_text', 'n_submit'), Input('mapped_datasets', 'value'), Input('callback_interval', 'disabled')],
        [State('search_text', 'value'), State('session', 'data')]
    )
    def update_results(search_submit, dataset_selected, loading_complete, search_str, session_data):
        """update the contents of channel data"""
        ctx = dash.callback_context    # validate specific context (https://dash.plotly.com/advanced-callbacks)
        # if not ctx.triggered :
        #     raise dash.exceptions.PreventUpdate
        trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]
        # elif trigger_id == "core_interval" and session_user is not None:   # interval should never be the trigger if already have user data
        #     raise dash.exceptions.PreventUpdate
        if not loading_complete:
            logger.info("UPDATE delayed while loading...")
            raise dash.exceptions.PreventUpdate

        module_trigger = None
        module_type = None
        if ":" in trigger_id:  # look for specific moduel trigger
            dict_trigger = json.loads(trigger_id)
            module_trigger = dict_trigger["index"]
            module_type = dict_trigger['type']
        df = app.dataset['data']
        logger.info(f"UPDATE: {trigger_id} [{module_trigger}, {module_type}], query '{search_str}', df ({df is None})")

        # prep output of which tags and their weights
        session_data['mapped'] = []
        if search_str is not None and len(search_str) > 0:
            session_data['mapped'] = generate_mapping(app, query=search_str, target_dataset=dataset_selected)

        # prep new filter in session data
        list_tags = [{"label":"(no mapped tags available)", "value":"none", "disabled":True}]
        tags_enabled = []
        list_types = [{"label":"(no mapped tags available)", "value":"none", "disabled":True}]
        types_enabled = []

        # prep output for selected tag types
        if len(session_data['mapped']) > 0:
            # list_types = [{"label":"(no mapped tags available)", "value":"none", "disabled":True}]
            # types_enabled = []
            list_tags = []
            # tags_enabled = [x['tag'] for x in session_data['mapped']]  # TODO: persist which tags are enabled if found?
            for x in session_data['mapped']:
                num_tag = "" if df is None else f", {len(df[df['tag'] == x['tag']])} events"
                list_tags.append({"value":x['tag'],
                    "label": f"{x['tag']} (match: {round(x['score']*1000)/1000}{num_tag})"})
                tags_enabled.append(x['tag'])
            tags_enabled = tags_enabled[:MAX_AUTO_ENABLED]  # limit to top N

        if df is not None:  # detect the numbers of types in dataset
            list_types = []
            for idx_group, df_group in df.groupby(["tag_type"]):   # run to get clusters and unique tag types
                list_types.append({"label": f"{idx_group} ({num_simplify(len(df_group))})", "value":idx_group, 'count':len(df_group)})
            list_types.sort(reverse=True, key=lambda x: x['count'])
            types_enabled = [x['value'] for x in list_types]
        
        # return it all
        return [list_tags, tags_enabled,
                f" ({len(session_data['mapped'])} tag{'s' if len(session_data['mapped']) != 1 else ''})",
                list_types, types_enabled, session_data]


    @app.callback(
        [Output('search_update', 'children'),
         Output("graph_histogram", "figure"), Output('graph_inventory', 'figure'),
         Output('core_results', 'style'), Output('core_empty', 'style'), Output('filter_asset_block', 'style')],
        [Input('mapped_tags', 'value'), Input('exclude_types', 'value'), 
         Input('filter_scores', 'value'), Input('asset_list', 'value'),
         Input('inventory_estimator', 'value')],       
        [State('session', 'data')]
    )
    def redraw_main(tag_active, type_active, score_active, asset_active, estimator_active, session_data):
        df = app.dataset['data']
        num_raw = 0
        num_filtered = 0
        if df is not None and tag_active is not None:
            num_raw = len(df)
            df_filter = df["tag"].isin(tag_active)   # filter out by tag names
            df_filter &= df["tag_type"].isin(type_active)   # filter out by tag type
            df_filter &= df["asset"].isin(asset_active)   # filter out by asset name
            df_filter &= df["score"] > score_active[0]/100  # filter out by score
            df_filter &= df["score"] <= score_active[1]/100   
            df = df[df_filter]   # finalize filter
            num_filtered = len(df)

        return [
            f"{num_filtered} of {num_raw} events, updated {dt.datetime.now().strftime(format='%H:%M:%S %Z')}",
            make_distribution_graph(df),   # draw primary distribution graph
            make_asset_graph(df, estimator_active),   # draw asset heatmap graph
            {"display":"block" if num_filtered else "none"}, {"display":"none" if num_filtered else "block"},
            {"display":"block" if num_raw else "none"},
        ]

    @app.callback(
        [Output('core_filter', 'is_open')],
        [Input('button_filters', 'n_clicks')],
        [State('core_filter', 'is_open')]
    )
    def trigger_submit(num_clicks, hidden_state):
        return [num_clicks is None or not hidden_state]

    @app.callback(
        [Output('search_text', 'n_submit')],
        [Input('search_text', 'value')],
        [State('search_text', 'n_submit')]
    )
    def submit_search(search_text, n_submit):
        if search_text is None or (re_space.search(search_text) is None and len(search_text) > 0):
            raise dash.exceptions.PreventUpdate
        return [1 if n_submit is None else n_submit + 1]

    @app.callback(
        [Output('callback_progress', 'style'), Output('callback_progress_animated', 'value'), 
         Output('callback_progress_animated', 'children'), Output('callback_progress_note', 'children'),
         Output('mapped_datasets', 'value'), Output('mapped_datasets', 'options'), 
         Output('asset_list_count', 'children'),
         Output('callback_interval', 'disabled'), Output('callback_count_last', 'children')],
        [Input('callback_interval', 'n_intervals')],
        [State('session', 'data'), State('callback_count_last', 'children'), State('asset_list_count', 'children')]
    )
    def update_progress(n_intervals, session_data, n_interval_last, num_assets):
        ctx = dash.callback_context    # validate specific context (https://dash.plotly.com/advanced-callbacks)
        if not ctx.triggered :
            raise dash.exceptions.PreventUpdate
        # print(n_intervals, app.processing)

        msg_parts = []
        list_datasets = dash.no_update
        list_datasets_sel = dash.no_update
        list_asset_count = dash.no_update
        num_assets = json.loads(num_assets)
        interval_disabled = False
        while True:
            msg = app.processing['progress'].run_local()
            if msg is None:
                break
            if msg.event == "progress":
                msg_parts.append(msg.args)
            elif msg.event == "load":
                msg_parts.append(f"Loaded a new model with {msg.args} elements ...")
                if msg.args == 0:  # terminate now if empty target request
                    interval_disabled = True
            elif msg.event == "map":
                num_assets = len([x for x in assets_refresh(app) if valid_mapping(x['value'])])
                msg_parts.append(f"Mapping complete {msg.args} elements... ({num_assets} assets)")
                list_datasets = targets_refresh(app)
                list_datasets_sel = list_datasets[0]['value']
            else:
                print("WEIRD NON_PROGRESS", msg)
        if app.processing['scheduler'].busy() or msg_parts:
            n_interval_last = n_intervals
        elif (app.dataset is not None and app.models is not None   # validate model finished loading
                and app.dataset['data'] is not None and len(app.dataset['data'])  # validate dataset loaded
                and num_assets > 0):   # validate assets are selected...
            # print("LOADING COMPLETE, DATASET AND MODEL DETECTED", "ASSETS:", num_assets)
            interval_disabled = True
        else:
            n_interval_last = json.loads(n_interval_last)
            if ((n_intervals - n_interval_last) % BACKEND_INTERVAL_WARN) == 0 and n_interval_last > BACKEND_INTERVAL_WARN:
                msg_parts.append(f"Warning: No back-end updates in {n_intervals - n_interval_last} intervals.  Did something break or is this a slow machine?")
            # print("NO ACTION, SHALL WE TIME OUT?")
            # if n_intervals - n_interval_last > 10:   # kinda arbitrary, but limit for spinner load
            #     interval_disabled = True
    
        dict_progress = {"value":0.5, "message":dash.no_update }
        if msg_parts:
            dict_progress['message'] = [html.Div(x, className="small") for x in msg_parts]
        print("LOADING UPDATE", msg_parts, n_intervals, n_interval_last, app.processing['scheduler'].busy())

        # if value_last == dict_progress['message']:
        #     raise dash.exceptions.PreventUpdate
        # if dict_progress['value'] >= 1:
        #     return [{"display":"none"}, 0, "(done)", "(task complete)", False]
        return [{"display":"none" if interval_disabled else "block"}, round(dict_progress['value']*100), 
                f"{round(dict_progress['value']*100)}%", dict_progress['message'],  
                list_datasets_sel, list_datasets, json.dumps(num_assets),
                interval_disabled, json.dumps(n_interval_last)]

    @app.callback(
        [Output('asset_list', 'value'), Output('asset_list', 'options')],
        [Input('asset_list_count', 'children')])
    def update_assets(num_assets):
        num_assets = json.loads(num_assets)
        if not num_assets:
            raise dash.exceptions.PreventUpdate
        # NOTE: it's weird, but this special callback is required on complete because of a UX update race condition
        # print("RECEIVED ASSET LIST", num_assets)
        list_assets = assets_refresh(app)
        list_assets_sel = [x['value'] for x in list_assets]
        return [list_assets_sel, list_assets]

    @app.callback(
        [Output('primary_item', 'children')],
        [Input('graph_inventory', 'clickData')])
    def display_click_data(clickData):
        ctx = dash.callback_context    # validate specific context (https://dash.plotly.com/advanced-callbacks)
        if not ctx.triggered:
            raise dash.exceptions.PreventUpdate

        # { "points": [ { "curveNumber": 0, "x": "00:10:00", "y": "old_stan_in_the_mountain.mp4 (hiveai)", "z": 0.609 } ] }
        df = app.dataset['data']
        clickData = clickData["points"][0]
        
        df_filter = df["asset"] == clickData['y']
        time_offset = preprocessing.timedelta_val(clickData['x'])
        df_filter &= df["time_begin"] > (time_offset - dt.timedelta(seconds=HEATMAP_INTERVAL_SECONDS))
        df_filter &= df["time_begin"] <= (time_offset + dt.timedelta(seconds=HEATMAP_INTERVAL_SECONDS))
        df_filtered = df[df_filter].copy()
        df_filtered['offset'] = df_filtered['time_begin'].apply(lambda x: preprocessing.timedelta_str(x))
        df_filtered['score'] = df_filtered['score'].apply(lambda x: round(x*1000)/1000)
        df_filtered['duration'] = df_filtered['duration'].apply(lambda x: round(x*1000)/1000)
 
        valid_columns = ['offset', 'duration', 'tag', 'score', 'tag_type', 'source_event', 'extractor']
        time_begin = df_filtered['offset'].min()
        time_end = df_filtered['offset'].max()

        return [[
            html.Div([
                html.Div("Event Exploration", className="h4 mb-0"),
                html.Div(f"(event time extents: {time_begin} - {time_end}, asset: {clickData['y']})", className="small text-muted mb-1")
                ]),
            html.Div([
                dash_table.DataTable(
                    data=df_filtered.to_dict('records'), sort_action='native', 
                    columns=[{'id': c, 'name': c} for c in valid_columns],
                    page_size=ALTAIR_DEFAULT_HEIGHT,  # we have less data in this example, so setting to 20
                    style_table={'height': '25em', 'overflowY': 'auto', 'padding-right':'1em', 'padding-left':'1em'},
                    style_cell={ 'whiteSpace': 'normal', 'height': 'auto'},
                    ),
            ], className="m-1 ")
        ]]



    # _GENERAL_MODEL: mapping.model_load(run_settings['mapping_model'])}
    # _GENERAL_MODEL = "_general"


    ### ------------------- plotting capabilities ----------------------------


    def make_distribution_graph(df_live):
        # bucket into score range, then show frequency count
        if df_live is None or len(df_live) < 1:
            return go.Figure()
        df_filter = df_live.copy(True)
        df_filter["score"] = df_filter["score"].apply(lambda x: math.floor(x * 100)/100)
        df_filter_tag = preprocessing.aggregate_tags(df_filter, None, "tag")
        df_filter = preprocessing.aggregate_tags(df_filter, None, ["tag","score"])
        if len(df_filter_tag) > MAX_AUTO_ENABLED:   # use the TAG base aggregation first
            score_cut = df_filter_tag.iloc[MAX_AUTO_ENABLED]["count"]
            tag_cut = df_filter_tag[df_filter_tag["count"] < score_cut]["tag"].unique()
            df_filter.loc[df_filter["tag"].isin(tag_cut), "tag"] = "(other)"

        fig = go.Figure(data=[
            go.Bar(name=idx_group, x=df_group['score'], y=df_group['count'])
            for idx_group, df_group in df_filter.groupby(["tag"])
            ])
        fig.update_layout(barmode='stack', yaxis_type="log", xaxis_showgrid=True, height=ALTAIR_DEFAULT_HEIGHT, 
            margin=go.layout.Margin(l=30, r=30, t=0, b=50), xaxis={"range":[0,1], "constrain":"domain"},  # meanwhile compresses the xaxis by decreasing its "domain"
            legend=dict(orientation="h", yanchor="top", y=0.97, xanchor="left", x=0.01)
            )
        fig.update_xaxes(dtick=0.05, autorange=True, title_text="event score")
        fig.update_yaxes(nticks=20, title_text="event count (log)")
        return fig


    def make_asset_graph(df_live, target_col='mean'):
        # asset histogram for average/max score by time; mean/max/min/count
        if df_live is None or len(df_live) < 1:
            return go.Figure()
        df_filter = df_live.copy(True)

        df_filter["interval"] = df_filter["time_begin"].apply(lambda x: int(math.floor(x.total_seconds() / HEATMAP_INTERVAL_SECONDS)))
        list_asset = []
        df_filter = preprocessing.aggregate_tags(df_filter, None, ["asset", "asset_idx", "interval"])
        list_interval = list(range(0, (df_filter["interval"].max()+1) * HEATMAP_INTERVAL_SECONDS, HEATMAP_INTERVAL_SECONDS))  # in seconds
        list_interval = [preprocessing.timedelta_str(dt.timedelta(seconds=x)) for x in list_interval]   # in string
        for idx_asset, df_asset in df_filter.groupby(["asset"]):  # TODO: optimize this loop!
            list_local = [0] * len(list_interval)
            for row in df_asset.itertuples():
                list_local[getattr(row, "interval")] = getattr(row, target_col)
            list_asset.append({'data':list_local, 'name':idx_asset, "idx":df_asset["asset_idx"].iloc[0]})
        list_asset.sort(reverse=True, key=lambda x: x['idx'])

        fig = go.Figure(data=go.Heatmap(
            z=np.asarray([x['data'] for x in list_asset]), 
            x=list_interval, colorscale='Jet',  
            y=[x['name'] for x in list_asset]
            ))
        fig.update_layout(xaxis_showgrid=True, height=max(ALTAIR_DEFAULT_HEIGHT, ALTAIR_DEFAULT_HEIGHT/16*len(list_asset)), 
            margin=go.layout.Margin(l=30, r=30, t=20, b=50), #xaxis={"range":[0,1], "constrain":"domain"},  # meanwhile compresses the xaxis by decreasing its "domain"
            )
        fig.update_xaxes(title_text="event time seconds (HH:MM:SS)", autorange=True, tickangle=45)
        fig.update_yaxes(title_text="asset name", tickmode='linear')
        return fig

