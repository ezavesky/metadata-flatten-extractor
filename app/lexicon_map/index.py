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
import pandas as pd
import numpy as np
from pathlib import Path
import re

import datetime as dt
import json

import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State, ALL
import plotly.graph_objects as go
from dash.exceptions import PreventUpdate

import logging
logger = logging.getLogger()

import mapping_spacy as mapping    # TODO: configure this in another way
from common import preprocessing

MAPPING_PRIMARY = "__primiary"

_app_obj = None   # our one dash app instance


def get_dash_app():
    """Return instance of dash app..."""
    global _app_obj
    return _app_obj


def create_dash_app(name, server, log_size=0):
    """Create dash app..."""
    global _app_obj
    _app_obj = dash.Dash(name, 
        meta_tags=[{"name": "viewport", "content": "width=device-width"}], server=server,
        external_stylesheets=[dbc.themes.BOOTSTRAP]
    )
    _app_obj.log = Queue(log_size*2)  # new queue for incoming data
    return _app_obj


def models_load(model_name, data_dir=None, models_dict=None):
    if models_dict is None:
        models_dict = {}

    path_data = Path(data_dir)
    for path_vocab in path_data.rglob("*.w2v"):   # scan data dir for secondary vocabularies
        models_dict[path_vocab.stem] = {"vocab": mapping.vocab_load(path_vocab.resolve()), "idx": len(models_dict)}

    list_files, path_new = preprocessing.data_discover_raw("lexicon", str(path_data), bundle_files=False)
    dict_stems = {}
    for path_test in list_files:  # consolidate inputs by the parent directory
        str_parent = str(path_test.parent)
        if str_parent not in dict_stems:
            dict_stems[str_parent] = []
        dict_stems[str_parent].append(str(path_test))




    # data_load_callback(stem_datafile, data_dir, allow_cache=True, ignore_update=False, fn_callback=None, 
    #                         nlp_model="en_core_web_lg", map_shots=True):
    # def optional_callback(str_new="", progress=0, is_warning=False):   # simple callback from load process

    # data_load_callback(stem_datafile, data_dir, allow_cache=True, ignore_update=False, fn_callback=None, 
    #                         nlp_model="en_core_web_lg", map_shots=True):

    # important for this to be at the end of the list
    if MAPPING_PRIMARY not in models_dict:   # first, load the primary model
        models_dict[MAPPING_PRIMARY] = {'vocab': mapping.model_load(model_name), 'idx': -1 }

    return models_dict


def generate_mapping(app, query=None, target_dataset=None, limit=20):
    list_return = []

    if query is not None:
        if target_dataset in app.models:
            return mapping.domain_map(app.models[MAPPING_PRIMARY]['vocab'], query, 
                                        app.models[target_dataset]['vocab'], k=limit)
        else:
            return mapping.domain_map(app.models[MAPPING_PRIMARY]['vocab'], query, 
                                        app.models[MAPPING_PRIMARY]['vocab'].vocab, k=limit)
    return [ ]


def layout_results(list_search):
    if list_search:
        return [
            dbc.ListGroupItem([html.Span(x['tag']), html.Span(f" ({round(x['score']*1000)/1000})", className="text-muted small")], className="pt-1 pb-1")
            for x in list_search
        ]
    return [ dbc.ListGroupItem([html.Span("(no query entered)")], className="pt-1 pb-1") ]


def layout_generate():
    app = get_dash_app()

    # https://dash.plot.ly/dash-core-components/store
    local_store = dcc.Store(id='session', storage_type='memory')   # use store

    list_datasets = [{"label": k, "value": k} for k in app.models if k != MAPPING_PRIMARY]
    if not list_datasets:
        list_datasets = [{"label":"(no datsets found)", "value":MAPPING_PRIMARY+MAPPING_PRIMARY}]

    return html.Div([
        dbc.Navbar([
            dbc.Col([ 
                dbc.Button([
                    html.I(className="fas fa-bars", title='Toggle Filters')
                    ], id="button_filters", size="sm", color="primary", className="float-left mt-2 mr-2"),
                html.H2(app.title, className="text-left align-text-top"),
            ], width=3),
            dbc.Col([ 
                dbc.Input(id="search_text", placeholder="(e.g. car truck not motorcycle, return to evaluate)", 
                    style={'width': '100%'}, debounce=False, bs_size="lg", className="rounded align-middle")
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

                    # dbc.ListGroup([
                    #     dbc.ListGroupItem("...", className="pt-1 pb-1"),
                    #     ], id="mapped_tags", className="itemlist border border-1 border-dark", flush=True)
                    ], width=12)),
                dbc.Row(dbc.Col([
                    dbc.FormGroup([
                        html.Span("Target Dataset", className="h4"),
                        dbc.RadioItems(options=list_datasets,value=list_datasets[0]['value'], id="mapped_datasets", inline=True),
                        ])
                    ], width=12), className="mt-2"),
                # dbc.Row(dbc.Col([
                #     dbc.DropdownMenu([
                #         dbc.DropdownMenuItem(k, id={'type': 'dataset', 'index':k}) 
                #         for k in app.models if k != MAPPING_PRIMARY                        
                #         ], id="mapped_datasets", label="Target Dataset", className=""),
                #     ], width=12), className="mt-2 mb-2"),
                dbc.Row(dbc.Col([
                    dbc.FormGroup([
                        html.Span("Tag Type", className="h4"),
                        dbc.Checklist(options=[], id="filter_types", inline=True),
                        ])
                    ], width=12)),
                dbc.Row(dbc.Col([
                    html.Div("Score Limiter", className="h4"),
                    dcc.RangeSlider(min=0, step=5, max=100, value=[50,100], id="filter_scores",
                        marks={0: {"label":"0.0"}, 50: {"label":"0.5"}, 100: {"label":"1.0"} })
                    ], width=12)),
                dbc.Row(dbc.Col([
                    html.Span("Asset Filter", className="h4"),
                    dbc.ListGroup([
                        dbc.ListGroupItem([html.Span("asset")], className="pt-1 pb-1"),
                        dbc.ListGroupItem([html.Span("asset1")], className="pt-1 pb-1"),
                        dbc.ListGroupItem([html.Span("asset2")], className="pt-1 pb-1"),
                        dbc.ListGroupItem([html.Span("asset3")], className="pt-1 pb-1"),
                        ], id="asset_list", className="itemlist")
                    ], width=12)),
                ], id="core_filter", className="col-md-3 col-sm-12 border border-1 dark rounded p-2 mr-1 ml-1 border-dark"),
            dbc.Col([
                dbc.Row(dbc.Col([
                    html.Div("(progress message", id="callback_progress_note", className="text-center h5"),
                    dbc.Progress(value=0, id="callback_progress_animated", style={"height": "2em"}, striped=True, animated=True),
                    dcc.Interval(id="callback_interval", n_intervals=0, interval=1000, disabled=True),
                    ]), id="callback_progress", style={"display":"none"}),
                dbc.Row(dbc.Col([
                    html.Div("ITEM", id="primary_item")
                    ])),
                ], id="core_tabs", className="border border-1 dark rounded mr-1 ml-1 p-1 border-dark")
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

def callback_create(app):
    result_names = [f"result_{idx}" for idx in range(app.settings['result_count'])]

    @app.callback(
        # [Output(name, 'children') for name in result_names] + 
        [Output('mapped_tags', 'options'), Output('mapped_tags', 'value'), Output('mapped_count', 'children'), 
         Output('filter_types', 'options'), Output('filter_types', 'value')],
        [Input('search_text', 'n_submit'), Input('mapped_datasets', 'value')],
        [State('search_text', 'value'), State('session', 'data')]
    )
    def update_results(search_submit, dataset_selected, search_str, session_data):
        """update the contents of channel data"""
        ctx = dash.callback_context    # validate specific context (https://dash.plotly.com/advanced-callbacks)
        # if not ctx.triggered :
        #     raise dash.exceptions.PreventUpdate
        trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]
        # elif trigger_id == "core_interval" and session_user is not None:   # interval should never be the trigger if already have user data
        #     raise dash.exceptions.PreventUpdate

        module_trigger = None
        module_type = None
        if ":" in trigger_id:  # look for specific moduel trigger
            dict_trigger = json.loads(trigger_id)
            module_trigger = dict_trigger["index"]
            module_type = dict_trigger['type']
        logger.info(f"UPDATE: {trigger_id} [{module_trigger}, {module_type}], query '{search_str}'")

        # prep output of which tags and their weights
        list_search = []
        if search_str is not None and len(search_str) > 0:
            list_search = generate_mapping(app, query=search_str, target_dataset=dataset_selected)

        # generate our mapped tag results
        # list_items = layout_results(list_search)
        list_tags = [{"label":"(no mapped tags available)", "value":"none", "disabled":True}]
        tags_enabled = []
        if list_search:
            list_tags = [{"label": f"{x['tag']} ({round(x['score']*1000)/1000})", "value":x["tag"]}
                for x in list_search]
            tags_enabled = [x['value'] for x in list_tags[:5]]  # TODO: persist which tags are enabled if found?

        # prep output for selected tag types
        list_types = [{"label":"(no mapped tags available)", "value":"none", "disabled":True}]
        types_enabled = []
        if list_search:
            list_types = []
            for x in ["face", "tag", "identity", "brand"]:
                rand_count = np.random.randint(1, 3000)
                list_types.append({"label": f"{x} ({rand_count})", "value":x, 'count':rand_count})
            list_types.sort(reverse=True, key=lambda x: x['count'])
            types_enabled = [x['value'] for x in list_types]
        
        # return it all
        return [list_tags, tags_enabled,
                f" ({len(list_search)} tag{'s' if len(list_search) != 1 else ''})",
                list_types, types_enabled]

    @app.callback(
        [Output('primary_item', 'children')],
        [Input('mapped_tags', 'value'), Input('filter_types', 'value'), Input('filter_scores', 'value')],
        [State('session', 'data')]
    )
    def redraw_main(tag_active, type_active, score_active, session_data):
        return [
            html.Div([
                html.Div(f"Something new --- {np.random.randint(0, 1000)}"),
                html.Div([html.Span("Tags: "), html.Span(json.dumps(tag_active))]),
                html.Div([html.Span("Types: "), html.Span(json.dumps(type_active))]),
                html.Div([html.Span("Scores: "), html.Span(json.dumps(score_active))]),
                html.Div([html.Span("Session: "), html.Span(json.dumps(session_data))]),
            ])    
        ]

    @app.callback(
        [Output('core_filter', 'is_open')],
        [Input('button_filters', 'n_clicks')],
        [State('core_filter', 'is_open')]
    )
    def trigger_submit(num_clicks, hidden_state):
        return [num_clicks is None or not hidden_state]

    re_space = re.compile(r"\s+$")
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
         Output('callback_interval', 'disabled')],
        [Input('callback_interval', 'n_intervals')],
        [State('callback_progress_note', 'children'), State('session', 'data')]
    )
    def update_progress(n_intervals, value_last, session_data):
        ctx = dash.callback_context    # validate specific context (https://dash.plotly.com/advanced-callbacks)
        if not ctx.triggered or 'callback' not in session_data:
            raise dash.exceptions.PreventUpdate
        dict_progress = session_data['callback']
        if value_last == dict_progress['message']:
            raise dash.exceptions.PreventUpdate
        if dict_progress['value'] >= 1:
            return [{"display":"none"}, 0, "(done)", "(task complete)", False]
        return [{"display":"block"}, round(dict_progress['value']*100), 
                f"{round(dict_progress['value']*100)}%", dict_progress['message'], False]



    # _GENERAL_MODEL: mapping.model_load(run_settings['mapping_model'])}
    # _GENERAL_MODEL = "_general"


