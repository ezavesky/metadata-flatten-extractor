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

MAPPING_PRIMARY = "__primary"
MAPPING_LEXICON = "lexicon"
MAX_RESULTS = 20   # max results for the mapping
MAX_AUTO_ENABLED = 5   # max results that are auto-enabled after mapping

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

### ---------------- data and nlp mpapping ---------------------------------------


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

def dataset_load(data_dir, df=None):
    # discover the dataframes for each asset
    path_data = Path(data_dir)
    list_files, path_new = preprocessing.data_discover_raw("lexicon", str(data_dir), bundle_files=False)
    dict_stems = {}
    for path_test in list_files:  # consolidate inputs by the parent directory
        str_parent = f"{path_test.parent.name} ({path_test.parent.parent.name})"  # use two parent depths
        if str_parent not in dict_stems:
            dict_stems[str_parent] = {'data':None, 'files':[], 'parent':str(path_test.parent), 
                                      'base':path_data.joinpath(path_test.parent.name + ".pkl.gz")}
        dict_stems[str_parent]['files'].append(path_test)

    # load the dataframes for each asset
    for str_parent in dict_stems:  # consolidate inputs by the parent directory
        def callback_load(str_new="", progress=0, is_warning=False):
            logger.info(f"[{str_parent} / {progress}%] {str_new}")
        df_new = preprocessing.data_load_callback(dict_stems[str_parent]['base'],
            data_dir=dict_stems[str_parent]['files'], map_shots=False, fn_callback=callback_load)
        df_new['asset'] = str_parent   # assign asset link back
        df_new['tag'] = df_new['tag'].str.lower()   # push all tags to lower case
        df = df_new if df is None else pd.concat([df, df_new])
        logger.info(f"Loaded assets '{str_parent}' from {str(dict_stems[str_parent]['base'])}... ({len(df_new)} rows)")

    return {'data':df, 'assets':dict_stems}


def generate_mapping(app, query=None, target_dataset=None, limit=MAX_RESULTS):
    list_return = []

    if query is not None:
        if target_dataset in app.models:
            return mapping.domain_map(app.models[MAPPING_PRIMARY]['vocab'], query, 
                                        app.models[target_dataset]['vocab'], k=limit)
        else:
            return mapping.domain_map(app.models[MAPPING_PRIMARY]['vocab'], query, 
                                        app.models[MAPPING_PRIMARY]['vocab'].vocab, k=limit)
    return [ ]

### ---------------- layout and UX interactions ---------------------------------------

def layout_generate():
    app = get_dash_app()

    # https://dash.plot.ly/dash-core-components/store
    local_store = dcc.Store(id='session', storage_type='memory', data={})   # use store

    list_datasets = [{"label": k, "value": k} for k in app.models if k != MAPPING_PRIMARY]
    if not list_datasets:
        list_datasets = [{"label":"(no datsets found)", "value":MAPPING_PRIMARY+MAPPING_PRIMARY}]

    list_assets = [{"label":x, "value":x, "sort":str(app.dataset['assets'][x]['parent'])} for x in app.dataset['assets']]
    if not list_assets:
        list_assets = [{"label":"(no assets found)", "value":MAPPING_PRIMARY+MAPPING_PRIMARY, "disabled":True, "sort":'x'}]
    list_assets.sort(key=lambda x: x['sort'])

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
                        dbc.RadioItems(options=list_datasets, value=list_datasets[0]['value'], id="mapped_datasets", inline=True),
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
                    html.Div([
                        html.Span("Asset Filter", className="h4"),
                        html.Span(f" ({len(list_assets)})", className="text-dark smalls"),
                    ]),
                    dbc.Checklist(id="asset_list", className="itemlist border border-1 border-dark pl-1 pr-1",
                        options=list_assets, value=[x['value'] for x in list_assets]),
                    ], width=12)),
                ], id="core_filter", className="col-md-3 col-sm-12 border border-1 dark rounded p-2 mr-1 ml-1 border-dark"),
            dbc.Col([
                dbc.Row(dbc.Col([
                    html.Div("(progress message", id="callback_progress_note", className="text-center h5"),
                    dbc.Progress(value=0, id="callback_progress_animated", style={"height": "2em"}, striped=True, animated=True),
                    dcc.Interval(id="callback_interval", n_intervals=0, interval=1000, disabled=True),
                    ]), id="callback_progress", style={"display":"none"}),
                dbc.Row(dbc.Col([
                    html.Div("", className="text-right text-muted small w-100", id="search_update")
                    ])),
                dbc.Row(dbc.Col([
                    html.Div("ITEM", id="primary_item")
                    ])),
                dbc.Row(dbc.Col([
                    dcc.Graph(id="tag_histogram"),
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
         Output('filter_types', 'options'), Output('filter_types', 'value'), Output('session', 'data')],
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
        if len(session_data['mapped']) > 0 and df is not None:
            # list_types = [{"label":"(no mapped tags available)", "value":"none", "disabled":True}]
            # types_enabled = []
            list_tags = []
            # tags_enabled = [x['tag'] for x in session_data['mapped']]  # TODO: persist which tags are enabled if found?
            for x in session_data['mapped']:
                num_tag = len(df[df["tag"] == x['tag']])
                list_tags.append({"value":x['tag'],
                    "label": f"{x['tag']} ({round(x['score']*1000)/1000}, {num_simplify(num_tag)} events)"})
                tags_enabled.append(x['tag'])
            df_filter = df["tag"].isin(tags_enabled)  # start the filter with all results
            tags_enabled = tags_enabled[:MAX_AUTO_ENABLED]  # limit to top N

            # detect the numbers of different tags
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
        [Output('primary_item', 'children'), Output('search_update', 'children')],
        [Input('mapped_tags', 'value'), Input('filter_types', 'value'), 
         Input('filter_scores', 'value'), Input('asset_list', 'value')],       
        [State('session', 'data')]
    )
    def redraw_main(tag_active, type_active, score_active, asset_active, session_data):
        df = app.dataset['data']
        num_raw = len(df)
        if df is not None:
            df_filter = df["tag"].isin(tag_active)   # filter out by tag names
            df_filter &= df["tag_type"].isin(type_active)   # filter out by tag type
            df_filter &= df["asset"].isin(asset_active)   # filter out by asset name
            df_filter &= df["score"] > score_active[0]/100  # filter out by score
            df_filter &= df["score"] <= score_active[1]/100   

            df = df[df_filter]   # finalize filter

        return [
            html.Div([

            ]),
            f"{len(df)} of {num_raw} events, updated {dt.datetime.now().strftime(format='%H:%M:%S %Z')}"
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


    ### ------------------- plotting capabilities ----------------------------

    # Update Histogram Figure based on Month, Day and Times Chosen
    @app.callback(
        [Output("tag_histogram", "figure")],
        [Input('mapped_tags', 'value')],
        [State('session', 'data')]
    )
    def update_histogram(tags_picked, session_data):
        # keep sorted order score
        # display frequency of hits?
        # [xVal, yVal, colorVal] = get_selection(monthPicked, dayPicked, selection)

        # layout = go.Layout(
        #     bargap=0.01,
        #     bargroupgap=0,
        #     barmode="group",
        #     margin=go.layout.Margin(l=10, r=0, t=0, b=50),
        #     showlegend=False,
        #     # plot_bgcolor="#323130",
        #     # paper_bgcolor="#323130",
        #     dragmode="select",
        #     # font=dict(color="white"),
        #     xaxis=dict(
        #         range=[0, MAX_RESULTS],
        #         showgrid=False,
        #         nticks=MAX_RESULTS+1,
        #         fixedrange=True,
        #         # ticksuffix=":00",
        #     ),
        #     yaxis=dict(
        #         range=[0, max(yVal) + max(yVal) / 4],
        #         showticklabels=False,
        #         showgrid=False,
        #         fixedrange=True,
        #         rangemode="nonnegative",
        #         zeroline=False,
        #     ),
        #     annotations=[
        #         dict(
        #             x=xi,
        #             y=yi,
        #             text=str(yi),
        #             xanchor="center",
        #             yanchor="bottom",
        #             showarrow=False,
        #             font=dict(color="white"),
        #         )
        #         for xi, yi in zip(xVal, yVal)
        #     ],
        # )

        # return go.Figure(
        #     data=[
        #         go.Bar(x=xVal, y=yVal, marker=dict(color=colorVal), hoverinfo="x"),
        #         go.Scatter(
        #             opacity=0,
        #             x=xVal,
        #             y=yVal / 2,
        #             hoverinfo="none",
        #             mode="markers",
        #             marker=dict(color="rgb(66, 134, 244, 0)", symbol="square", size=40),
        #             visible=True,
        #         ),
        #     ],
        #     layout=layout,
        # )
        return [[]]


