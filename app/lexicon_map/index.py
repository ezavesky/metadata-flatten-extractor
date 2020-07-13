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


from os.path import join as path_join
from queue import Queue
import pandas as pd

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


def session_load(app, settings):
    if hasattr(app, 'store') and hasattr(app.store, 'data'):
        return app.store.data
    dtn = utils.dt_format()
    session = {'filters': {}, 'result_offset': 0}
    session.update(settings)
    return session

def generate_mapping(app, query=None, target_dataset=None):
    list_return = []
    if query is None:
        return [ dbc.ListGroupItem([html.Span("(no query entered)")], className="pt-1 pb-1") ]
    list_return = [
        dbc.ListGroupItem([html.Span("term1"), html.Span(" (0.30)", className="text-muted small")], className="pt-1 pb-1"),
        dbc.ListGroupItem([html.Span("term3"), html.Span(" (0.31)", className="text-muted small")], className="pt-1 pb-1"),
        dbc.ListGroupItem([html.Span("term2"), html.Span(" (0.23)", className="text-muted small")], className="pt-1 pb-1"),
    ]
    return list_return


def layout_generate():
    global _app_obj 

    # https://dash.plot.ly/dash-core-components/store
    _app_obj.store = dcc.Store(id='session', storage_type='memory')   # use store
    
    return html.Div([
        dbc.Navbar([
            dbc.Col([ 
                dbc.Button([
                    html.I(className="fas fa-bars", title='Toggle Filters')
                    ], id="button_filters", size="sm", color="primary", className="float-left mt-2 mr-2"),
                html.H2(_app_obj.title, className="text-left align-text-top"),
            ], width=3),
            dbc.Col([ 
                dbc.Input(id="search_text", placeholder="(e.g. car truck not motorcycle)", 
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
                    html.Span("Mapped Results", className="h4"),
                    html.Span(" (0)", className="text-light", id="mapped_count"),
                    ])),
                dbc.Row(dbc.Col([
                    dbc.ListGroup([
                        dbc.ListGroupItem("...", className="pt-1 pb-1"),
                        ], id="mapped_list", className="itemlist border border-1 border-dark", flush=True)
                    ], width=12)),
                dbc.Row(dbc.Col([
                    dbc.DropdownMenu([
                        dbc.DropdownMenuItem("target1", id={'type': 'dataset', 'index': "target1"}),
                        dbc.DropdownMenuItem("target2", id={'type': 'dataset', 'index': "target2"}),
                        ], id="mapped_datasets", label="Target Dataset", className=""),
                    ], width=12), className="mt-2 mb-2"),
                dbc.Row(dbc.Col("Asset Filter", className="h4", width="auto")),
                dbc.Row(dbc.Col([
                    dbc.ListGroup([
                        dbc.ListGroupItem([html.Span("asset")], className="pt-1 pb-1"),
                        dbc.ListGroupItem([html.Span("asset1")], className="pt-1 pb-1"),
                        dbc.ListGroupItem([html.Span("asset2")], className="pt-1 pb-1"),
                        dbc.ListGroupItem([html.Span("asset3")], className="pt-1 pb-1"),
                        ], id="asset_list", className="itemlist")
                    ], width=12)),
                ], id="core_filter", className="col-md-3 col-sm-12 border border-1 dark rounded p-2 mr-1 ml-1 border-dark"),
            dbc.Col([], id="core_tabs", className="border border-1 dark rounded mr-1 ml-1 p-1 border-dark")
        ], className="rounded h-100 mt-1"),
        
        # Hidden div inside the app that stores the intermediate value
        _app_obj.store,   # use store
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
        [Output('mapped_list', 'children'), Output('mapped_count', 'children')],
        [Input('search_text', 'value'), Input({'type': 'dataset', 'index': ALL}, 'n_clicks')],
        [State('session', 'data')]
    )
    def update_results(search_str, dataset_clicks, session_data):
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
        logger.info(f"UPDATE: {trigger_id} [{module_trigger}, {module_type}]")

        list_dom = generate_mapping(app, query=None, target_dataset=None)
        return [list_dom, f" ({len(list_dom)} tags)"]

    @app.callback(
        Output('core_filter', 'is_open'),
        [Input('button_filters', 'n_clicks')],
        [State('core_filter', 'is_open')]
    )
    def toggle_filters(num_clicks, hidden_state):
        return num_clicks is None or not hidden_state


    # _GENERAL_MODEL: mapping.model_load(run_settings['mapping_model'])}
    # _GENERAL_MODEL = "_general"


