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

import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
import plotly.graph_objects as go
from dash.exceptions import PreventUpdate


from quality.layout import coverage, assets, utils

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


def layout_generate():
    global _app_obj 

    # https://dash.plot.ly/dash-core-components/store
    _app_obj.store = dcc.Store(id='session', storage_type='memory', 
                                data=session_load(_app_obj, _app_obj.settings))   # use store
    return html.Div([
        dbc.Navbar([
            dbc.Col([ 
                dbc.Button([
                    html.I(className="fas fa-bars", title='Toggle Filters')
                    ], id="button_filters", size="sm", color="primary", className="float-left mt-2 mr-2"),
                html.H2(_app_obj.title, className="text-left"),
            ], width=3),
            dbc.Col([ 
                dbc.Tabs(id="tab_selection", active_tab='coverage', persistence=True, className='border-bottom-0', children=[
                    dbc.Tab(label='Event Coverage', tab_id='coverage', disabled=False),
                    dbc.Tab(label='Extractor Comparison', tab_id='extractors', disabled=True),
                    dbc.Tab(label='Visual Region', tab_id='regions', disabled=True),
                    dbc.Tab(label='Search', tab_id='search', disabled=True),
                    dbc.Tab(label='Asset List', tab_id='assets'),
                ])
                # dcc.Input(id="search_text", placeholder="(e.g. Kit Harrington in the snow)", 
                #     style={'width': '100%'}, debounce=True, className="rounded")
            ], width=7, md=7, sm=6, className='pt-2 '),
            dbc.Col([ 
                # icon gallery - https://fontawesome.com/icons?d=gallery
                dbc.Button([
                    html.I(className="fas fa-external-link-alt", title='Snapshot current state...')
                    ], id="button_generate", className="mb-1", size="sm", color="primary"),
                ],
                width=2, md=2, sm=1, className='pt-2 text-right')
            ],
            color="primary", dark=True, className="row p-0 app-header bg-dark text-capitalize text-light"),
        dbc.Row([
            dbc.Collapse([], id="core_filter", className="col-md-3 col-sm-12 border border-1 dark rounded p-1 mr-1 ml-1 border-dark"),
            dbc.Col([], id="core_tabs", md=8, width=8, sm=12, className="border border-1 dark rounded mr-1 ml-1 p-1 border-dark")
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
    assets.callback_create(app)

    result_names = [f"result_{idx}" for idx in range(app.settings['result_count'])]
    @app.callback([Output('core_tabs', 'children'), Output('core_filter', 'children')],
                [Input('tab_selection', 'active_tab')])
    def render_content(tab):
        if tab == 'coverage':
            return [coverage.layout_generate(app), coverage.sidebar_generate(app)]
        elif tab == 'extractors':
            return [[], []] # tab1.layout
        elif tab == 'regions':
            return [[], []] # tab1.layout
        elif tab == 'search':
            return [[], []] # tab2.layout
        elif tab == 'assets':
            from . import assets
            return [assets.layout_generate(app), assets.sidebar_generate(app)]


    # @app.callback(
    #     # [Output(name, 'children') for name in result_names] + 
    #     [Output('panel_results', 'children')] +
    #         [Output('result_count', 'children'), Output('session', 'data')],
    #     [Input('search_text', 'value')],
    #     [State('session', 'data')]
    # )
    # def update_results(search_str, session_data):
    #     """update the contents of channel data"""
    #     # utils.logger.info(f"SESSION: {session_data}")
    #     df_result = pd.DataFrame()
    #     num_results = len(df_result)
    #     # df_result, num_results = transforms.execute_search(search_str, page_size=session_data['result_count'], 
    #     #     offset=session_data['result_offset'], url_search=session_data['url_search'])
    #     list_dom = generate_results(df_result, session_data)
    #     str_page_display = "(no results)"
    #     if df_result is not None:
    #         str_page_display = f"{session_data['result_offset']}-{session_data['result_offset'] + len(df_result)}"
    #         if num_results > 0:
    #             str_page_display += f" of {num_results}"
    #     return [list_dom, str_page_display, session_data]

    @app.callback(
        Output('core_filter', 'is_open'),
        [Input('button_filters', 'n_clicks')],
        [State('core_filter', 'is_open')]
    )
    def toggle_filters(num_clicks, hidden_state):
        return num_clicks is None or not hidden_state

