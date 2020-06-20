

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


from . import utils

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


def generate_results(df_result=None, session_data=None):
    list_return = []
    if df_result is not None and session_data is not None:
        for idx, df_row in df_result.iterrows():
            int_time = int(df_row['milliseconds']/1000)
            int_sec = int_time % 60
            int_time = int((int_time - int_sec) / 60)
            int_min = int_time % 60
            int_hour = int((int_time - int_sec) / 60)
            path_image = f"{int_hour:0{2}}-{int_min:0{2}}-{int_sec:0{2}}.jpg"

            div_new = html.Div([
                html.Div([
                    # https://getbootstrap.com/docs/4.0/layout/media-object/#alignment
                    html.Img(src=f"{session_data['url_media']}/{df_row['filename']}/{path_image}",
                                className="align-self-start mr-3", style={'max-width': '350px'}),
                    html.Div([
                        html.Div([
                            f"{df_row['franchise']}, S{df_row['season']}, E{df_row['episode']}"
                            ], className="mt-0 h6"),
                        html.Div([
                            f"{int_hour:0{2}}:{int_min:0{2}}:{int_sec:0{2}}"
                            ], className="p"),
                        html.Div([
                            f"{df_row['confidence']:{2.3}}% match"
                            ], className="p text-muted")

                            # "franchise": "Game of Thrones",
                            # "season": 1,
                            # "episode": 7,
                            # "title": "you win or die",
                            # "milliseconds": 1171000,
                            # "confidence": 5.485599,
                            # "filename": "ai_got_07_you_win_or_die_264675_PRO35_10-out.mp4"
                        ], className="media-body")
                    ], className="media col-xl-8 col-md-10 col-sm-12 mx-auto")
                ], id=f"result_{idx}", hidden=False, className="mb-3")
            # utils.logger.info(f"RESULT: {df_row}")
            list_return.append(div_new)
    return list_return


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
                    dbc.Tab(label='Event Coverage', tab_id='coverage'),
                    dbc.Tab(label='Extractor Comparison', tab_id='extractors'),
                    dbc.Tab(label='Visual Region', tab_id='regions'),
                    dbc.Tab(label='Search', tab_id='search'),
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
            dbc.Collapse([], id="core_filter", className="col-md-3 col-sm-12 border border-1 dark rounded mr-1 ml-1 border-dark"),
            dbc.Col([], id="core_tabs", className="border border-1 dark rounded mr-1 ml-1 border-dark")
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
    global _app_obj

    # validate trigger input
    def has_trigger(prop_name, is_prefix=False):
        """Quick check that name is in property inputs"""
        for obj in dash.callback_context.triggered:
            if obj['value'] is not None:
                if is_prefix:
                    if obj['prop_id'].startswith(prop_name): 
                        return obj
                else:
                    if obj['prop_id'].endswith(prop_name): 
                        return obj
        return None

    result_names = [f"result_{idx}" for idx in range(app.settings['result_count'])]


    @app.callback([Output('core_tabs', 'children'), Output('core_filter', 'children')],
                [Input('tab_selection', 'active_tab')])
    def render_content(tab):
        if tab == 'coverage':
            from . import coverage
            return [coverage.layout_generate(_app_obj), coverage.sidebar_generate(_app_obj)]
        elif tab == 'extractors':
            return [[], []] # tab1.layout
        elif tab == 'regions':
            return [[], []] # tab1.layout
        elif tab == 'search':
            return [[], []] # tab2.layout


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

