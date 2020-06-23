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


import pandas as pd
import json
import tempfile
import shutil
from pathlib import Path

import dash
import plotly
import dash_core_components as dcc
import dash_html_components as html 
import dash_bootstrap_components as dbc 
import dash_table

from dash.dependencies import Input, Output, State

from quality.common import preprocessing, media

from quality.layout import utils
from quality.database import transforms

PAGE_SIZE = 50
SELECT_INVALID = "(invalid)"


def callback_create(app):
    """Create callbacks for the assets tab..."""

    # callback for turning on input options
    @app.callback(
        [Output('asset_group_disk', "is_open"), Output('asset_group_jobs', "is_open"), Output('asset_group_media', "is_open")],
        [Input('asset_add_source', 'value')], 
    )
    def update_inputs(source_add):
        list_valid = [False, False, True]
        if source_add == "disk":
            list_valid[0] = True
        elif source_add == "jobs":
            list_valid[1] = True
        return list_valid

    # callback for enabling add option
    @app.callback(
        [Output('asset_add', "color"), Output('asset_add', "disabled"), Output("extractor_count", "children")],
        [Input('asset_file', 'filename'), Input('extractor_file_type', 'value'), Input('asset_jobs', 'value'), 
         Input('asset_name', 'value'), Input('asset_quality', 'value'),
         Input('asset_add_source', 'value')]
    )
    def update_add_button(list_of_names, extractor_type, asset_jobs, asset_name, asset_quality, source_add):
        num_files = ""
        if list_of_names is not None and len(list_of_names):
            num_files = html.Small(f" ({len(list_of_names)} file(s))", className="text-muted")
        if asset_name is not None and len(asset_name) and asset_quality is not None and len(asset_quality):
            if (source_add == "disk" and extractor_type!=SELECT_INVALID and list_of_names is not None and len(list_of_names)) \
                    or (source_add == "jobs" and asset_jobs is not None and len(asset_jobs)):
                return ["success", False, num_files]
        return ["secondary", True, num_files]

    # callback for turning on rename and delete options
    @app.callback(
        [Output('asset_rename', "color"), Output('asset_rename', "disabled"), Output('asset_delete', "color"), Output('asset_delete', "disabled")],
        [Input('asset_add_source', 'value')]
    )
    def update_modify_button(source_add):
        return ["secondary", True, "secondary", True]

    # callback for adding new asset
    @app.callback(
        Output('asset_add_div', 'children'),
        [Input('asset_add', 'n_clicks')],
        [State('asset_file', 'contents'), State('asset_file', 'filename'), State('asset_file', 'last_modified'), 
         State('extractor_file_type', 'value'), 
         State('asset_name', 'value'), State('asset_quality', 'value'), State('asset_episode', 'value'), State('asset_url', 'value'), 
         State('asset_jobs', 'value'), State('asset_add_source', 'value'), State('session', 'data')]
    )
    def asset_add(n_clicks, list_of_contents, list_of_names, list_of_dates, 
                    extractor_type, asset_name, asset_quality, asset_episode, asset_url,
                    asset_jobs, source_add, session_data):
        if n_clicks is None:
            return []

        df = None
        app.logger.info(f"ADD_ASSET: {[n_clicks, list_of_names, list_of_dates, extractor_type, asset_name, asset_quality, asset_episode, asset_url, asset_jobs, source_add, session_data]}")
        if list_of_names is not None and len(list_of_names):
            path_temp = tempfile.mkdtemp()  # get temp dir
            path_extractor = Path(path_temp).joinpath(extractor_type)
            path_extractor.mkdir(parents=True)  # create nested extractor dir

            list_output = [    # example from here - https://dash.plotly.com/dash-core-components/upload
                media.parse_upload_contents(str(path_extractor), c, n, d) for c, n, d in
                zip(list_of_contents, list_of_names, list_of_dates)]
            app.logger.info(f"ADD_ASSET (uploaded files): {list_output} (extractor: {extractor_type})")
            df = preprocessing.data_parse_callback(path_temp, extractor_type, fn_callback=None, verbose=True)
            shutil.rmtree(path_temp)   # cleanup

        if df is not None:
            dict_asset = {"title_name": asset_name, "dataset": "default", "episode_id": asset_episode, 
                            "content_quality": asset_quality, "media_url": asset_url }   # content_id is auto-gen here
            app.logger.info(f"ADD_ASSET (event teaser): {dict_asset, df})")

            content_id = transforms.asset_create(session_data, app.logger, dict_asset, df, delete_prior=False)
            return [html.Span(f"New ContentID: {content_id}")]
        return [html.Span(f"Content Add Error")]

    # callback for refreshing list of assets
    @app.callback(
        Output('asset_list_div', 'children'),
        [Input('asset_refresh', 'n_clicks')],
        [State('session', 'data')]
    )
    def asset_refresh(n_clicks, session_data):
        div_empty = [html.I("No valid events or assets available."),
            html.Pre(f"Configuration: {json.dumps(session_data, indent=4, sort_keys=True)}", 
                     className="text-muted small bg-light border p-1 mt-2 border-dark rounded")]
        if n_clicks is None:
            return div_empty
        df_result = transforms.asset_retrieve(session_data, app.logger)
        app.logger.info(f"LIST: {df_result}")
        if len(df_result):
            return dbc.Table(df_result, striped=True, bordered=True, hover=True, id="table_assets")
        return div_empty


def sidebar_generate(app):
    dict_parsers = preprocessing.data_parser_list()
    list_parsers = [{"label":"(select an extractor)", "value":SELECT_INVALID}] + [{"label":k, "value":k} for k in dict_parsers]

    return [
        dbc.Row(dbc.Col(dbc.Button("Refresh", id="asset_refresh", block=True, outline=False, color="primary")), className="m-2" ),
        dbc.Row(dbc.Col(dbc.Button("Rename", id="asset_rename", block=True, outline=False, color="secondary")), className="m-2" ),
        dbc.Row(dbc.Col(dbc.Button("Delete", id="asset_delete", block=True, outline=False, color="secondary")), className="m-2" ),
        dbc.Row(dbc.Col([
            dbc.Row(dbc.Col(
                dbc.FormGroup([
                    dbc.Label("New Asset Source"),
                    dbc.RadioItems(
                        options=[
                            {"label": "Upload Extractor Outputs", "value": "disk"},
                            {"label": "ContentAI Jobs", "value": "jobs"},
                            {"label": "ContentAI URI", "value": "uri", "disabled": True},
                        ],
                        value="disk", id="asset_add_source"),
                    ])
                ), className="mb-1"),
            dbc.Row(dbc.Col(
                dbc.Collapse([
                    dbc.FormGroup( [
                        dbc.Label("Extractor Type"),
                        dbc.Select(id='extractor_file_type', bs_size="sm", options=list_parsers, value=SELECT_INVALID),
                        ]),
                    dbc.FormGroup( [
                        dbc.Label([html.Span("Extractor Output Files"), html.Span("", id="extractor_count")]),
                        dcc.Upload(
                            id='asset_file', multiple=True,  # Allow multiple files to be uploaded
                            children=html.Div([
                                dbc.Button("Upload", block=True, size="sm", outline=False, color="primary")
                            ])),
                        ]),
                    ], id="asset_group_disk", is_open=False)
                ), className="mb-1"),
            dbc.Row(dbc.Col(
                dbc.Collapse(
                    dbc.FormGroup( [
                        dbc.Label("ContentAI Jobs"),
                        dbc.Input(placeholder="asset job numbers", type="text", bs_size="sm", id='asset_jobs'),
                        dbc.FormText("comma-separated ContentAI job identifiers"),
                        ]),
                    id="asset_group_jobs", is_open=False)
                ), className="mb-1"),
            dbc.Row(dbc.Col(
                dbc.Collapse([
                    dbc.Label("Asset Information"),
                    dbc.InputGroup( [
                        dbc.InputGroupAddon("Name", addon_type="prepend", className="font-weight-bold"),
                        dbc.Input(placeholder="asset name", type="text", id='asset_name'),
                        ], size="sm", className="mb-1"),
                    dbc.InputGroup( [
                        dbc.InputGroupAddon("Quality", addon_type="prepend", className="font-weight-bold"),
                        dbc.Input(placeholder="asset quality description", type="text", id='asset_quality'),
                        ], size="sm", className="mb-1"),
                    dbc.InputGroup( [
                        dbc.InputGroupAddon("Episode", addon_type="prepend"),
                        dbc.Input(placeholder="(opt) episode information", type="text", id='asset_episode'),
                        ], size="sm", className="mb-1"),
                    dbc.InputGroup( [
                        dbc.InputGroupAddon("Media Url", addon_type="prepend"),
                        dbc.Input(placeholder="(opt) media URL", type="text", id='asset_url'),
                        dbc.FormText("media url for retrieval of asset frames in UX"),
                        ], size="sm", className="mb-1"),
                    ], id="asset_group_media", is_open=True)
                ), className="mb-1"),
            dbc.Row( dbc.Col([
                dbc.Button("Add", id="asset_add", block=True, color="secondary", outline=False), 
                html.Div(id='asset_add_div')]), className="mb-1 text-muted small" ),
        ]), className="m-1 border border-1 dark rounded")
    ] 


def layout_generate(app):
    df = pd.DataFrame()
    return [
        dbc.Row(dbc.Col([
            html.H2("Available Assets"),
            html.Div(id="asset_list_div")
            ]), className="m-2"),
        dbc.Row(dbc.Col(className="h-100"))
    ]

