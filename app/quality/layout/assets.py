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
        # app.logger.info(f"ADD_ASSET: {[n_clicks, list_of_names, list_of_dates, extractor_type, asset_name, asset_quality, asset_episode, asset_url, asset_jobs, source_add, session_data]}")
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
            dict_asset = {"title_name": asset_name, "dataset": ["default"], "episode_id": asset_episode, 
                            "quality": asset_quality, "media_url": asset_url }   # content_id is auto-gen here
            app.logger.info(f"ADD_ASSET (event teaser): {dict_asset, df})")

            transforms.intialize(session_data, app.logger, False)   # onetime init of database?
            content_id, num_unique = transforms.asset_create(dict_asset, df)
            return [html.Span(f"New ContentID: {content_id}")]
        return [html.Span(f"Content Add Error")]

    # callback for refreshing list of assets
    @app.callback(
        Output('asset_list_div', 'children'),
        [Input('asset_refresh', 'n_clicks')],
        [State('session', 'data')]
    )
    def asset_refresh(n_clicks, session_data):
        div_update = html.Div(f"(updated {utils.dt_format()})", className="small text-muted")
        div_empty = [html.I(f"No valid events or assets available."), div_update,
                     html.Pre(f"Configuration: {json.dumps(session_data, indent=4, sort_keys=True)}", 
                            className="text-muted small bg-light border p-1 mt-2 border-dark rounded")]
        if n_clicks is None:
            return div_empty
        transforms.intialize(session_data, app.logger, False)   # onetime init of database?
        df, num_result = transforms.asset_retrieve()
        if not len(df):
            return div_empty
        # df["dataset"] = df["dataset"].apply(lambda x: ",".join(x))
        # print(df)
        return [div_update,
            html.Div(id='datatable-interactivity-container'),
            dash_table.DataTable(   # formatting madness -- https://dash.plotly.com/datatable/width
                id='datatable-interactivity',
                columns=[
                    {"name": i, "id": i, "deletable": False, "selectable": False} for i in df.columns
                ],
                data=df.to_dict('records'),
                derived_virtual_data=df.to_dict(orient='records'),
                editable=False,
                filter_action="native",
                sort_action="native",
                sort_mode="multi",
                column_selectable=False,
                row_selectable="single",
                row_deletable=False,
                selected_columns=[],
                selected_rows=[],
                page_action="native",
                page_current= 0,
                page_size= 10,
                # fixed_rows={'headers': True},
                # style_cell_conditional=[
                #     {
                #         'if': {'column_id': c},
                #         'textAlign': 'left'
                #     } for c in ['Date', 'Region']
                # ],
                style_data_conditional=[
                    {
                        'if': {'row_index': 'odd'},
                        'backgroundColor': 'rgb(248, 248, 248)'
                    }
                ],
                style_header={
                    'backgroundColor': 'rgb(230, 230, 230)',
                    'fontWeight': 'bold'
                },
                # style_table={'overflowX': 'auto'},                
                style_cell={
                    'height': 'auto',
                    # all three widths are needed
                    'minWidth': '95px', 'width': '95px', 'maxWidth': '95px',
                    'overflow': 'hidden', 'textOverflow': 'ellipsis',
                    'whiteSpace': 'normal'
                }),
            ]

    @app.callback(
        [Output('datatable-interactivity-container', "children"),
         Output('asset_rename', "color"), Output('asset_rename', "disabled"),
         Output('asset_delete', "color"), Output('asset_delete', "disabled")],
        [Input('datatable-interactivity', "derived_virtual_data"),
         Input('datatable-interactivity', "derived_virtual_selected_rows")])
    def update_graphs(rows, selected_rows):
        # When the table is first rendered, `derived_virtual_data` and
        # `derived_virtual_selected_rows` will be `None`. This is due to an
        # idiosyncracy in Dash (unsupplied properties are always None and Dash
        # calls the dependent callbacks when the component is first rendered).
        # So, if `rows` is `None`, then the component was just rendered
        # and its value will be the same as the component's dataframe.
        # Instead of setting `None` in here, you could also set
        # `derived_virtual_data=df.to_rows('dict')` when you initialize
        # the component.

        list_results = [[], "secondary", True, "secondary", True]
        if selected_rows is None or not selected_rows:
            return list_results

        selected_rows = rows[selected_rows[0]]

        # table_header = [
        #     html.Thead(html.Tr([html.Th("First Name"), html.Th("Last Name")]))
        # ]
        table_rows = html.Tbody([ html.Tr([html.Th(k), html.Td(selected_rows[k])]) for k in selected_rows ])
        list_results[0] = [html.H5(f"Selected Asset: {selected_rows['title_name']}"), 
                            dbc.Table(table_rows, bordered=True, hover=True, responsive=True, striped=True, size='sm')]
        list_results[2] = list_results[4] = False
        list_results[1] = list_results[3] = "danger"
        return list_results


def sidebar_generate(app):
    dict_parsers = preprocessing.data_parser_list()
    list_parsers = [{"label":"(select an extractor)", "value":SELECT_INVALID}] + [{"label":k, "value":k} for k in dict_parsers]

    return [
        dbc.Row(dbc.Col(dbc.Button("Refresh", id="asset_refresh", block=True, outline=False, color="primary")), className="m-2" ),
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
                dbc.Button("Add", id="asset_add", block=True, color="secondary", disabled=True, outline=False), 
                html.Div(id='asset_add_div')]), className="mb-1 text-muted small" ),
        ]), className="m-1 border border-1 dark rounded"),
        dbc.Row(dbc.Col(dbc.Button("Change Datasets", id="asset_rename", block=True, outline=False, disabled=True, color="secondary")), className="m-2" ),
        dbc.Row(dbc.Col(dbc.Button("Delete", id="asset_delete", block=True, outline=False, disabled=True, color="secondary")), className="m-2" ),
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

