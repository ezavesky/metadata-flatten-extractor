import dash
import plotly
import dash_core_components as dcc
import dash_html_components as html 
import dash_bootstrap_components as dbc 
import dash_table
import pandas as pd
from dash.dependencies import Input, Output, State

from quality.layout import utils
from quality.database import transforms

PAGE_SIZE = 50


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
        [Output('asset_add', "color"), Output('asset_add', "disabled")],
        [Input('asset_file_path', 'value'), Input('asset_jobs', 'value'), Input('asset_add_source', 'value')]
    )
    def update_add_button(file_path, asset_jobs, source_add):
        if (source_add == "disk" and file_path is not None and len(file_path)) \
                or (source_add == "jobs" and asset_jobs is not None and len(asset_jobs)):
            return ["success", False]
        return ["secondary", True]

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
        [State('asset_file_path', 'value'), State('asset_jobs', 'value'), State('asset_add_source', 'value'), State('session', 'data')]
    )
    def asset_add(n_clicks, file_path, asset_jobs, source_add, session_data):
        app.logger.info(f"ADD: {n_clicks, file_path, asset_jobs, source_add, session_data}")
        if n_clicks is None:
            return []
        return []

    # callback for refreshing list of assets
    @app.callback(
        Output('asset_list_div', 'children'),
        [Input('asset_refresh', 'n_clicks')],
        [State('session', 'data')]
    )
    def asset_refresh(n_clicks, session_data):
        app.logger.info(f"REFRESH: {session_data}")
        if n_clicks is None:
            return []
        df_result = transforms.asset_retrieve(session_data, app.logger)
        app.logger.info(f"LIST: {df_result}")
        return dbc.Table(df_result, striped=True, bordered=True, hover=True, id="table_assets")



def sidebar_generate(app):
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
                            {"label": "Disk-Based Import", "value": "disk"},
                            {"label": "ContentAI Jobs", "value": "jobs"},
                            {"label": "ContentAI URI", "value": "uri", "disabled": True},
                        ],
                        value="disk", id="asset_add_source"),
                    ])
                ), className="mb-1"),
            dbc.Row(dbc.Col(
                dbc.Collapse(
                    dbc.FormGroup( [
                        dbc.Label("Asset Metadata Path"),
                        dbc.Input(placeholder="asset input directory", type="text", id='asset_file_path'),
                        dbc.FormText("absolute path for disk-based import"),
                        ]),
                    id="asset_group_disk", is_open=False)
                ), className="mb-1"),
            dbc.Row(dbc.Col(
                dbc.Collapse(
                    dbc.FormGroup( [
                        dbc.Label("ContentAI Jobs"),
                        dbc.Input(placeholder="asset job numbers", type="text", id='asset_jobs'),
                        dbc.FormText("one or more comma-separated ContentAI job identifiers"),
                        ]),
                    id="asset_group_jobs", is_open=False)
                ), className="mb-1"),
            dbc.Row(dbc.Col(
                dbc.Collapse(
                    dbc.FormGroup( [
                        dbc.Label("Asset Media Url"),
                        dbc.Input(placeholder="asset media URL", type="text", id='asset_file_media'),
                        dbc.FormText("media url for retrieval of asset frames in UX"),
                        ]),
                    id="asset_group_media", is_open=True)
                ), className="mb-1"),
            dbc.Row( dbc.Col([dbc.Button("Add", id="asset_add", block=True, color="secondary", outline=False), html.Div(id='asset_add_div')]), className="mb-1" ),
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

