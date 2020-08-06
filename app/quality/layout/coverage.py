import dash
import plotly
import dash_core_components as dcc
import dash_html_components as html 
import dash_bootstrap_components as dbc 
import dash_table
import pandas as pd
from dash.dependencies import Input, Output

from . import utils


PAGE_SIZE = 50
#  html.Div([
#         dbc.Row([dbc.Col(html.Div(html.P("A single, half-width column")),style = {'padding':'50px'})
#                 ,dbc.Col(

def sidebar_generate(_app_obj):
    return [
        html.Div([
            html.Div([
                html.Div("filter list", className='text-left text-muted '),
                ] + utils.generate_filters(_app_obj.store), className="mb-1 col")
            ], className="row  m-0"),
        html.Div([
            html.Div([
                html.Div("filters", className='text-left text-muted'),
                dcc.Dropdown(options=[
                    ],
                    placeholder="select a filter", multi=True,
                    value="_empty", id="filter_suffix", className="pb-1")
                ], className="col ")
            ], className="row m-0 border border-bottom-0 border-right-0  border-left-0 border-dark")
        ]


def layout_generate(_app_obj):
    return [
        html.Div([ 
            html.Div([ 
                html.Div([
                    html.Div([
                        html.Div([
                            # icon gallery - https://fontawesome.com/icons?d=gallery
                            html.Span("(results: 0) ", id="result_count"),
                        ], className="float-right"),
                        ], className="col")
                    ], className="row small text-muted "),   # counter wrapper
                html.Div([ 
                    html.Div([]
                        , className="col ", id="panel_results"),
                    ], className="row ")
                ], className="col ")  # result column wrapper
            ], className="row ")  # result row wrapper
        ]

