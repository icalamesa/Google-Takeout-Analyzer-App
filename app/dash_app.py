# dash_app.py
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd

from charts import create_custom_chart, CONFIG

df = pd.DataFrame({
    'Category': ['A', 'B', 'C', 'D'],
    'Values1': [4, 1, 3, 5],
    'Values2': [7, 2, 6, 4],
    'Values3': [3, 5, 2, 8],
    'Values4': [6, 9, 7, 1]
})

def init_dash_app(server, pathname):
    dash_app = dash.Dash(
        __name__,
        server=server,
        url_base_pathname=pathname
    )

    container_style = {
        'backgroundColor': CONFIG["background"]["paper_bgcolor"],
        'padding': '20px',
        'fontFamily': CONFIG["font"]["family"]
    }

    def chart_wrapper_style():
        return {
            'width': '48%',
            'display': 'inline-block',
            'verticalAlign': 'top',
            'margin': '10px 1%',
        }

    graph_style = {'height': '400px'}

    dash_app.layout = html.Div([
        html.H1(
            "Hello World",
            style={'color': CONFIG["font"]["color"], 'textAlign': 'center'}
        ),
        html.Div([
            html.Div([
                dcc.Dropdown(
                    id='dropdown-1',
                    options=[{'label': cat, 'value': cat} for cat in df['Category']],
                    value=None,
                    clearable=True
                ),
                dcc.Graph(id='chart-1', style=graph_style)
            ], style=chart_wrapper_style()),

            html.Div([
                dcc.Dropdown(
                    id='dropdown-2',
                    options=[{'label': cat, 'value': cat} for cat in df['Category']],
                    value=None,
                    clearable=True
                ),
                dcc.Graph(id='chart-2', style=graph_style)
            ], style=chart_wrapper_style()),
        ]),
        html.Div([
            html.Div([
                dcc.Dropdown(
                    id='dropdown-3',
                    options=[{'label': cat, 'value': cat} for cat in df['Category']],
                    value=None,
                    clearable=True
                ),
                dcc.Graph(id='chart-3', style=graph_style)
            ], style=chart_wrapper_style()),

            html.Div([
                dcc.Dropdown(
                    id='dropdown-4',
                    options=[{'label': cat, 'value': cat} for cat in df['Category']],
                    value=None,
                    clearable=True
                ),
                dcc.Graph(id='chart-4', style=graph_style)
            ], style=chart_wrapper_style()),
        ])
    ], style=container_style)

    @dash_app.callback(
        Output('chart-1', 'figure'),
        Input('dropdown-1', 'value')
    )
    def update_chart1(selected_category):
        filtered_df = df[df['Category'] == selected_category] if selected_category else df
        return create_custom_chart(filtered_df, x_col='Category', y_col='Values1', title="Chart 1", chart_type="bar")

    @dash_app.callback(
        Output('chart-2', 'figure'),
        Input('dropdown-2', 'value')
    )
    def update_chart2(selected_category):
        filtered_df = df[df['Category'] == selected_category] if selected_category else df
        return create_custom_chart(filtered_df, x_col='Category', y_col='Values2', title="Chart 2", chart_type="line")

    @dash_app.callback(
        Output('chart-3', 'figure'),
        Input('dropdown-3', 'value')
    )
    def update_chart3(selected_category):
        filtered_df = df[df['Category'] == selected_category] if selected_category else df
        return create_custom_chart(filtered_df, x_col='Category', y_col='Values3', title="Chart 3", chart_type="scatter")

    @dash_app.callback(
        Output('chart-4', 'figure'),
        Input('dropdown-4', 'value')
    )
    def update_chart4(selected_category):
        filtered_df = df[df['Category'] == selected_category] if selected_category else df
        return create_custom_chart(filtered_df, x_col='Category', y_col='Values4', title="Chart 4", chart_type="area")

    return dash_app
