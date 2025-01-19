import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd
import plotly.graph_objects as go
import dash_bootstrap_components as dbc

from charts import create_custom_chart, CONFIG
import database

df = pd.DataFrame({
    'Category': ['A', 'B', 'C', 'D'],
    'Values1': [4, 1, 3, 5],
    'Values2': [7, 2, 6, 4],
    'Values3': [3, 5, 2, 8],
    'Values4': [6, 9, 7, 1]
})

def generate_dynamic_rows_kpi(df):
    rows = []
    for _, row in df.iterrows():
        label = row[0]
        value = row[1]
        style = row[2] if len(row) > 2 else {}
        rows.append(
            dbc.Row(
                [
                    dbc.Col(html.P(label, className="text-muted"), width=8),
                    dbc.Col(html.P(value, style=style), width=4),
                ],
                align="center"
            )
        )
    return rows

def init_dash_app(server, pathname, TakeoutDatabase):
    dash_app = dash.Dash(
        __name__,
        server=server,
        url_base_pathname=pathname,
        external_stylesheets=[dbc.themes.BOOTSTRAP]
    )

    container_style = {
        'backgroundColor': CONFIG["background"]["paper_bgcolor"],
        'padding': '20px',
        'fontFamily': CONFIG["font"]["family"],
        'minHeight': '100vh'
    }

    graph_style = {'height': '300px'}

    dash_app.layout = dbc.Container(
        fluid=True,
        style=container_style,
        children=[
            dbc.Row(
                className="d-flex align-items-center justify-content-center",
                style={
                    "padding": "1.5rem 0",
                    "marginBottom": "0rem"
                },
                children=[
                    dbc.Col(
                        html.Div(
                            [
                                html.H1(
                                    f"Welcome, {TakeoutDatabase.query_data('select MAX(FormattedName) as name from clean_profiles')['name'].iloc[0]}",
                                    className="text-center",
                                    style={
                                        "color": CONFIG["font"]["color"],
                                        "fontSize": "1.8rem",
                                        "fontWeight": "600",
                                        "margin": "0"
                                    },
                                ),
                                html.Hr(
                                    className="align-items-center justify-content-center",
                                    style={
                                        "border": "0",
                                        "borderTop": "2px solid #d6d6d6",
                                        "width": "100%",
                                        "margin-top": "1.7rem",
                                        "margin-right": "1.7rem",
                                    }
                                )
                            ]
                        ),
                        width="90%"
                    )
                ]
            ),
            dbc.Row(className="align-items-center justify-content-between",
                children=[
                    dbc.Col(
                        dcc.Dropdown(
                            id="platform-filter",
                            options=[
                                {"label": platform, "value": platform}
                                for platform in TakeoutDatabase.query_data(
                                    "SELECT DISTINCT platform FROM clean_activity_history"
                                )["platform"]
                            ],
                            placeholder="Select Platform",
                            multi=True,
                            style={"marginBottom": "1rem", "marginRight": "0rem"}
                        ),
                        width=4,
                    ),
                    dbc.Col(
                        dcc.DatePickerRange(
                            id="date-filter",
                            start_date=TakeoutDatabase.query_data("SELECT MIN(activity_timestamp) as start FROM clean_activity_history")["start"].iloc[0],
                            end_date=TakeoutDatabase.query_data("SELECT MAX(activity_timestamp) as end FROM clean_activity_history")["end"].iloc[0],
                            display_format="YYYY-MM-DD",
                            style={"marginBottom": "1rem"}
                        ),
                        width=8,
                    ),
                ],
                style={"marginRight": "2rem", "marginLeft": "2rem", "marginBottom": "1rem"}
            ),
            dbc.Row(
                className="d-flex align-items-start justify-content-between",
                style={"minHeight": "13rem", "margin-left": "2rem", "margin-right": "2rem"},
                children=[
                    dbc.Col(
                        children=dbc.Card(
                            [
                                dbc.CardHeader("Available Activity History"),
                                dbc.CardBody([
                                    dbc.Row(
                                        children=[
                                            dbc.Col(
                                                html.H2(
                                                    id="kpi-total-count",
                                                    className="card-title"
                                                ),
                                                width="auto"
                                            ),
                                            dbc.Col(
                                                html.Span("↑ 22%", style={"color": "green", "fontWeight": "bold"}),
                                                width="auto"
                                            ),
                                        ],
                                        align="center"
                                    ),
                                    html.Hr(),
                                    dbc.Col(
                                        style={"margin-top": "1rem"},
                                        children=html.Div(id="kpi-rows")
                                    )
                                ])
                            ]
                        ),
                        width=4,
                    ),
                    dbc.Col(
                        dcc.Graph(id='chart-1', style={"height": "350px"}),
                        width=8,
                    )
                ]
            ),
        ]
    )

    @dash_app.callback(
        [Output('kpi-total-count', 'children'), Output('kpi-rows', 'children')],
        [Input('platform-filter', 'value'), Input('date-filter', 'start_date'), Input('date-filter', 'end_date')]
    )
    def update_kpi(platform_filter, start_date, end_date):
        query = f"""
        SELECT platform, COUNT(*) AS count 
        FROM clean_activity_history 
        WHERE activity_timestamp BETWEEN '{start_date}' AND '{end_date}'
        """
        if platform_filter:
            query += f" AND platform IN ({', '.join([f"'{p}'" for p in platform_filter])})"
        query += " GROUP BY platform ORDER BY count DESC"
        df_kpi = TakeoutDatabase.query_data(query)
        total_count = df_kpi["count"].sum()
        rows = generate_dynamic_rows_kpi(df_kpi)
        return total_count, rows

    @dash_app.callback(
        Output('chart-1', 'figure'),
        [Input('platform-filter', 'value'), Input('date-filter', 'start_date'), Input('date-filter', 'end_date')]
    )
    def update_chart1(platform_filter, start_date, end_date):
        query = f"""
        SELECT 
            YEAR(activity_timestamp) AS period_year,
            MONTH(activity_timestamp) AS month_no,
            CONCAT(YEAR(activity_timestamp), '-', MONTH(activity_timestamp)) AS period,
            COUNT(*) AS count
        FROM clean_activity_history
        WHERE activity_timestamp BETWEEN '{start_date}' AND '{end_date}'
        """
        if platform_filter:
            query += f" AND platform IN ({', '.join([f"'{p}'" for p in platform_filter])})"
        query += """
        GROUP BY 1, 2
        ORDER BY YEAR(activity_timestamp) ASC, MONTH(activity_timestamp) ASC
        """
        filtered_df = TakeoutDatabase.query_data(query)
        return create_custom_chart(
            filtered_df,
            x_col='period',
            y_col='count',
            title="Activity Timeline",
            chart_type="bar"
        )

    return dash_app


    @dash_app.callback(
        Output('chart-2', 'figure'),
        Input('chart-2', 'id')
    )
    def update_chart2(_):
        query = (
            """
            SELECT 
                DATE_TRUNC('month', activity_timestamp) AS period,
                AVG(EXTRACT(HOUR FROM activity_timestamp)) AS avg_hour,
                STDDEV(EXTRACT(HOUR FROM activity_timestamp)) AS std_hour
            FROM clean_activity_history
            WHERE activity_timestamp IS NOT NULL
            GROUP BY 1
            ORDER BY 1
            """
        )
        df_line = TakeoutDatabase.query_data(query)
        if df_line.empty:
            return go.Figure(data=[], layout=go.Layout(title="No Data Found"))
        
        return create_custom_chart(
            df_line,
            x_col="period",
            y_col="avg_hour",
            title="Average Watch Timestamp (with Std Dev)",
            chart_type="line_error",
            error_col="std_hour"
        )

    @dash_app.callback(
        Output('chart-3', 'figure'),
        Input('chart-3', 'id')
    )
    def update_chart3(_):
        filtered_df = df
        return create_custom_chart(
            filtered_df,
            x_col='Category',
            y_col='Values3',
            title="Chart 3",
            chart_type="scatter"
        )

    @dash_app.callback(
        Output('chart-4', 'figure'),
        Input('chart-4', 'id')
    )
    def update_chart4(_):
        filtered_df = df
        return create_custom_chart(
            filtered_df,
            x_col='Category',
            y_col='Values4',
            title="Chart 4",
            chart_type="area"
        )

    return dash_app
