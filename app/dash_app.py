import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
from charts import create_custom_chart, CONFIG
from data_interface import GoogleTakeoutProcessor  
import os

takeout_processor = None

def generate_dynamic_rows_kpi(df):
    """Generate dynamic rows for KPI cards."""
    rows = []
    for _, row in df.iterrows():
        label = row['platform']
        value = row['count']
        rows.append(
            dbc.Row(
                [
                    dbc.Col(html.P(label, className="text-muted"), width=8),
                    dbc.Col(html.P(value, className="font-weight-bold"), width=4),
                ],
                align="center"
            )
        )
    return rows

def init_dash_app(server, pathname, takeout_processor):
    """Initialize the Dash application."""
    dash_app = dash.Dash(
        __name__,
        server=server,
        url_base_pathname=pathname,
        external_stylesheets=[dbc.themes.BOOTSTRAP]
    )

    takeout_processor = takeout_processor
    container_style = {
        'backgroundColor': CONFIG["background"]["paper_bgcolor"],
        'padding': '20px',
        'fontFamily': CONFIG["font"]["family"],
        'minHeight': '100vh'
    }

    dash_app.layout = dbc.Container(
        fluid=True,
        style=container_style,
        children=[
            # Header
            dbc.Row(
                className="d-flex align-items-center justify-content-center",
                style={"padding": "1.5rem 0", "marginBottom": "0rem"},
                children=[
                    dbc.Col(
                        html.Div(
                            [
                                html.H1(
                                    f"Welcome, {takeout_processor.query_data('SELECT MAX(FormattedName) as name FROM clean_profiles')['name'].iloc[0]}",
                                    className="text-center",
                                    style={"color": CONFIG["font"]["color"], "fontSize": "1.8rem", "fontWeight": "600"}
                                ),
                                html.Hr(
                                    style={
                                        "border": "0",
                                        "borderTop": "2px solid #d6d6d6",
                                        "width": "100%",
                                        "marginTop": "1.7rem"
                                    }
                                )
                            ]
                        ),
                        width="90%"
                    )
                ]
            ),
            # Filters
            dbc.Row(
                className="align-items-center justify-content-between",
                style={"marginRight": "2rem", "marginLeft": "2rem", "marginBottom": "1rem"},
                children=[
                    dbc.Col(
                        dcc.Dropdown(
                            id="platform-filter",
                            options=[
                                {"label": platform, "value": platform}
                                for platform in takeout_processor.query_data(
                                    "SELECT DISTINCT platform FROM clean_activity_history"
                                )["platform"]
                            ],
                            placeholder="Select Platform",
                            multi=True,
                            style={"marginBottom": "1rem"}
                        ),
                        width=4,
                    ),
                    dbc.Col(
                        dcc.DatePickerRange(
                            id="date-filter",
                            start_date=takeout_processor.query_data("SELECT MIN(activity_timestamp) AS start FROM clean_activity_history")["start"].iloc[0],
                            end_date=takeout_processor.query_data("SELECT MAX(activity_timestamp) AS end FROM clean_activity_history")["end"].iloc[0],
                            display_format="YYYY-MM-DD",
                            style={"marginBottom": "1rem"}
                        ),
                        width=8,
                    )
                ]
            ),
            # KPI and Graphs
            dbc.Row(
                className="d-flex align-items-start justify-content-between",
                style={"minHeight": "13rem", "marginLeft": "2rem", "marginRight": "2rem"},
                children=[
                    # KPI Card
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader("Available Activity History"),
                                dbc.CardBody([
                                    dbc.Row(
                                        [
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
                                    html.Div(id="kpi-rows", style={"marginTop": "1rem"})
                                ])
                            ]
                        ),
                        width=4,
                    ),
                    # Chart
                    dbc.Col(
                        dcc.Graph(id='chart-1', style={"height": "350px"}),
                        width=8,
                    )
                ]
            ),
        ]
    )

    # Callbacks
    @dash_app.callback(
        [Output('kpi-total-count', 'children'), Output('kpi-rows', 'children')],
        [Input('platform-filter', 'value'), Input('date-filter', 'start_date'), Input('date-filter', 'end_date')]
    )
    def update_kpi(platform_filter, start_date, end_date):
        """Update KPI metrics based on filters."""
        query = f"""
        SELECT platform, COUNT(*) AS count 
        FROM clean_activity_history 
        WHERE activity_timestamp BETWEEN '{start_date}' AND '{end_date}'
        """
        if platform_filter:
            query += f" AND platform IN ({', '.join([f"'{p}'" for p in platform_filter])})"
        query += " GROUP BY platform ORDER BY count DESC"
        df_kpi = takeout_processor.query_data(query)
        total_count = df_kpi["count"].sum()
        rows = generate_dynamic_rows_kpi(df_kpi)
        return total_count, rows

    @dash_app.callback(
        Output('chart-1', 'figure'),
        [Input('platform-filter', 'value'), Input('date-filter', 'start_date'), Input('date-filter', 'end_date')]
    )
    def update_chart1(platform_filter, start_date, end_date):
        """Update Chart 1 based on filters."""
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
        ORDER BY period_year ASC, month_no ASC
        """
        filtered_df = takeout_processor.query_data(query)
        return create_custom_chart(
            filtered_df,
            x_col='period',
            y_col='count',
            title="Activity Timeline",
            chart_type="bar"
        )

    return dash_app
