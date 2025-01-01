import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd

# Assuming 'charts' is a module in your local directory
try:
    import charts  # Ensure you have a `charts.py` file in the current directory.
except ImportError:
    raise ImportError("Could not import the 'charts' module. Ensure it exists in the current directory.")

# Step 1: Data Preparation
df = pd.DataFrame({
    'Category': ['A', 'B', 'C', 'D'],
    'Values1': [4, 1, 3, 5],
    'Values2': [7, 2, 6, 4],
    'Values3': [3, 5, 2, 8],
    'Values4': [6, 9, 7, 1]
})

# Step 2: App Initialization
app = dash.Dash(__name__)

# Step 3: Define the Layout
app.layout = html.Div([
    html.H1("Dashboard with Multiple Charts and Dropdowns"),
    html.Div([
        # Chart 1
        html.Div([
            dcc.Dropdown(
                id='dropdown-1',
                options=[
                    {'label': category, 'value': category}
                    for category in df['Category']
                ],
                value=None,
                clearable=True
            ),
            dcc.Graph(id='chart-1')
        ], style={'width': '48%', 'display': 'inline-block'}),
        
        # Chart 2
        html.Div([
            dcc.Dropdown(
                id='dropdown-2',
                options=[
                    {'label': category, 'value': category}
                    for category in df['Category']
                ],
                value=None,
                clearable=True
            ),
            dcc.Graph(id='chart-2')
        ], style={'width': '48%', 'display': 'inline-block'}),
    ]),

    html.Div([
        # Chart 3
        html.Div([
            dcc.Dropdown(
                id='dropdown-3',
                options=[
                    {'label': category, 'value': category}
                    for category in df['Category']
                ],
                value=None,
                clearable=True
            ),
            dcc.Graph(id='chart-3')
        ], style={'width': '48%', 'display': 'inline-block'}),
        
        # Chart 4
        html.Div([
            dcc.Dropdown(
                id='dropdown-4',
                options=[
                    {'label': category, 'value': category}
                    for category in df['Category']
                ],
                value=None,
                clearable=True
            ),
            dcc.Graph(id='chart-4')
        ], style={'width': '48%', 'display': 'inline-block'}),
    ])
])


# Updated Callbacks with Custom Chart Function

@app.callback(
    Output('chart-1', 'figure'),
    Input('dropdown-1', 'value')
)
def update_chart1(selected_category):
    filtered_df = df[df['Category'] == selected_category] if selected_category else df
    return charts.create_custom_chart(filtered_df, x_col='Category', y_col='Values1', title="Chart 1", chart_type="bar")


@app.callback(
    Output('chart-2', 'figure'),
    Input('dropdown-2', 'value')
)
def update_chart2(selected_category):
    filtered_df = df[df['Category'] == selected_category] if selected_category else df
    return charts.create_custom_chart(filtered_df, x_col='Category', y_col='Values2', title="Chart 2", chart_type="line")


@app.callback(
    Output('chart-3', 'figure'),
    Input('dropdown-3', 'value')
)
def update_chart3(selected_category):
    filtered_df = df[df['Category'] == selected_category] if selected_category else df
    return charts.create_custom_chart(filtered_df, x_col='Category', y_col='Values3', title="Chart 3", chart_type="scatter")


@app.callback(
    Output('chart-4', 'figure'),
    Input('dropdown-4', 'value')
)
def update_chart4(selected_category):
    filtered_df = df[df['Category'] == selected_category] if selected_category else df
    return charts.create_custom_chart(filtered_df, x_col='Category', y_col='Values4', title="Chart 4", chart_type="area")


# Run the App
if __name__ == '__main__':
    app.run_server(debug=True)
