# config.py
import plotly.graph_objects as go

CONFIG = {
    "font": {
        "family": "Montserrat, sans-serif",
        "size": 15,
        "color": "#3b3b3b" 
    },
    "marker": {
        "size": 12,
        "line_width": 0.5
    },
    "line": {
        "width": 2
    },
    "axis": {
        "line_color": "#3b3b3b",
        "line_width": 2
    },
    "background": {
        "plot_bgcolor": "rgba(0,0,0,0)", 
        "paper_bgcolor": "#fff1e0"        
    },
    "margin": {
        "l": 40, "r": 40, "t": 40, "b": 40
    },
    "accent_color": "#00bcd4",
    "accent_border": "#3b3b3b" 
}


def create_custom_chart(data, x_col, y_col, title, chart_type="bar", color=None, border_color=None):
    """
    Create a Plotly chart with a clear (transparent) plot area and a paper background
    that matches the dashboard's background (#fff1e0). A border (via a CSS wrapper in the layout)
    can provide the rounded effect.
    
    Parameters:
        data (pd.DataFrame): Data for the chart.
        x_col (str): Column for the x-axis.
        y_col (str): Column for the y-axis.
        title (str): Chart title.
        chart_type (str): 'bar', 'line', 'scatter', or 'area'.
        color (str): Primary color for data elements (defaults to CONFIG accent).
        border_color (str): Border color for markers (defaults to CONFIG accent_border).
    
    Returns:
        go.Figure: A configured Plotly figure.
    """
    if color is None:
        color = CONFIG["accent_color"]
    if border_color is None:
        border_color = CONFIG["accent_border"]

    marker_config = dict(
        color=color,
        size=CONFIG["marker"]["size"],
        line=dict(color=border_color, width=CONFIG["marker"]["line_width"])
    )

    if chart_type == "bar":
        fig = go.Figure(
            data=[
                go.Bar(
                    x=data[x_col],
                    y=data[y_col],
                    marker=dict(color=color, line=dict(color=border_color, width=CONFIG["marker"]["line_width"])),
                    text=data[y_col],
                    textfont=CONFIG["font"],
                    textposition="outside"
                )
            ]
        )
    elif chart_type == "line":
        fig = go.Figure(
            data=[
                go.Scatter(
                    x=data[x_col],
                    y=data[y_col],
                    mode="lines+markers+text",
                    line=dict(color=color, width=CONFIG["line"]["width"]),
                    marker=marker_config,
                    text=data[y_col],
                    textfont=CONFIG["font"],
                    textposition="top center"
                )
            ]
        )
    elif chart_type == "scatter":
        fig = go.Figure(
            data=[
                go.Scatter(
                    x=data[x_col],
                    y=data[y_col],
                    mode="markers+text",
                    marker=marker_config,
                    text=data[y_col],
                    textfont=CONFIG["font"],
                    textposition="top center"
                )
            ]
        )
    elif chart_type == "area":
        fig = go.Figure(
            data=[
                go.Scatter(
                    x=data[x_col],
                    y=data[y_col],
                    fill="tozeroy",
                    mode="lines+markers+text",
                    line=dict(color=color, width=CONFIG["line"]["width"]),
                    marker=marker_config,
                    text=data[y_col],
                    textfont=CONFIG["font"],
                    textposition="top center"
                )
            ]
        )
    else:
        raise ValueError(f"Unsupported chart type: {chart_type}")

    fig.update_layout(
        title=dict(
            text=title,
            font=dict(
                size=16,
                family=CONFIG["font"]["family"],
                color=CONFIG["font"]["color"]
            ),
            x=0.5
        ),
        xaxis=dict(
            showgrid=False,
            zeroline=True,
            zerolinecolor=CONFIG["axis"]["line_color"],
            linecolor=CONFIG["axis"]["line_color"],
            linewidth=CONFIG["axis"]["line_width"]
        ),
        yaxis=dict(
            showgrid=False,
            zeroline=True,
            zerolinecolor=CONFIG["axis"]["line_color"],
            linecolor=CONFIG["axis"]["line_color"],
            linewidth=CONFIG["axis"]["line_width"]
        ),
        plot_bgcolor=CONFIG["background"]["plot_bgcolor"],
        paper_bgcolor=CONFIG["background"]["paper_bgcolor"],
        margin=CONFIG["margin"]
    )

    return fig
