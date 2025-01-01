import plotly.graph_objects as go

# Centralized configuration
CONFIG = {
    "font": {
        "family": "Arial",
        "size": 15,
        "color": "black"
    },
    "marker": {
        "size": 12,  # Default marker size
        "line_width": 0.5  # Border width for markers
    },
    "line": {
        "width": 2  # Default line width
    },
    "axis": {
        "line_color": "black",  # Axis line color
        "line_width": 2,  # Axis line width
    },
    "background": {
        "plot_bgcolor": "white",  # Plot background color
        "paper_bgcolor": "white"  # Outer paper background color
    },
    "margin": {
        "l": 40, "r": 40, "t": 40, "b": 40  # Default chart margins
    }
}


def create_custom_chart(data, x_col, y_col, title, chart_type="bar", color="grey", border_color="black"):
    """
    A helper function to create customized charts using centralized settings.
    
    Parameters:
        data (pd.DataFrame): DataFrame containing the data.
        x_col (str): The column for the x-axis.
        y_col (str): The column for the y-axis.
        title (str): Title of the chart.
        chart_type (str): Type of chart ('bar', 'line', 'scatter', 'area').
        color (str): Primary color for the chart.
        border_color (str): Border color for data points.
    
    Returns:
        plotly.graph_objects.Figure: A customized Plotly chart.
    """
    # Set up common marker properties
    marker_config = dict(
        color=color,
        size=CONFIG["marker"]["size"],
        line=dict(color=border_color, width=CONFIG["marker"]["line_width"])
    )

    # Initialize the figure based on chart type
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

    # Customize the layout
    fig.update_layout(
        title=dict(text=title, font=dict(size=16, family=CONFIG["font"]["family"]), x=0.5),  # Centered title
        xaxis=dict(
            showgrid=False,
            zeroline=True,
            zerolinecolor=CONFIG["axis"]["line_color"],
            linecolor=CONFIG["axis"]["line_color"],
            linewidth=CONFIG["axis"]["line_width"],
        ),
        yaxis=dict(
            showgrid=False,
            zeroline=True,
            zerolinecolor=CONFIG["axis"]["line_color"],
            linecolor=CONFIG["axis"]["line_color"],
            linewidth=CONFIG["axis"]["line_width"],
        ),
        plot_bgcolor=CONFIG["background"]["plot_bgcolor"],
        paper_bgcolor=CONFIG["background"]["paper_bgcolor"],
        margin=CONFIG["margin"],
    )

    return fig
