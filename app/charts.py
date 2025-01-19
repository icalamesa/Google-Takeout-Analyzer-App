import plotly.graph_objects as go

import plotly.graph_objects as go

CONFIG = {
    "font": {
        "family": "Montserrat, sans-serif",
        "size": 9,
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
        "paper_bgcolor": "#f8f8f8"
    },
    "margin": {
        "l": 5,
        "r": 5,
        "t": 0,   # Slightly taller top margin for the "card header" feel
        "b": 0
    },
    "accent_color": "#00bcd4",
    "accent_border": "#3b3b3b",
    # Optional: A 'card_border_width' or 'card_border_radius' for shape styling
    "card_border_width": 1,
    "card_border_radius": 0  # Change to 10 or 15 if you plan a custom path for rounding
}


def create_custom_chart(
    data,
    x_col,
    y_col,
    title,
    chart_type="bar",
    color=None,
    border_color=None,
    error_col=None
):
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
    elif chart_type == "line_error":
        # This new chart type handles error bars, using `error_col` if provided
        if error_col and error_col in data.columns:
            error_values = data[error_col]
        else:
            error_values = None
        fig = go.Figure(
            data=[
                go.Scatter(
                    x=data[x_col],
                    y=data[y_col],
                    mode="lines+markers+text",
                    line=dict(color=color, width=CONFIG["line"]["width"]),
                    marker={"size": 14,"line_width": 0.5},
                    text=[f"{val:.1f}" for val in data[y_col]],
                    textfont={ "family": "Montserrat, sans-serif", "size": 6,"color": "#3b3b3b"},
                    textposition="middle center",
                    error_y=dict(type='data', array=error_values, visible=True) if error_values is not None else None
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
    elif chart_type == "heatmap":
        pivot = data.pivot(index='time_gap', columns='period', values='count')
        fig = go.Figure(
            data=go.Heatmap(
                z=pivot.values,
                x=list(pivot.columns),
                y=list(pivot.index),
                colorscale="Viridis"
            )
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
