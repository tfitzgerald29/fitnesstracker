import plotly.graph_objects as go
from dash import Input, Output, callback, dcc, html
from plotly.subplots import make_subplots

from backend.cycling_processor import CyclingProcessor

from ..config import CARD_STYLE, COLORS, get_user_id


def cycling_cp_layout():
    return html.Div(
        [
            # Critical Power model section
            html.H3(
                "Critical Power Model",
                style={
                    "color": COLORS["accent"],
                    "marginBottom": "12px",
                    "fontSize": "0.95rem",
                },
            ),
            dcc.RadioItems(
                id="cp-period",
                options=[
                    {"label": "3 Months", "value": "3"},
                    {"label": "6 Months", "value": "6"},
                    {"label": "9 Months", "value": "9"},
                    {"label": "12 Months", "value": "12"},
                ],
                value="6",
                inline=True,
                labelStyle={
                    "marginRight": "12px",
                    "cursor": "pointer",
                    "color": COLORS["text"],
                },
                inputStyle={"marginRight": "4px"},
                style={
                    "display": "flex",
                    "flexWrap": "wrap",
                    "gap": "8px",
                    "rowGap": "6px",
                    "marginBottom": "12px",
                },
            ),
            html.Div(id="cp-results"),
            html.Div(dcc.Graph(id="cp-chart"), style=CARD_STYLE),
            # CP over time section
            html.H3(
                "Critical Power Over Time",
                style={
                    "color": COLORS["accent"],
                    "marginBottom": "12px",
                    "marginTop": "24px",
                    "fontSize": "0.95rem",
                },
            ),
            html.Div(dcc.Graph(id="cp-over-time-chart"), style=CARD_STYLE),
        ]
    )


@callback(
    Output("cp-results", "children"),
    Output("cp-chart", "figure"),
    Input("cp-period", "value"),
)
def update_cp_model(period):
    cp = CyclingProcessor(user_id=get_user_id())
    period_months = int(period)
    result = cp.estimate_critical_power(period_months)

    fig = go.Figure()

    if result["cp"] is None:
        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor=COLORS["card"],
            plot_bgcolor=COLORS["card"],
            annotations=[
                dict(
                    text="Not enough data for CP model",
                    xref="paper",
                    yref="paper",
                    x=0.5,
                    y=0.5,
                    showarrow=False,
                    font=dict(color=COLORS["muted"]),
                )
            ],
        )
        return html.Div(), fig

    durations = result["durations"]
    watts = result["watts"]
    fit_durations = result["fit_durations"]
    fitted = result["fitted_watts"]
    cp_val = result["cp"]
    wprime = result["wprime_kj"]
    r2 = result["r2"]

    # Format durations for x-axis
    def fmt_dur(s):
        if s < 60:
            return f"{s}s"
        elif s < 3600:
            return f"{s // 60}m"
        else:
            return f"{s // 3600}h"

    labels = [fmt_dur(d) for d in durations]
    fit_labels = [fmt_dur(d) for d in fit_durations]

    # Actual best power points
    fig.add_trace(
        go.Scatter(
            x=durations,
            y=watts,
            mode="markers",
            name="Best power",
            marker=dict(color="rgba(100,181,246,0.9)", size=8),
            hovertemplate="%{text}: %{y:.0f}W<extra></extra>",
            text=labels,
        )
    )

    # Fitted CP model curve (2min–20min only)
    fig.add_trace(
        go.Scatter(
            x=fit_durations,
            y=fitted,
            mode="lines",
            name="CP model",
            line=dict(color="rgba(255,152,0,0.8)", width=2, dash="dash"),
            hovertemplate="%{text}: %{y:.0f}W<extra></extra>",
            text=fit_labels,
        )
    )

    # CP horizontal line
    fig.add_hline(
        y=cp_val,
        line_dash="dot",
        line_color="rgba(76,175,80,0.6)",
        annotation_text=f"CP = {cp_val}W",
        annotation_font_color=COLORS["text"],
    )

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor=COLORS["card"],
        plot_bgcolor=COLORS["card"],
        margin=dict(l=50, r=20, t=20, b=40),
        height=350,
        xaxis=dict(
            type="log",
            tickvals=[5, 10, 30, 60, 120, 300, 600, 1200, 1800, 3600, 5400, 7200],
            ticktext=[
                "5s",
                "10s",
                "30s",
                "1m",
                "2m",
                "5m",
                "10m",
                "20m",
                "30m",
                "1h",
                "1.5h",
                "2h",
            ],
            title="Duration",
            gridcolor="rgba(255,255,255,0.05)",
        ),
        yaxis=dict(
            title="Power (W)",
            gridcolor="rgba(255,255,255,0.05)",
        ),
        showlegend=True,
        legend=dict(x=0.7, y=0.95),
    )

    # Summary cards
    summary = html.Div(
        style={
            "display": "flex",
            "flexWrap": "wrap",
            "gap": "12px",
            "rowGap": "8px",
            "marginBottom": "12px",
        },
        children=[
            html.Div(
                style={
                    **CARD_STYLE,
                    "display": "inline-block",
                    "textAlign": "center",
                    "padding": "12px 24px",
                },
                children=[
                    html.Div(
                        f"{cp_val}W",
                        style={
                            "fontSize": "1.3rem",
                            "fontWeight": "bold",
                            "color": COLORS["accent"],
                        },
                    ),
                    html.Div(
                        "Critical Power",
                        style={"fontSize": "0.75rem", "color": COLORS["muted"]},
                    ),
                ],
            ),
            html.Div(
                style={
                    **CARD_STYLE,
                    "display": "inline-block",
                    "textAlign": "center",
                    "padding": "12px 24px",
                },
                children=[
                    html.Div(
                        f"{wprime} kJ",
                        style={
                            "fontSize": "1.3rem",
                            "fontWeight": "bold",
                            "color": COLORS["accent"],
                        },
                    ),
                    html.Div(
                        "W\u2032",
                        style={"fontSize": "0.75rem", "color": COLORS["muted"]},
                    ),
                ],
            ),
            html.Div(
                style={
                    **CARD_STYLE,
                    "display": "inline-block",
                    "textAlign": "center",
                    "padding": "12px 24px",
                },
                children=[
                    html.Div(
                        f"{r2}",
                        style={
                            "fontSize": "1.3rem",
                            "fontWeight": "bold",
                            "color": COLORS["accent"],
                        },
                    ),
                    html.Div(
                        "R\u00b2",
                        style={"fontSize": "0.75rem", "color": COLORS["muted"]},
                    ),
                ],
            ),
        ],
    )

    return summary, fig


@callback(
    Output("cp-over-time-chart", "figure"),
    Input("cp-period", "value"),
)
def update_cp_over_time(period):
    processor = CyclingProcessor(user_id=get_user_id())
    data = processor.cp_over_time(period_months=int(period))

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        row_heights=[0.6, 0.4],
    )

    if not data["dates"]:
        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor=COLORS["card"],
            plot_bgcolor=COLORS["card"],
            annotations=[
                dict(
                    text="Not enough data",
                    xref="paper",
                    yref="paper",
                    x=0.5,
                    y=0.5,
                    showarrow=False,
                    font=dict(color=COLORS["muted"]),
                )
            ],
        )
        return fig

    dates = data["dates"]
    cp_vals = data["cp"]
    wprime_vals = data["wprime_kj"]

    # CP line
    fig.add_trace(
        go.Scatter(
            x=dates,
            y=cp_vals,
            mode="lines+markers",
            name="CP",
            line=dict(color="rgba(100,181,246,0.9)", width=2),
            marker=dict(size=5),
            hovertemplate="%{x}: %{y}W<extra>CP</extra>",
        ),
        row=1,
        col=1,
    )

    # W' line
    fig.add_trace(
        go.Scatter(
            x=dates,
            y=wprime_vals,
            mode="lines+markers",
            name="W\u2032",
            line=dict(color="rgba(255,152,0,0.9)", width=2),
            marker=dict(size=5),
            hovertemplate="%{x}: %{y} kJ<extra>W\u2032</extra>",
        ),
        row=2,
        col=1,
    )

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor=COLORS["card"],
        plot_bgcolor=COLORS["card"],
        margin=dict(l=50, r=20, t=20, b=40),
        height=400,
        showlegend=False,
        hovermode="x unified",
    )

    fig.update_yaxes(
        title_text="CP (W)", row=1, col=1, gridcolor="rgba(255,255,255,0.05)"
    )
    fig.update_yaxes(
        title_text="W\u2032 (kJ)", row=2, col=1, gridcolor="rgba(255,255,255,0.05)"
    )
    fig.update_xaxes(gridcolor="rgba(255,255,255,0.05)", row=1, col=1)
    fig.update_xaxes(gridcolor="rgba(255,255,255,0.05)", row=2, col=1)

    return fig
