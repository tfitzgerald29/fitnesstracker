import plotly.graph_objects as go
from dash import Input, Output, callback, dcc, html
from plotly.subplots import make_subplots

from backend.cycling_processor import CyclingProcessor

from ..config import CARD_STYLE, COLORS

COVARIATE_LABELS = {
    "ctl_lag3": "CTL (3mo lag)",
    "spring": "Spring (Mar-May)",
    "summer": "Summer (Jun-Aug)",
    "fall": "Fall (Sep-Nov)",
}


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
                style={"marginBottom": "12px"},
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
            # CP Covariate Analysis section
            html.H3(
                "CP Covariate Analysis",
                style={
                    "color": COLORS["accent"],
                    "marginBottom": "12px",
                    "marginTop": "24px",
                    "fontSize": "0.95rem",
                },
            ),
            html.Div(id="cp-covariate-results"),
            html.Div(dcc.Graph(id="cp-covariate-chart"), style=CARD_STYLE),
        ]
    )


@callback(
    Output("cp-results", "children"),
    Output("cp-chart", "figure"),
    Input("cp-period", "value"),
)
def update_cp_model(period):
    cp = CyclingProcessor()
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
        style={"display": "flex", "gap": "12px", "marginBottom": "12px"},
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
    processor = CyclingProcessor()
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


@callback(
    Output("cp-covariate-results", "children"),
    Output("cp-covariate-chart", "figure"),
    Input("cp-period", "value"),
)
def update_cp_covariates(period):
    processor = CyclingProcessor()
    result = processor.cp_covariate_analysis()

    empty_fig = go.Figure()
    empty_fig.update_layout(
        template="plotly_dark",
        paper_bgcolor=COLORS["card"],
        plot_bgcolor=COLORS["card"],
    )

    if result["models"] is None:
        empty_fig.add_annotation(
            text="Not enough data for covariate analysis (need 5+ months)",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
            font=dict(color=COLORS["muted"]),
        )
        return html.Div(), empty_fig

    def _build_table(model_result, dep_var):
        header = html.Tr(
            [
                html.Th(
                    "Covariate", style={"textAlign": "left", "padding": "4px 12px"}
                ),
                html.Th("Coeff", style={"textAlign": "right", "padding": "4px 12px"}),
                html.Th("95% CI", style={"textAlign": "right", "padding": "4px 12px"}),
                html.Th("p-value", style={"textAlign": "right", "padding": "4px 12px"}),
            ]
        )
        rows = []
        for c in model_result["coefficients"]:
            label = COVARIATE_LABELS.get(c["name"], c["name"])
            if c["name"] == "const":
                label = "Intercept (Winter baseline)"
            sig = "*" if c["pvalue"] < 0.05 else ""
            p_color = COLORS["accent"] if c["pvalue"] < 0.05 else COLORS["muted"]
            rows.append(
                html.Tr(
                    [
                        html.Td(label, style={"padding": "4px 12px"}),
                        html.Td(
                            f"{c['coef']:.1f}",
                            style={"textAlign": "right", "padding": "4px 12px"},
                        ),
                        html.Td(
                            f"[{c['ci_low']:.1f}, {c['ci_high']:.1f}]",
                            style={
                                "textAlign": "right",
                                "padding": "4px 12px",
                                "fontSize": "0.8rem",
                            },
                        ),
                        html.Td(
                            f"{c['pvalue']:.3f}{sig}",
                            style={
                                "textAlign": "right",
                                "padding": "4px 12px",
                                "color": p_color,
                            },
                        ),
                    ]
                )
            )

        r2_text = (
            f"R\u00b2={model_result['r2']:.3f}  "
            f"Adj R\u00b2={model_result['r2_adj']:.3f}  "
            f"n={model_result['n']}"
        )
        return html.Div(
            [
                html.Div(
                    f"Peak {dep_var}",
                    style={
                        "fontWeight": "bold",
                        "color": COLORS["accent"],
                        "marginBottom": "4px",
                    },
                ),
                html.Div(
                    r2_text,
                    style={
                        "fontSize": "0.8rem",
                        "color": COLORS["muted"],
                        "marginBottom": "6px",
                    },
                ),
                html.Table(
                    [html.Thead(header), html.Tbody(rows)],
                    style={
                        "fontSize": "0.85rem",
                        "color": COLORS["text"],
                        "width": "100%",
                    },
                ),
            ],
            style={"flex": "1", **CARD_STYLE},
        )

    tables = [_build_table(mod, label) for label, mod in result["models"].items()]

    summary = html.Div(
        style={"display": "flex", "gap": "12px", "marginBottom": "12px"},
        children=tables,
    )

    # Correlation heatmap
    corr = result["correlation"]
    labels = [COVARIATE_LABELS.get(c, c) for c in corr["columns"]]

    fig = go.Figure(
        data=go.Heatmap(
            z=corr["values"],
            x=labels,
            y=labels,
            colorscale="RdBu_r",
            zmid=0,
            zmin=-1,
            zmax=1,
            text=[[f"{v:.2f}" for v in row] for row in corr["values"]],
            texttemplate="%{text}",
            textfont={"size": 11},
            hovertemplate="%{x} vs %{y}: %{z:.3f}<extra></extra>",
        )
    )

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor=COLORS["card"],
        plot_bgcolor=COLORS["card"],
        margin=dict(l=120, r=20, t=30, b=80),
        height=350,
        title=dict(text="Correlation Matrix", font=dict(size=13)),
        xaxis=dict(tickangle=-35),
    )

    return summary, fig
