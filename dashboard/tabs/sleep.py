import plotly.graph_objects as go
from dash import Input, Output, State, callback, dcc, html

from backend.sleep_processor import SleepProcessor
from ..config import CARD_STYLE, COLORS, get_user_id

ACCENT = "#00BCD4"  # sleep teal


# ── Private helpers ───────────────────────────────────────────────────────────


def _stat_card(label, value, unit="", sub=""):
    value_str = f"{value} {unit}".strip() if value is not None else "—"
    children = [
        html.Div(
            value_str,
            style={"fontSize": "1.5rem", "fontWeight": "bold", "color": ACCENT},
        ),
        html.Div(
            label,
            style={"fontSize": "0.8rem", "color": COLORS["muted"], "marginTop": "4px"},
        ),
    ]
    if sub:
        children.append(
            html.Div(
                sub,
                style={
                    "fontSize": "0.7rem",
                    "color": COLORS["muted"],
                    "marginTop": "2px",
                },
            )
        )
    return html.Div(
        style={
            **CARD_STYLE,
            "display": "inline-block",
            "textAlign": "center",
            "padding": "16px 28px",
            "minWidth": "130px",
        },
        children=children,
    )


def _stage_card(label, hours, pct, color):
    hrs_str = f"{hours:.2f}h" if hours is not None else "—"
    pct_str = f"{pct:.1f}%" if pct is not None else "—"
    return html.Div(
        style={
            **CARD_STYLE,
            "display": "inline-block",
            "textAlign": "center",
            "padding": "12px 20px",
            "minWidth": "100px",
            "borderTop": f"3px solid {color}",
        },
        children=[
            html.Div(
                hrs_str,
                style={"fontSize": "1.3rem", "fontWeight": "bold", "color": color},
            ),
            html.Div(
                pct_str,
                style={
                    "fontSize": "0.75rem",
                    "color": COLORS["muted"],
                    "marginTop": "2px",
                },
            ),
            html.Div(
                label,
                style={
                    "fontSize": "0.75rem",
                    "color": COLORS["muted"],
                    "marginTop": "4px",
                },
            ),
        ],
    )


def _score_badge(label, value):
    if value is None:
        color = COLORS["muted"]
    elif value >= 80:
        color = "#4CAF50"
    elif value >= 60:
        color = "#FF9800"
    else:
        color = "#f44336"
    return html.Div(
        style={
            **CARD_STYLE,
            "display": "inline-block",
            "textAlign": "center",
            "padding": "12px 20px",
            "minWidth": "110px",
        },
        children=[
            html.Div(
                str(value) if value is not None else "—",
                style={"fontSize": "1.4rem", "fontWeight": "bold", "color": color},
            ),
            html.Div(
                f"{value} / 100" if value is not None else "— / 100",
                style={
                    "fontSize": "0.7rem",
                    "color": COLORS["muted"],
                    "marginTop": "2px",
                },
            ),
            html.Div(
                label,
                style={
                    "fontSize": "0.75rem",
                    "color": COLORS["muted"],
                    "marginTop": "4px",
                },
            ),
        ],
    )


# ── Layout ────────────────────────────────────────────────────────────────────


def sleep_tab():
    metric_options = [
        {"label": "Total Sleep (hrs)", "value": "total_sleep_hrs"},
        {"label": "Sleep Score", "value": "score_overall"},
        {"label": "Deep Sleep (hrs)", "value": "deep_hrs"},
        {"label": "REM Sleep (hrs)", "value": "rem_hrs"},
        {"label": "Avg SpO2 (%)", "value": "avg_spo2"},
        {"label": "Sleep Efficiency (%)", "value": "sleep_efficiency_pct"},
        {"label": "Avg HR (bpm)", "value": "avg_hr"},
        {"label": "Sleep Stress", "value": "avg_sleep_stress"},
    ]

    return html.Div(
        [
            # ── Summary cards ─────────────────────────────────────────────────
            html.Div(id="sleep-summary-cards"),
            # ── Trend chart ───────────────────────────────────────────────────
            html.H3(
                "Sleep Trend",
                style={
                    "color": ACCENT,
                    "marginBottom": "8px",
                    "marginTop": "24px",
                    "fontSize": "0.95rem",
                },
            ),
            html.Div(
                style={
                    "display": "flex",
                    "alignItems": "center",
                    "gap": "12px",
                    "marginBottom": "8px",
                },
                children=[
                    html.Span(
                        "Metric:",
                        style={"color": COLORS["muted"], "fontSize": "0.85rem"},
                    ),
                    dcc.Dropdown(
                        id="sleep-metric-picker",
                        options=metric_options,
                        value="total_sleep_hrs",
                        clearable=False,
                        style={
                            "width": "220px",
                            "backgroundColor": COLORS["card"],
                            "color": COLORS["text"],
                            "border": f"1px solid {COLORS['border']}",
                            "borderRadius": "4px",
                        },
                    ),
                    html.Span(
                        "Window:",
                        style={
                            "color": COLORS["muted"],
                            "fontSize": "0.85rem",
                            "marginLeft": "12px",
                        },
                    ),
                    dcc.Dropdown(
                        id="sleep-window-picker",
                        options=[
                            {"label": "Last 30 days", "value": 30},
                            {"label": "Last 90 days", "value": 90},
                            {"label": "Last 365 days", "value": 365},
                            {"label": "All time", "value": 0},
                        ],
                        value=90,
                        clearable=False,
                        style={
                            "width": "160px",
                            "backgroundColor": COLORS["card"],
                            "color": COLORS["text"],
                            "border": f"1px solid {COLORS['border']}",
                            "borderRadius": "4px",
                        },
                    ),
                ],
            ),
            html.Div(
                dcc.Graph(id="sleep-trend-chart", config={"displayModeBar": False}),
                style=CARD_STYLE,
            ),
            # ── Stage breakdown ───────────────────────────────────────────────
            html.H3(
                "Nightly Stage Breakdown",
                style={
                    "color": ACCENT,
                    "marginBottom": "8px",
                    "marginTop": "8px",
                    "fontSize": "0.95rem",
                },
            ),
            html.Div(
                dcc.Graph(id="sleep-stage-chart", config={"displayModeBar": False}),
                style=CARD_STYLE,
            ),
            # ── Score breakdown ───────────────────────────────────────────────
            html.H3(
                "Latest Sleep Scores",
                style={
                    "color": ACCENT,
                    "marginBottom": "8px",
                    "marginTop": "8px",
                    "fontSize": "0.95rem",
                },
            ),
            html.Div(id="sleep-score-cards"),
        ]
    )


# ── Callbacks ─────────────────────────────────────────────────────────────────


@callback(
    Output("sleep-summary-cards", "children"),
    Output("sleep-score-cards", "children"),
    Input("tabs", "value"),
    State("user-store", "data"),
)
def update_sleep_overview(tab, user_data):
    if tab != "sleep":
        return [], []

    sp = SleepProcessor(user_id=get_user_id(user_data))

    if sp.sleep.is_empty():
        empty = html.Div("No sleep data found.", style={"color": COLORS["muted"]})
        return [empty], []

    stats = sp.summary_stats()
    recent = sp.recent_stats(days=30)

    all_avg = stats.get("avg_sleep_hrs")
    rec_avg = recent.get("avg_sleep_hrs")
    sub = f"30d avg: {rec_avg}h" if rec_avg is not None else ""

    summary_cards = html.Div(
        style={
            "display": "flex",
            "gap": "12px",
            "flexWrap": "wrap",
            "marginBottom": "8px",
        },
        children=[
            _stat_card("Nights tracked", stats.get("total_nights", 0)),
            _stat_card("Avg sleep", all_avg, "hrs", sub),
            _stat_card("Avg score", stats.get("avg_score")),
            _stat_card("Avg deep", stats.get("avg_deep_hrs"), "hrs"),
            _stat_card("Avg REM", stats.get("avg_rem_hrs"), "hrs"),
            _stat_card("Avg efficiency", stats.get("avg_efficiency_pct"), "%"),
            _stat_card("Avg SpO2", stats.get("avg_spo2"), "%"),
        ],
    )

    # Latest night's detail
    last = sp.sleep.tail(1).to_dicts()[0]
    date_label = last.get("calendar_date", "")

    # Stage hours + percentages (% of total time in bed)
    total_in_bed = last.get("total_in_bed_sec") or 0

    def _pct(sec):
        return round(sec / total_in_bed * 100, 1) if total_in_bed > 0 else None

    stage_row = html.Div(
        style={
            "display": "flex",
            "gap": "10px",
            "flexWrap": "wrap",
            "marginBottom": "16px",
        },
        children=[
            _stage_card(
                "Deep", last.get("deep_hrs"), _pct(last.get("deep_sec") or 0), "#1565C0"
            ),
            _stage_card(
                "REM", last.get("rem_hrs"), _pct(last.get("rem_sec") or 0), "#F57C00"
            ),
            _stage_card(
                "Light",
                last.get("light_hrs"),
                _pct(last.get("light_sec") or 0),
                "#FFD600",
            ),
            _stage_card(
                "Awake",
                last.get("awake_hrs"),
                _pct(last.get("awake_sec") or 0),
                "#90A4AE",
            ),
        ],
    )

    # Garmin sleep scores
    score_row = html.Div(
        style={"display": "flex", "gap": "10px", "flexWrap": "wrap"},
        children=[
            _score_badge("Overall", last.get("score_overall")),
            _score_badge("Quality", last.get("score_quality")),
            _score_badge("Duration", last.get("score_duration")),
            _score_badge("Recovery", last.get("score_recovery")),
            _score_badge("Deep", last.get("score_deep")),
            _score_badge("REM", last.get("score_rem")),
        ],
    )

    score_section = html.Div(
        [
            html.Div(
                f"Most recent night: {date_label}",
                style={
                    "color": COLORS["muted"],
                    "fontSize": "0.8rem",
                    "marginBottom": "12px",
                },
            ),
            stage_row,
            html.Div(
                "Garmin sleep scores",
                style={
                    "color": COLORS["muted"],
                    "fontSize": "0.75rem",
                    "marginBottom": "8px",
                },
            ),
            score_row,
        ]
    )

    return summary_cards, score_section


@callback(
    Output("sleep-trend-chart", "figure"),
    Input("tabs", "value"),
    Input("sleep-metric-picker", "value"),
    Input("sleep-window-picker", "value"),
    State("user-store", "data"),
)
def update_sleep_trend(tab, metric, window_days, user_data):
    fig = go.Figure()
    fig.update_layout(
        paper_bgcolor=COLORS["bg"],
        plot_bgcolor=COLORS["bg"],
        font={"color": COLORS["text"]},
        margin={"t": 20, "b": 40, "l": 50, "r": 20},
        xaxis={"gridcolor": COLORS["border"], "showgrid": True},
        yaxis={"gridcolor": COLORS["border"], "showgrid": True},
        showlegend=False,
    )

    if tab != "sleep":
        return fig

    sp = SleepProcessor(user_id=get_user_id(user_data))
    df = sp.chart_data(metric)
    if df.is_empty():
        return fig

    if window_days and window_days > 0:
        latest = df["calendar_date"].max()
        cutoff = (
            __import__("polars")
            .Series([latest])
            .str.to_date()
            .dt.offset_by(f"-{window_days}d")
            .cast(__import__("polars").Utf8)[0]
        )
        df = df.filter(__import__("polars").col("calendar_date") >= cutoff)

    if df.is_empty():
        return fig

    dates = df["calendar_date"].to_list()
    values = df[metric].to_list()

    # 7-day rolling average
    import polars as pl

    rolling = df.with_columns(
        pl.col(metric).rolling_mean(window_size=7, min_samples=1).alias("rolling_avg")
    )
    rolling_values = rolling["rolling_avg"].to_list()

    fig.add_trace(
        go.Bar(
            x=dates,
            y=values,
            marker_color=ACCENT,
            opacity=0.45,
            name=metric,
        )
    )
    fig.add_trace(
        go.Scatter(
            x=dates,
            y=rolling_values,
            mode="lines",
            line={"color": "#ffffff", "width": 1.5},
            name="7d avg",
        )
    )

    return fig


@callback(
    Output("sleep-stage-chart", "figure"),
    Input("tabs", "value"),
    Input("sleep-window-picker", "value"),
    State("user-store", "data"),
)
def update_sleep_stages(tab, window_days, user_data):
    fig = go.Figure()
    fig.update_layout(
        paper_bgcolor=COLORS["bg"],
        plot_bgcolor=COLORS["bg"],
        font={"color": COLORS["text"]},
        margin={"t": 20, "b": 40, "l": 50, "r": 20},
        xaxis={"gridcolor": COLORS["border"], "showgrid": True},
        yaxis={
            "gridcolor": COLORS["border"],
            "showgrid": True,
            "title": "% of time in bed",
            "range": [0, 100],
        },
        barmode="stack",
        legend={
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.02,
            "xanchor": "left",
            "x": 0,
        },
    )

    if tab != "sleep":
        return fig

    sp = SleepProcessor(user_id=get_user_id(user_data))
    df = sp.stage_breakdown_data()
    if df.is_empty():
        return fig

    if window_days and window_days > 0:
        import polars as pl

        latest = df["calendar_date"].max()
        cutoff = (
            pl.Series([latest])
            .str.to_date()
            .dt.offset_by(f"-{window_days}d")
            .cast(pl.Utf8)[0]
        )
        df = df.filter(pl.col("calendar_date") >= cutoff)

    if df.is_empty():
        return fig

    dates = df["calendar_date"].to_list()
    # Colors chosen for deuteranopia/protanopia safety:
    # deep = dark blue, REM = orange, light = yellow, awake = light grey.
    # Avoids red/green confusion; blue-orange-yellow trio is clearly distinct.
    stage_colors = {
        "deep_pct": "#1565C0",  # dark blue
        "rem_pct": "#F57C00",  # orange
        "light_pct": "#FFD600",  # yellow
        "awake_pct": "#90A4AE",  # light grey-blue
    }
    stage_labels = {
        "deep_pct": "Deep",
        "rem_pct": "REM",
        "light_pct": "Light",
        "awake_pct": "Awake",
    }

    for col, color in stage_colors.items():
        fig.add_trace(
            go.Bar(
                x=dates,
                y=df[col].to_list(),
                name=stage_labels[col],
                marker_color=color,
            )
        )

    return fig
