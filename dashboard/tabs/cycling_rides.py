import plotly.graph_objects as go
from dash import Input, Output, State, callback, dcc, html
from plotly.subplots import make_subplots

from backend.cycling_processor import CyclingProcessor

from ..config import CARD_STYLE, COLORS, get_user_id

_RIDE_PROCESSOR_CACHE: dict[str, CyclingProcessor] = {}


def _processor_key(user_id: str | None, ride_ts: str | None) -> str | None:
    if not ride_ts:
        return None
    return f"{user_id or '__local__'}:{ride_ts}"


def _get_cached_processor(user_id: str | None, ride_ts: str | None) -> CyclingProcessor:
    key = _processor_key(user_id, ride_ts)
    if not key:
        return CyclingProcessor(user_id=user_id)

    proc = _RIDE_PROCESSOR_CACHE.get(key)
    if proc is None:
        proc = CyclingProcessor(user_id=user_id)
        _RIDE_PROCESSOR_CACHE.clear()
        _RIDE_PROCESSOR_CACHE[key] = proc
    return proc


def _normalize_ride_data(value):
    if isinstance(value, dict):
        return value.get("source_file"), value.get("ride_ts")
    return value, None


def _fmt_ride_time(seconds):
    """Format total ride time seconds into Xh Xm Xs."""
    if seconds is None:
        return None
    s = int(seconds)
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    if h:
        return f"{h}h {m}m {sec}s"
    return f"{m}m {sec}s"


def _stat_card(label, value, unit=""):
    if value is None:
        display = "—"
    elif value == "N/A":
        display = "N/A"
    else:
        display = f"{value} {unit}".strip()
    return html.Div(
        style={
            **CARD_STYLE,
            "display": "inline-block",
            "width": "140px",
            "textAlign": "center",
            "marginRight": "8px",
        },
        children=[
            html.Div(
                display,
                style={"fontSize": "1.3rem", "fontWeight": "600", "color": "#fff"},
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


def _climb_stat(label, value):
    return html.Div(
        children=[
            html.Div(
                value,
                style={"fontSize": "0.95rem", "fontWeight": "600", "color": "#fff"},
            ),
            html.Div(
                label,
                style={
                    "fontSize": "0.7rem",
                    "color": COLORS["muted"],
                    "marginTop": "2px",
                },
            ),
        ],
    )


def _format_duration(seconds):
    """Format seconds into a readable label for the x-axis."""
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        m = seconds // 60
        return f"{m}min"
    else:
        h = seconds // 3600
        m = (seconds % 3600) // 60
        return f"{h}h{m:02d}" if m else f"{h}h"


def _fmt_zone_time(seconds):
    """Format seconds into Xh Xm or Xm Xs."""
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h {m}m"
    return f"{m}m {s}s"


def _nearest_idx(dist_list, target_mi):
    """Find the index in dist_list closest to target_mi."""
    if not dist_list:
        return None
    best_idx = 0
    best_diff = abs(dist_list[0] - target_mi)
    for i, d in enumerate(dist_list[1:], 1):
        diff = abs(d - target_mi)
        if diff < best_diff:
            best_diff = diff
            best_idx = i
    return best_idx


def cycling_rides_layout(user_id=None):
    cp = CyclingProcessor(user_id=user_id)
    ride_options = cp.list_rides()
    return html.Div(
        [
            dcc.Dropdown(
                id="ride-selector",
                options=ride_options,
                value=ride_options[0]["value"] if ride_options else None,
                placeholder="Search by date...",
                searchable=True,
                style={"marginBottom": "16px", "color": "#000"},
            ),
            html.Div(id="ride-detail-content"),
        ]
    )


@callback(
    Output("ride-detail-content", "children"),
    Input("ride-selector", "value"),
    State("user-store", "data"),
)
def update_ride_detail(ride_ts, user_data):
    if not ride_ts:
        return html.Div(
            "Select a ride to view details.", style={"color": COLORS["muted"]}
        )

    cp = _get_cached_processor(get_user_id(user_data), ride_ts)
    s = cp.get_ride_summary(ride_ts)
    if not s:
        return html.Div("Ride not found.", style={"color": COLORS["muted"]})

    # Peak powers from record_mesgs
    peak_powers = cp.get_peak_powers(s["source_file"]) if s.get("source_file") else []
    peak_section = []
    if peak_powers:
        peak_section = [
            html.H3(
                "Peak Power",
                style={
                    "color": COLORS["accent"],
                    "marginBottom": "12px",
                    "marginTop": "24px",
                    "fontSize": "0.95rem",
                },
            ),
            html.Div(
                style={"display": "flex", "flexWrap": "wrap", "gap": "4px"},
                children=[
                    _stat_card(p["duration"], p["watts"], "W") for p in peak_powers
                ],
            ),
        ]

    return html.Div(
        [
            html.H3(
                s["date"],
                style={
                    "color": COLORS["accent"],
                    "marginBottom": "16px",
                    "fontSize": "1rem",
                },
            ),
            html.Div(
                style={"display": "flex", "flexWrap": "wrap", "gap": "4px"},
                children=[
                    _stat_card("Distance", s["distance_mi"], "mi"),
                    _stat_card(
                        "Ride Time", _fmt_ride_time(s.get("total_timer_time_s")), ""
                    ),
                    _stat_card("Avg Speed", s["avg_speed_mph"], "mph"),
                    _stat_card("Avg Power", s["avg_power"], "W"),
                    _stat_card("NP", s["normalized_power"], "W"),
                    _stat_card("TSS", s["tss"]),
                    _stat_card("IF", s["intensity_factor"]),
                    _stat_card("Avg Cadence", s["avg_cadence"], "rpm"),
                    _stat_card("Avg HR", s["avg_hr"], "bpm"),
                    _stat_card("Max HR", s["max_hr"], "bpm"),
                    _stat_card("Elevation", s["total_ascent_ft"], "ft"),
                    _stat_card("Calories", s["calories"], "kcal"),
                    _stat_card("Work", s["work_kj"], "kJ"),
                    _stat_card("FTP", s["ftp"], "W"),
                    _stat_card(
                        "L/R Balance",
                        (
                            f"{s['left_balance']}/{s['right_balance']}"
                            if s.get("left_balance")
                            else None
                        ),
                        "%",
                    ),
                ],
            ),
            *peak_section,
            # Elevation profile section
            html.H3(
                "Elevation Profile",
                style={
                    "color": COLORS["accent"],
                    "marginBottom": "12px",
                    "marginTop": "24px",
                    "fontSize": "0.95rem",
                },
            ),
            html.Div(dcc.Graph(id="elevation-profile-chart"), style=CARD_STYLE),
            # Detected climbs section
            html.Div(id="climbs-section"),
            # Route map section
            html.H3(
                "Route Map",
                style={
                    "color": COLORS["accent"],
                    "marginBottom": "12px",
                    "marginTop": "24px",
                    "fontSize": "0.95rem",
                },
            ),
            dcc.RadioItems(
                id="map-color-mode",
                options=[
                    {"label": "Power", "value": "power"},
                    {"label": "Elevation", "value": "elevation"},
                    {"label": "Climbs", "value": "climbs"},
                ],
                value="power",
                inline=True,
                labelStyle={
                    "marginRight": "12px",
                    "cursor": "pointer",
                    "color": COLORS["text"],
                },
                inputStyle={"marginRight": "4px"},
                style={"marginBottom": "12px"},
            ),
            html.Div(dcc.Graph(id="route-map-chart"), style=CARD_STYLE),
            # W' Balance section
            html.H3(
                "W\u2032 Balance",
                style={
                    "color": COLORS["accent"],
                    "marginBottom": "12px",
                    "marginTop": "24px",
                    "fontSize": "0.95rem",
                },
            ),
            html.Div(id="wprime-stats"),
            html.Div(dcc.Graph(id="wprime-balance-chart"), style=CARD_STYLE),
            # Power histogram section
            html.H3(
                "Power Histogram",
                style={
                    "color": COLORS["accent"],
                    "marginBottom": "12px",
                    "marginTop": "24px",
                    "fontSize": "0.95rem",
                },
            ),
            html.Div(dcc.Graph(id="power-histogram-chart"), style=CARD_STYLE),
            # Power zone distribution section
            html.H3(
                "Power Zone Distribution",
                style={
                    "color": COLORS["accent"],
                    "marginBottom": "12px",
                    "marginTop": "24px",
                    "fontSize": "0.95rem",
                },
            ),
            html.Div(dcc.Graph(id="power-zone-chart"), style=CARD_STYLE),
            # Power curve section
            html.H3(
                "Power Curve",
                style={
                    "color": COLORS["accent"],
                    "marginBottom": "12px",
                    "marginTop": "24px",
                    "fontSize": "0.95rem",
                },
            ),
            dcc.Checklist(
                id="power-curve-compare",
                options=[
                    {"label": " 4 Weeks", "value": "1"},
                    {"label": " 3 Months", "value": "3"},
                    {"label": " 12 Months", "value": "12"},
                ],
                value=["3"],
                inline=True,
                labelStyle={
                    "marginRight": "16px",
                    "cursor": "pointer",
                    "color": COLORS["text"],
                },
                inputStyle={"marginRight": "4px"},
                style={"marginBottom": "12px"},
            ),
            dcc.Store(
                id="ride-source-file",
                data={
                    "source_file": s.get("source_file"),
                    "ride_ts": ride_ts,
                },
            ),
            html.Div(dcc.Graph(id="power-curve-chart"), style=CARD_STYLE),
        ]
    )


@callback(
    Output("power-curve-chart", "figure"),
    Input("ride-source-file", "data"),
    Input("power-curve-compare", "value"),
    State("user-store", "data"),
)
def update_power_curve(ride_data, compare_periods, user_data):
    source_file, ride_ts = _normalize_ride_data(ride_data)
    cp = _get_cached_processor(get_user_id(user_data), ride_ts)

    fig = go.Figure()

    # Current ride curve
    if source_file:
        curve = cp.get_power_curve(source_file)
        if curve["durations"]:
            labels = [_format_duration(d) for d in curve["durations"]]
            fig.add_trace(
                go.Scatter(
                    x=labels,
                    y=curve["watts"],
                    name="This Ride",
                    mode="lines",
                    line=dict(color="#2196F3", width=3),
                    hovertemplate="%{y}W<extra>This Ride</extra>",
                )
            )

    # Comparison curves
    compare_colors = {"1": "#FF9800", "3": "#4CAF50", "12": "#E91E63"}
    compare_labels = {"1": "Best 4 Weeks", "3": "Best 3 Months", "12": "Best 12 Months"}
    for period in compare_periods or []:
        best = cp.get_best_power_curve(period_months=int(period), chart=True)
        if best["durations"]:
            labels = [_format_duration(d) for d in best["durations"]]
            fig.add_trace(
                go.Scatter(
                    x=labels,
                    y=best["watts"],
                    name=compare_labels[period],
                    mode="lines",
                    line=dict(color=compare_colors[period], width=2, dash="dash"),
                    hovertemplate="%{y}W<extra>" + compare_labels[period] + "</extra>",
                )
            )

    fig.update_layout(
        paper_bgcolor=COLORS["card"],
        plot_bgcolor=COLORS["card"],
        font_color=COLORS["text"],
        hovermode="x unified",
        xaxis=dict(gridcolor=COLORS["border"], title="Duration"),
        yaxis=dict(gridcolor=COLORS["border"], title="Watts", rangemode="tozero"),
        height=400,
        margin=dict(t=20, b=60, l=60, r=30),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )

    return fig


@callback(
    Output("wprime-stats", "children"),
    Output("wprime-balance-chart", "figure"),
    Input("ride-source-file", "data"),
    State("user-store", "data"),
)
def update_wprime_balance(ride_data, user_data):
    source_file, ride_ts = _normalize_ride_data(ride_data)
    cp = _get_cached_processor(get_user_id(user_data), ride_ts)
    data = cp.get_wprime_balance(source_file) if source_file else {}
    time_min = data.get("time_min", [])
    bal_kj = data.get("wprime_bal_kj", [])
    power = data.get("power", [])
    ftp = data.get("ftp", 0)
    wp_kj = data.get("wprime_kj", 0)

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.06,
        row_heights=[0.55, 0.45],
    )

    if not time_min:
        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor=COLORS["card"],
            plot_bgcolor=COLORS["card"],
            annotations=[
                dict(
                    text="No power data available",
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

    # Stats cards showing CP model values as of this ride
    min_bal = round(min(bal_kj), 1) if bal_kj else 0
    stats = html.Div(
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
                        f"{ftp}W",
                        style={
                            "fontSize": "1.3rem",
                            "fontWeight": "bold",
                            "color": COLORS["accent"],
                        },
                    ),
                    html.Div(
                        "CP", style={"fontSize": "0.75rem", "color": COLORS["muted"]}
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
                        f"{wp_kj} kJ",
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
                        f"{min_bal} kJ",
                        style={
                            "fontSize": "1.3rem",
                            "fontWeight": "bold",
                            "color": COLORS["accent"],
                        },
                    ),
                    html.Div(
                        "Min W\u2032 bal",
                        style={"fontSize": "0.75rem", "color": COLORS["muted"]},
                    ),
                ],
            ),
        ],
    )

    # W' balance area chart
    fig.add_trace(
        go.Scatter(
            x=time_min,
            y=bal_kj,
            mode="lines",
            fill="tozeroy",
            name="W\u2032 bal",
            line=dict(color="rgba(100,181,246,0.9)", width=1.5),
            fillcolor="rgba(100,181,246,0.15)",
            hovertemplate="W\u2032: %{y:.1f} kJ<extra></extra>",
        ),
        row=1,
        col=1,
    )

    # Add depletion threshold line
    fig.add_hline(
        y=wp_kj * 0.25,
        line_dash="dot",
        line_color="rgba(244,67,54,0.5)",
        row=1,
        col=1,
    )

    # Power trace with FTP line
    fig.add_trace(
        go.Scatter(
            x=time_min,
            y=power,
            mode="lines",
            name="Power",
            line=dict(color="rgba(255,255,255,0.5)", width=0.8),
            hovertemplate="Power: %{y}W<extra></extra>",
        ),
        row=2,
        col=1,
    )

    fig.add_hline(
        y=ftp,
        line_dash="dash",
        line_color="rgba(255,152,0,0.7)",
        annotation_text=f"CP {ftp}W  |  W\u2032 {wp_kj} kJ",
        annotation_font_color=COLORS["muted"],
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
        title_text="W\u2032 (kJ)", row=1, col=1, gridcolor="rgba(255,255,255,0.05)"
    )
    fig.update_yaxes(
        title_text="Power (W)", row=2, col=1, gridcolor="rgba(255,255,255,0.05)"
    )
    fig.update_xaxes(
        title_text="Time (min)", row=2, col=1, gridcolor="rgba(255,255,255,0.05)"
    )
    fig.update_xaxes(gridcolor="rgba(255,255,255,0.05)", row=1, col=1)

    return stats, fig


@callback(
    Output("power-histogram-chart", "figure"),
    Input("ride-source-file", "data"),
    State("user-store", "data"),
)
def update_power_histogram(ride_data, user_data):
    source_file, ride_ts = _normalize_ride_data(ride_data)
    fig = go.Figure()

    if source_file:
        cp = _get_cached_processor(get_user_id(user_data), ride_ts)
        data = cp.get_power_histogram(source_file)
        if data["bins"]:
            fig.add_trace(
                go.Bar(
                    x=data["bins"],
                    y=data["counts"],
                    marker_color=data["zone_colors"],
                    hovertemplate="%{x}W: %{y}s<extra></extra>",
                )
            )

    fig.update_layout(
        paper_bgcolor=COLORS["card"],
        plot_bgcolor=COLORS["card"],
        font_color=COLORS["text"],
        xaxis=dict(gridcolor=COLORS["border"], title="Power (W)"),
        yaxis=dict(gridcolor=COLORS["border"], title="Time (seconds)"),
        height=350,
        margin=dict(t=20, b=60, l=60, r=30),
        showlegend=False,
        bargap=0.05,
    )
    return fig


@callback(
    Output("power-zone-chart", "figure"),
    Input("ride-source-file", "data"),
    State("user-store", "data"),
)
def update_power_zone_chart(ride_data, user_data):
    source_file, ride_ts = _normalize_ride_data(ride_data)
    fig = go.Figure()

    if source_file:
        cp = _get_cached_processor(get_user_id(user_data), ride_ts)
        data = cp.get_power_zone_distribution(source_file)
        if data["zones"]:
            labels = [
                f"{z} ({_fmt_zone_time(s)}, {p}%)"
                for z, s, p in zip(data["zones"], data["seconds"], data["percentages"])
            ]
            fig.add_trace(
                go.Bar(
                    y=data["zones"],
                    x=data["percentages"],
                    orientation="h",
                    marker_color=data["colors"],
                    text=[f"{p}%" for p in data["percentages"]],
                    textposition="auto",
                    textfont=dict(color="#fff", size=12),
                    hovertext=labels,
                    hoverinfo="text",
                )
            )

    fig.update_layout(
        paper_bgcolor=COLORS["card"],
        plot_bgcolor=COLORS["card"],
        font_color=COLORS["text"],
        xaxis=dict(gridcolor=COLORS["border"], title="% of Ride Time"),
        yaxis=dict(gridcolor=COLORS["border"], autorange="reversed"),
        height=300,
        margin=dict(t=20, b=60, l=120, r=30),
        showlegend=False,
    )
    return fig


@callback(
    Output("elevation-profile-chart", "figure"),
    Input("ride-source-file", "data"),
    State("user-store", "data"),
)
def update_elevation_profile(ride_data, user_data):
    source_file, ride_ts = _normalize_ride_data(ride_data)
    fig = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        row_heights=[0.45, 0.3, 0.25],
        subplot_titles=["Elevation", "Grade", "Smoothing Delta (5s − 20s)"],
    )

    if source_file:
        cp = _get_cached_processor(get_user_id(user_data), ride_ts)
        data = cp.get_elevation_profile(source_file)
        if data["distance_mi"]:
            dist = data["distance_mi"]
            alt = data["altitude_ft"]
            grade = data["grade_pct"]
            instant = data.get("grade_instant", grade)

            # Elevation area fill (top)
            fig.add_trace(
                go.Scatter(
                    x=dist,
                    y=alt,
                    mode="lines",
                    line=dict(color="rgba(255,255,255,0.3)", width=1),
                    fill="tozeroy",
                    fillcolor="rgba(100,100,100,0.2)",
                    hoverinfo="skip",
                    showlegend=False,
                ),
                row=1,
                col=1,
            )

            # Colored markers by smoothed grade (top)
            fig.add_trace(
                go.Scatter(
                    x=dist,
                    y=alt,
                    mode="markers",
                    marker=dict(
                        size=4,
                        color=grade,
                        colorscale=[
                            [0, "#2196F3"],
                            [0.3, "#4CAF50"],
                            [0.5, "#FFEB3B"],
                            [0.7, "#FF9800"],
                            [1.0, "#F44336"],
                        ],
                        cmin=-5,
                        cmax=15,
                        showscale=True,
                        colorbar=dict(title="Grade %", thickness=10, len=0.5),
                    ),
                    hovertemplate="%{y:.0f} ft | %{marker.color:.1f}%<extra></extra>",
                    showlegend=False,
                ),
                row=1,
                col=1,
            )

            # Instant grade (~5s) — closer to Garmin display (bottom)
            fig.add_trace(
                go.Scatter(
                    x=dist,
                    y=instant,
                    mode="lines",
                    name="~5s Grade",
                    line=dict(color="rgba(255,152,0,0.6)", width=1),
                    hovertemplate="%{y:.1f}%<extra>~5s</extra>",
                ),
                row=2,
                col=1,
            )

            # Smoothed grade (~20s) (bottom)
            fig.add_trace(
                go.Scatter(
                    x=dist,
                    y=grade,
                    mode="lines",
                    name="~20s Grade",
                    line=dict(color="#2196F3", width=2),
                    hovertemplate="%{y:.1f}%<extra>~20s</extra>",
                ),
                row=2,
                col=1,
            )

            # Zero line on grade subplot
            fig.add_hline(
                y=0, line_dash="dot", line_color=COLORS["muted"], row=2, col=1
            )

            # Delta between instant and smoothed (bottom)
            delta = [round(i - s, 1) for i, s in zip(instant, grade)]
            fig.add_trace(
                go.Scatter(
                    x=dist,
                    y=delta,
                    mode="lines",
                    name="Delta",
                    line=dict(color="rgba(255,152,0,0.8)", width=1),
                    hovertemplate="Delta: %{y:+.0f}%<extra></extra>",
                    showlegend=False,
                ),
                row=3,
                col=1,
            )
            fig.add_hline(
                y=0, line_dash="dot", line_color=COLORS["muted"], row=3, col=1
            )

    fig.update_layout(
        paper_bgcolor=COLORS["card"],
        plot_bgcolor=COLORS["card"],
        font_color=COLORS["text"],
        hovermode="x unified",
        height=650,
        margin=dict(t=30, b=60, l=60, r=30),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            font=dict(size=10),
        ),
    )
    fig.update_xaxes(gridcolor=COLORS["border"], row=1, col=1)
    fig.update_xaxes(gridcolor=COLORS["border"], row=2, col=1)
    fig.update_xaxes(gridcolor=COLORS["border"], title="Distance (mi)", row=3, col=1)
    fig.update_yaxes(gridcolor=COLORS["border"], title="Elevation (ft)", row=1, col=1)
    fig.update_yaxes(gridcolor=COLORS["border"], title="Grade %", row=2, col=1)
    fig.update_yaxes(gridcolor=COLORS["border"], title="Delta %", row=3, col=1)

    # Style subplot titles
    for ann in fig.layout.annotations:
        ann.font.color = COLORS["muted"]
        ann.font.size = 11

    return fig


@callback(
    Output("climbs-section", "children"),
    Input("ride-source-file", "data"),
    State("user-store", "data"),
)
def update_climbs_section(ride_data, user_data):
    source_file, ride_ts = _normalize_ride_data(ride_data)
    if not source_file:
        return []

    cp = _get_cached_processor(get_user_id(user_data), ride_ts)
    climbs = cp.detect_climbs(source_file)
    if not climbs:
        return []

    header = html.H3(
        f"Detected Climbs ({len(climbs)})",
        style={
            "color": COLORS["accent"],
            "marginBottom": "12px",
            "marginTop": "24px",
            "fontSize": "0.95rem",
        },
    )

    climb_cards = []
    for i, c in enumerate(climbs, 1):
        duration_str = _fmt_ride_time(c["duration_s"]) if c["duration_s"] else "—"
        vam_str = f"{c['vam']} m/h" if c["vam"] else "—"
        climb_cards.append(
            html.Div(
                style={
                    **CARD_STYLE,
                    "marginBottom": "8px",
                    "display": "flex",
                    "alignItems": "center",
                    "gap": "24px",
                    "padding": "12px 16px",
                },
                children=[
                    html.Div(
                        f"#{i}",
                        style={
                            "fontSize": "1.1rem",
                            "fontWeight": "700",
                            "color": COLORS["accent"],
                            "minWidth": "30px",
                        },
                    ),
                    html.Div(
                        style={"display": "flex", "flexWrap": "wrap", "gap": "16px"},
                        children=[
                            _climb_stat(
                                "Location", f"mi {c['start_mi']}–{c['end_mi']}"
                            ),
                            _climb_stat("Distance", f"{c['distance_mi']} mi"),
                            _climb_stat("Gain", f"{c['elevation_gain_ft']} ft"),
                            _climb_stat("Time", duration_str),
                            _climb_stat("Avg Grade", f"{c['avg_grade']}%"),
                            _climb_stat("Max Grade", f"{c['max_grade']}%"),
                            _climb_stat(
                                "Avg Power",
                                f"{c['avg_power']}W" if c.get("avg_power") else "—",
                            ),
                            _climb_stat(
                                "NP",
                                f"{c['normalized_power']}W"
                                if c.get("normalized_power")
                                else "—",
                            ),
                            _climb_stat(
                                "Avg Cadence",
                                f"{c['avg_cadence']} rpm"
                                if c.get("avg_cadence")
                                else "—",
                            ),
                            _climb_stat("VAM", vam_str),
                        ],
                    ),
                ],
            )
        )

    return [header, *climb_cards]


@callback(
    Output("route-map-chart", "figure"),
    Input("ride-source-file", "data"),
    Input("map-color-mode", "value"),
    State("user-store", "data"),
)
def update_route_map(ride_data, color_mode, user_data):
    source_file, ride_ts = _normalize_ride_data(ride_data)
    fig = go.Figure()

    if source_file:
        cp = _get_cached_processor(get_user_id(user_data), ride_ts)
        route = cp.get_ride_route(source_file)
        if route["lat"]:
            # Base route line
            fig.add_trace(
                go.Scattermap(
                    lat=route["lat"],
                    lon=route["lon"],
                    mode="lines",
                    line=dict(color="#2196F3", width=3),
                    hoverinfo="skip",
                )
            )

            if color_mode == "power" and route["power"]:
                fig.add_trace(
                    go.Scattermap(
                        lat=route["lat"],
                        lon=route["lon"],
                        mode="markers",
                        marker=dict(
                            size=6,
                            color=route["power"],
                            colorscale=[
                                [0.0, "#313695"],  # deep blue   — easy / zone 1
                                [0.25, "#74add1"],  # light blue  — zone 2
                                [0.5, "#fee090"],  # yellow      — tempo / zone 3
                                [0.75, "#f46d43"],  # orange      — threshold / zone 4
                                [1.0, "#a50026"],  # dark red    — VO2max+ / zone 5+
                            ],
                            showscale=True,
                            colorbar=dict(title="W", thickness=12, len=0.6),
                        ),
                        hovertemplate="%{marker.color:.0f}W<extra></extra>",
                    )
                )

            elif color_mode == "elevation" and route["elevation"]:
                elev_ft = [e * 3.28084 for e in route["elevation"]]
                fig.add_trace(
                    go.Scattermap(
                        lat=route["lat"],
                        lon=route["lon"],
                        mode="markers",
                        marker=dict(
                            size=6,
                            color=elev_ft,
                            colorscale="Viridis",
                            showscale=True,
                            colorbar=dict(title="ft", thickness=12, len=0.6),
                        ),
                        hovertemplate="%{marker.color:.0f} ft<extra></extra>",
                    )
                )

            elif color_mode == "climbs":
                # Get elevation profile for grade data and detect climbs
                profile = cp.get_elevation_profile(source_file)
                climbs = cp.detect_climbs(source_file)

                if profile["grade_pct"] and route["lat"]:
                    # Color route by grade
                    grade = profile["grade_pct"]
                    # Profile may have fewer points than route (nulls filtered)
                    # Pad or trim to match route length
                    if len(grade) < len(route["lat"]):
                        grade = grade + [0.0] * (len(route["lat"]) - len(grade))
                    else:
                        grade = grade[: len(route["lat"])]

                    fig.add_trace(
                        go.Scattermap(
                            lat=route["lat"],
                            lon=route["lon"],
                            mode="markers",
                            marker=dict(
                                size=6,
                                color=grade,
                                colorscale=[
                                    [0, "#2196F3"],
                                    [0.3, "#4CAF50"],
                                    [0.5, "#FFEB3B"],
                                    [0.7, "#FF9800"],
                                    [1.0, "#F44336"],
                                ],
                                cmin=-5,
                                cmax=15,
                                showscale=True,
                                colorbar=dict(title="Grade %", thickness=12, len=0.6),
                            ),
                            hovertemplate="%{marker.color:.1f}%<extra></extra>",
                        )
                    )

                # Mark climb start/end with larger markers
                if climbs:
                    profile_dist = profile["distance_mi"]
                    route_lat = route["lat"]
                    route_lon = route["lon"]
                    # Build a distance list for the route to map climb miles to lat/lon
                    route_prof = cp.get_elevation_profile(source_file)
                    r_dist = (
                        route_prof["distance_mi"] if route_prof["distance_mi"] else []
                    )

                    for ci, c in enumerate(climbs, 1):
                        # Find indices closest to start_mi and end_mi
                        start_idx = _nearest_idx(r_dist, c["start_mi"])
                        end_idx = _nearest_idx(r_dist, c["end_mi"])
                        if start_idx is not None and end_idx is not None:
                            # Highlight climb segment
                            seg_lat = route_lat[start_idx : end_idx + 1]
                            seg_lon = route_lon[start_idx : end_idx + 1]
                            fig.add_trace(
                                go.Scattermap(
                                    lat=seg_lat,
                                    lon=seg_lon,
                                    mode="lines",
                                    line=dict(color="#F44336", width=6),
                                    hoverinfo="skip",
                                    showlegend=False,
                                )
                            )
                            # Start marker
                            fig.add_trace(
                                go.Scattermap(
                                    lat=[route_lat[start_idx]],
                                    lon=[route_lon[start_idx]],
                                    mode="markers+text",
                                    marker=dict(size=10, color="#F44336"),
                                    text=[f"#{ci}"],
                                    textposition="top center",
                                    textfont=dict(color="#fff", size=11),
                                    hovertemplate=(
                                        f"Climb #{ci}<br>"
                                        f"{c['distance_mi']} mi, {c['elevation_gain_ft']} ft<br>"
                                        f"Avg {c['avg_grade']}%, Max {c['max_grade']}%"
                                        "<extra></extra>"
                                    ),
                                    showlegend=False,
                                )
                            )

            center_lat = sum(route["lat"]) / len(route["lat"])
            center_lon = sum(route["lon"]) / len(route["lon"])
            fig.update_layout(
                map=dict(
                    style="open-street-map",
                    center=dict(lat=center_lat, lon=center_lon),
                    zoom=11,
                ),
            )

    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        font_color=COLORS["text"],
        height=500,
        margin=dict(t=0, b=0, l=0, r=0),
        showlegend=False,
    )
    return fig
