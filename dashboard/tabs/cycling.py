from dash import Input, Output, callback, dcc, html
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from backend.cycling import CyclingProcessor

from ..config import CARD_STYLE, COLORS


def _x_labels(df, group_by):
    if group_by == "year":
        return df["year"].cast(str).to_list()
    elif group_by == "month":
        return [f"{r['year']}-{r['month']:02d}" for r in df.to_dicts()]
    else:
        return df["week_starting"].to_list()


def _cycling_overview():
    return html.Div([
        # Cycling summary section
        html.H3("Cycling Summary", style={"color": COLORS["accent"], "marginBottom": "12px", "fontSize": "0.95rem"}),
        dcc.RadioItems(
            id="cycling-summary-group",
            options=[
                {"label": "Annual", "value": "year"},
                {"label": "Monthly", "value": "month"},
                {"label": "Weekly", "value": "week"},
            ],
            value="month",
            inline=True,
            labelStyle={"marginRight": "12px", "cursor": "pointer"},
            inputStyle={"marginRight": "4px"},
            style={"marginBottom": "12px"},
        ),
        html.Div(dcc.Graph(id="cycling-summary-chart"), style=CARD_STYLE),

        # Training load section
        html.H3("Training Load", style={"color": COLORS["accent"], "marginBottom": "12px", "marginTop": "24px", "fontSize": "0.95rem"}),
        html.Div(
            style={"display": "flex", "gap": "8px", "marginBottom": "12px"},
            children=[
                dcc.RadioItems(
                    id="date-range",
                    options=[
                        {"label": "3M", "value": "3"},
                        {"label": "6M", "value": "6"},
                        {"label": "1Y", "value": "12"},
                        {"label": "All", "value": "all"},
                    ],
                    value="12",
                    inline=True,
                    labelStyle={"marginRight": "12px", "cursor": "pointer"},
                    inputStyle={"marginRight": "4px"},
                ),
                dcc.Checklist(
                    id="show-forecast",
                    options=[{"label": " Show Forecast", "value": "yes"}],
                    value=["yes"],
                    style={"marginLeft": "24px"},
                    inputStyle={"marginRight": "4px"},
                ),
            ],
        ),
        html.Div(dcc.Graph(id="training-load-chart"), style=CARD_STYLE),
    ])


def _cycling_rides():
    cp = CyclingProcessor()
    ride_options = cp.list_rides()
    return html.Div([
        dcc.Dropdown(
            id="ride-selector",
            options=ride_options,
            placeholder="Search by date...",
            searchable=True,
            style={"marginBottom": "16px", "color": "#000"},
        ),
        html.Div(id="ride-detail-content"),
    ])


def cycling_tab():
    return html.Div([
        dcc.Tabs(
            id="cycling-subtabs",
            value="overview",
            children=[
                dcc.Tab(
                    label="Overview", value="overview",
                    style={"padding": "6px 16px", "lineHeight": "28px"},
                    selected_style={"padding": "6px 16px", "lineHeight": "28px", "borderTop": f"2px solid {COLORS['accent']}"},
                ),
                dcc.Tab(
                    label="Rides", value="rides",
                    style={"padding": "6px 16px", "lineHeight": "28px"},
                    selected_style={"padding": "6px 16px", "lineHeight": "28px", "borderTop": f"2px solid {COLORS['accent']}"},
                ),
            ],
            style={"height": "40px", "marginBottom": "16px"},
            colors={
                "border": "transparent",
                "primary": COLORS["accent"],
                "background": "transparent",
            },
        ),
        html.Div(id="cycling-subtab-content"),
    ])


@callback(
    Output("cycling-subtab-content", "children"),
    Input("cycling-subtabs", "value"),
)
def render_cycling_subtab(subtab):
    if subtab == "rides":
        return _cycling_rides()
    return _cycling_overview()


@callback(
    Output("cycling-summary-chart", "figure"),
    Input("cycling-summary-group", "value"),
)
def update_cycling_summary(group_by):
    cp = CyclingProcessor()
    df = cp.summarize_cycling(group_by=group_by)

    if df.is_empty():
        fig = go.Figure()
        fig.update_layout(
            paper_bgcolor=COLORS["card"], plot_bgcolor=COLORS["card"],
            font_color=COLORS["text"],
            annotations=[{"text": "No cycling data", "showarrow": False, "font": {"size": 14, "color": COLORS["muted"]}}],
        )
        return fig

    x = _x_labels(df, group_by)
    miles = df["miles"].to_list()
    hours = df["hours"].to_list()
    tss = df["tss"].to_list()
    rides = df["rides"].to_list()

    fig = make_subplots(
        rows=3, cols=1,
        vertical_spacing=0.12,
        subplot_titles=["Miles", "Hours", "TSS"],
    )

    fig.add_trace(go.Bar(
        x=x, y=miles, name="Miles", marker_color="#2196F3",
        text=miles, textposition="outside", textfont_size=10,
    ), row=1, col=1)
    fig.add_trace(go.Bar(
        x=x, y=hours, name="Hours", marker_color="#4CAF50",
        text=hours, textposition="outside", textfont_size=10,
    ), row=2, col=1)
    fig.add_trace(go.Bar(
        x=x, y=tss, name="TSS", marker_color="#FF9800",
        text=tss, textposition="outside", textfont_size=10,
    ), row=3, col=1)

    fig.update_layout(
        paper_bgcolor=COLORS["card"],
        plot_bgcolor=COLORS["card"],
        font_color=COLORS["text"],
        showlegend=False,
        height=900,
        margin=dict(t=40, b=60, l=60, r=30),
    )

    # Set y-axis range with 30% headroom so "outside" labels don't collide
    for i, series in enumerate([miles, hours, tss], start=1):
        max_val = max(series) if series else 1
        fig.update_xaxes(gridcolor=COLORS["border"], row=i, col=1)
        fig.update_yaxes(
            gridcolor=COLORS["border"], automargin=True,
            range=[0, max_val * 1.3], row=i, col=1,
        )

    # Style subplot titles
    for ann in fig.layout.annotations:
        ann.font.color = COLORS["muted"]
        ann.font.size = 12

    return fig


@callback(
    Output("training-load-chart", "figure"),
    Input("date-range", "value"),
    Input("show-forecast", "value"),
)
def update_training_load(date_range, show_forecast):
    start_date = None
    if date_range != "all":
        from datetime import date, timedelta

        months = int(date_range)
        d = date.today()
        start_date = (d.replace(day=1) - timedelta(days=months * 30)).isoformat()

    cp = CyclingProcessor()
    fig = cp.plot_training_load(start_date=start_date, include_forecast="yes" in (show_forecast or []))

    fig.update_layout(
        paper_bgcolor=COLORS["card"],
        plot_bgcolor=COLORS["card"],
        font_color=COLORS["text"],
        xaxis=dict(gridcolor=COLORS["border"]),
        yaxis=dict(gridcolor=COLORS["border"]),
        height=550,
    )
    return fig


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
            html.Div(display, style={"fontSize": "1.3rem", "fontWeight": "600", "color": "#fff"}),
            html.Div(label, style={"fontSize": "0.75rem", "color": COLORS["muted"], "marginTop": "4px"}),
        ],
    )


@callback(
    Output("ride-detail-content", "children"),
    Input("ride-selector", "value"),
)
def update_ride_detail(ride_ts):
    if not ride_ts:
        return html.Div("Select a ride to view details.", style={"color": COLORS["muted"]})

    cp = CyclingProcessor()
    s = cp.get_ride_summary(ride_ts)
    if not s:
        return html.Div("Ride not found.", style={"color": COLORS["muted"]})

    # Peak powers from record_mesgs
    peak_powers = cp.get_peak_powers(s["source_file"]) if s.get("source_file") else []
    peak_section = []
    if peak_powers:
        peak_section = [
            html.H3("Peak Power", style={"color": COLORS["accent"], "marginBottom": "12px", "marginTop": "24px", "fontSize": "0.95rem"}),
            html.Div(
                style={"display": "flex", "flexWrap": "wrap", "gap": "4px"},
                children=[_stat_card(p["duration"], p["watts"], "W") for p in peak_powers],
            ),
        ]

    return html.Div([
        html.H3(s["date"], style={"color": COLORS["accent"], "marginBottom": "16px", "fontSize": "1rem"}),
        html.Div(
            style={"display": "flex", "flexWrap": "wrap", "gap": "4px"},
            children=[
                _stat_card("Distance", s["distance_mi"], "mi"),
                _stat_card("Ride Time", _fmt_ride_time(s.get("total_timer_time_s")), ""),
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
                _stat_card("L/R Balance", f"{s['left_balance']}/{s['right_balance']}" if s.get("left_balance") else None, "%"),
            ],
        ),
        *peak_section,
        # Power curve section
        html.H3("Power Curve", style={"color": COLORS["accent"], "marginBottom": "12px", "marginTop": "24px", "fontSize": "0.95rem"}),
        dcc.Checklist(
            id="power-curve-compare",
            options=[
                {"label": " 4 Weeks", "value": "1"},
                {"label": " 3 Months", "value": "3"},
                {"label": " 12 Months", "value": "12"},
            ],
            value=[],
            inline=True,
            labelStyle={"marginRight": "16px", "cursor": "pointer"},
            inputStyle={"marginRight": "4px"},
            style={"marginBottom": "12px"},
        ),
        dcc.Store(id="ride-source-file", data=s.get("source_file")),
        html.Div(dcc.Graph(id="power-curve-chart"), style=CARD_STYLE),
    ])


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


@callback(
    Output("power-curve-chart", "figure"),
    Input("ride-source-file", "data"),
    Input("power-curve-compare", "value"),
)
def update_power_curve(source_file, compare_periods):
    cp = CyclingProcessor()

    fig = go.Figure()

    # Current ride curve
    if source_file:
        curve = cp.get_power_curve(source_file)
        if curve["durations"]:
            labels = [_format_duration(d) for d in curve["durations"]]
            fig.add_trace(go.Scatter(
                x=labels, y=curve["watts"], name="This Ride",
                mode="lines", line=dict(color="#2196F3", width=3),
                hovertemplate="%{y}W<extra>This Ride</extra>",
            ))

    # Comparison curves
    compare_colors = {"1": "#FF9800", "3": "#4CAF50", "12": "#E91E63"}
    compare_labels = {"1": "Best 4 Weeks", "3": "Best 3 Months", "12": "Best 12 Months"}
    for period in (compare_periods or []):
        best = cp.get_best_power_curve(period_months=int(period))
        if best["durations"]:
            labels = [_format_duration(d) for d in best["durations"]]
            fig.add_trace(go.Scatter(
                x=labels, y=best["watts"], name=compare_labels[period],
                mode="lines", line=dict(color=compare_colors[period], width=2, dash="dash"),
                hovertemplate="%{y}W<extra>" + compare_labels[period] + "</extra>",
            ))

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
