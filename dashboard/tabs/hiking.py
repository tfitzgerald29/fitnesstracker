import plotly.graph_objects as go
import polars as pl
from dash import Input, Output, callback, dcc, html

from backend.hiking_processor import HikingProcessor
from ..config import CARD_STYLE, COLORS, get_user_id

ACCENT = "#8BC34A"  # hiking green


# ── Private helpers ───────────────────────────────────────────────────────────


def _fmt_time(seconds) -> str:
    if seconds is None:
        return "—"
    s = int(seconds)
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    if h:
        return f"{h}h {m}m"
    return f"{m}m {sec}s"


def _stat_card(label, value, sub=""):
    children = [
        html.Div(
            str(value),
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
            "padding": "14px 20px",
            "minWidth": "120px",
        },
        children=children,
    )


# ── Layout ────────────────────────────────────────────────────────────────────


def hiking_tab():
    return html.Div(
        [
            # ── Summary cards ─────────────────────────────────────────────────
            html.Div(id="hiking-summary-cards"),
            # ── Monthly trends ─────────────────────────────────────────────────
            html.H3(
                "Monthly Trends",
                style={
                    "color": ACCENT,
                    "marginBottom": "12px",
                    "marginTop": "24px",
                    "fontSize": "0.95rem",
                },
            ),
            html.Div(dcc.Graph(id="hiking-trends-chart"), style=CARD_STYLE),
            # ── Hike selector ──────────────────────────────────────────────────
            html.H3(
                "Hikes",
                style={
                    "color": ACCENT,
                    "marginBottom": "12px",
                    "marginTop": "24px",
                    "fontSize": "0.95rem",
                },
            ),
            dcc.Dropdown(
                id="hiking-session-dropdown",
                placeholder="Select a hike...",
                style={"marginBottom": "16px", "color": "#000"},
            ),
            # ── Session stat cards ─────────────────────────────────────────────
            html.Div(id="hiking-session-detail"),
            # ── Route map (static — hidden until a hike is selected) ───────────
            dcc.Store(id="hiking-source-file"),
            html.Div(
                id="hiking-map-section",
                style={"display": "none"},
                children=[
                    html.H3(
                        "Route Map",
                        style={
                            "color": ACCENT,
                            "marginBottom": "12px",
                            "marginTop": "24px",
                            "fontSize": "0.95rem",
                        },
                    ),
                    dcc.RadioItems(
                        id="hiking-map-mode",
                        options=[
                            {"label": "Elevation", "value": "elevation"},
                            {"label": "Heart Rate", "value": "heart_rate"},
                            {"label": "Route", "value": "route"},
                        ],
                        value="elevation",
                        inline=True,
                        labelStyle={
                            "marginRight": "16px",
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
                    html.Div(dcc.Graph(id="hiking-route-map"), style=CARD_STYLE),
                ],
            ),
        ]
    )


# ── Callbacks ─────────────────────────────────────────────────────────────────


@callback(
    Output("hiking-summary-cards", "children"),
    Output("hiking-session-dropdown", "options"),
    Output("hiking-session-dropdown", "value"),
    Output("hiking-trends-chart", "figure"),
    Input("tabs", "value"),
)
def update_hiking_overview(tab):
    if tab != "hiking":
        return [], [], None, go.Figure()

    hp = HikingProcessor(user_id=get_user_id())

    if hp.hiking.is_empty():
        return (
            [html.Div("No hiking data found.", style={"color": COLORS["muted"]})],
            [],
            None,
            go.Figure(),
        )

    stats = hp.summary_stats()
    cards = html.Div(
        style={
            "display": "flex",
            "gap": "12px",
            "flexWrap": "wrap",
            "marginBottom": "8px",
        },
        children=[
            _stat_card("Hikes", stats.get("total_hikes", 0)),
            _stat_card("Total Miles", stats.get("total_miles", 0)),
            _stat_card("Total Hours", stats.get("total_hours", 0)),
            _stat_card("Total Ascent", f"{stats.get('total_ascent_ft', 0):,}", "ft"),
        ],
    )

    options = hp.list_hikes()
    default_value = options[0]["value"] if options else None

    monthly = hp.monthly_summary()
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=monthly["month"].to_list(),
            y=monthly["miles"].to_list(),
            name="Miles",
            marker_color=ACCENT,
        )
    )
    fig.add_trace(
        go.Scatter(
            x=monthly["month"].to_list(),
            y=monthly["hours"].to_list(),
            name="Hours",
            yaxis="y2",
            mode="lines+markers",
            line={"color": "#FF9800", "width": 2},
        )
    )
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        yaxis={"title": "Miles", "gridcolor": COLORS["border"]},
        yaxis2={
            "title": "Hours",
            "overlaying": "y",
            "side": "right",
            "gridcolor": COLORS["border"],
        },
        xaxis={"gridcolor": COLORS["border"]},
        legend={"orientation": "h", "y": 1.12},
        margin={"t": 40, "b": 40, "l": 50, "r": 50},
    )

    return cards, options, default_value, fig


@callback(
    Output("hiking-session-detail", "children"),
    Output("hiking-source-file", "data"),
    Output("hiking-map-section", "style"),
    Input("hiking-session-dropdown", "value"),
)
def update_hiking_session(source_file):
    hidden = {"display": "none"}
    visible = {}

    if not source_file:
        return [], None, hidden

    hp = HikingProcessor(user_id=get_user_id())
    if hp.hiking.is_empty():
        return [], None, hidden

    session = hp.hiking.filter(pl.col("source_file") == source_file)
    if session.is_empty():
        return [], None, hidden

    r = session.to_dicts()[0]

    ascent_ft = round(r["total_ascent"] * 3.28084) if r.get("total_ascent") else None
    descent_ft = round(r["total_descent"] * 3.28084) if r.get("total_descent") else None

    # Average pace using timer time (moving_time is not populated for hikes)
    pace_str = "—"
    dist_m = r.get("total_distance")
    timer_s = r.get("total_timer_time")
    if dist_m and timer_s and dist_m > 0:
        secs_per_mile = timer_s / (dist_m / 1609.344)
        pace_str = f"{int(secs_per_mile // 60)}:{int(secs_per_mile % 60):02d} /mi"

    cards = html.Div(
        style={
            "display": "flex",
            "gap": "12px",
            "flexWrap": "wrap",
            "marginBottom": "16px",
        },
        children=[
            _stat_card("Date", r["timestamp"].strftime("%Y-%m-%d")),
            _stat_card(
                "Distance",
                f"{round(r['total_distance'] / 1609.344, 2)} mi"
                if r.get("total_distance")
                else "—",
            ),
            _stat_card("Duration", _fmt_time(r.get("total_timer_time"))),
            _stat_card("Avg Pace", pace_str),
            _stat_card("Ascent", f"{ascent_ft:,} ft" if ascent_ft else "—"),
            _stat_card("Descent", f"{descent_ft:,} ft" if descent_ft else "—"),
            _stat_card(
                "Avg HR",
                int(r["avg_heart_rate"]) if r.get("avg_heart_rate") else "—",
                "bpm",
            ),
            _stat_card(
                "Max HR",
                int(r["max_heart_rate"]) if r.get("max_heart_rate") else "—",
                "bpm",
            ),
            _stat_card(
                "Calories",
                f"{int(r['total_calories']):,}" if r.get("total_calories") else "—",
            ),
        ],
    )
    return [cards], source_file, visible


@callback(
    Output("hiking-route-map", "figure"),
    Input("hiking-source-file", "data"),
    Input("hiking-map-mode", "value"),
)
def update_hiking_route_map(source_file, color_mode):
    fig = go.Figure()

    if not source_file:
        fig.update_layout(
            paper_bgcolor=COLORS["card"],
            font_color=COLORS["text"],
            height=500,
            margin={"t": 0, "b": 0, "l": 0, "r": 0},
        )
        return fig

    hp = HikingProcessor(user_id=get_user_id())
    route = hp.get_hike_route(source_file)

    if not route["lat"]:
        fig.update_layout(
            paper_bgcolor=COLORS["card"],
            font_color=COLORS["text"],
            height=500,
            margin={"t": 0, "b": 0, "l": 0, "r": 0},
            annotations=[
                {
                    "text": "No GPS data for this hike",
                    "xref": "paper",
                    "yref": "paper",
                    "x": 0.5,
                    "y": 0.5,
                    "showarrow": False,
                    "font": {"color": COLORS["muted"]},
                }
            ],
        )
        return fig

    # Base route line — dark green for contrast on OSM's light background
    fig.add_trace(
        go.Scattermap(
            lat=route["lat"],
            lon=route["lon"],
            mode="lines",
            line={"color": "#2E7D32", "width": 3},
            hoverinfo="skip",
            showlegend=False,
        )
    )

    if color_mode == "elevation":
        elev = route["elevation_ft"]
        valid_elev = [e for e in elev if e is not None]
        if valid_elev:
            fig.add_trace(
                go.Scattermap(
                    lat=route["lat"],
                    lon=route["lon"],
                    mode="markers",
                    marker={
                        "size": 4,
                        "color": elev,
                        "colorscale": "Viridis",
                        "showscale": True,
                        "colorbar": {"title": "ft", "thickness": 10, "len": 0.5},
                    },
                    hovertemplate="%{marker.color:.0f} ft<extra></extra>",
                    showlegend=False,
                )
            )

    elif color_mode == "heart_rate":
        hr = route["heart_rate"]
        valid_hr = [h for h in hr if h is not None]
        if valid_hr:
            fig.add_trace(
                go.Scattermap(
                    lat=route["lat"],
                    lon=route["lon"],
                    mode="markers",
                    marker={
                        "size": 4,
                        "color": hr,
                        "colorscale": "YlOrRd",
                        "showscale": True,
                        "colorbar": {"title": "bpm", "thickness": 10, "len": 0.5},
                    },
                    hovertemplate="%{marker.color:.0f} bpm<extra></extra>",
                    showlegend=False,
                )
            )

    # Start and end markers
    fig.add_trace(
        go.Scattermap(
            lat=[route["lat"][0]],
            lon=[route["lon"][0]],
            mode="markers",
            marker={"size": 10, "color": "#4CAF50"},
            hovertemplate="Start<extra></extra>",
            showlegend=False,
        )
    )
    fig.add_trace(
        go.Scattermap(
            lat=[route["lat"][-1]],
            lon=[route["lon"][-1]],
            mode="markers",
            marker={"size": 10, "color": "#F44336"},
            hovertemplate="End<extra></extra>",
            showlegend=False,
        )
    )

    center_lat = sum(route["lat"]) / len(route["lat"])
    center_lon = sum(route["lon"]) / len(route["lon"])

    fig.update_layout(
        map={
            "style": "open-street-map",
            "center": {"lat": center_lat, "lon": center_lon},
            "zoom": 12,
        },
        paper_bgcolor="rgba(0,0,0,0)",
        font_color=COLORS["text"],
        height=500,
        margin={"t": 0, "b": 0, "l": 0, "r": 0},
        showlegend=False,
    )
    return fig
