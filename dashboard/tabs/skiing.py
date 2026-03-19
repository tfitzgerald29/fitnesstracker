import plotly.graph_objects as go
from dash import Input, Output, callback, dcc, html

from backend.skiing_processor import skiing
from ..config import CARD_STYLE, COLORS, get_user_id

ACCENT = "#00BCD4"  # alpine skiing cyan


# ── Private helpers ───────────────────────────────────────────────────────────


def _fmt_time(seconds) -> str:
    if seconds is None:
        return "—"
    s = int(seconds)
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    if h:
        return f"{h}h {m}m {sec}s"
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


def _table_style():
    return {
        "style_header": {
            "backgroundColor": COLORS["card"],
            "color": COLORS["text"],
            "fontWeight": "bold",
            "border": f"1px solid {COLORS['border']}",
        },
        "style_cell": {
            "backgroundColor": COLORS["bg"],
            "color": COLORS["text"],
            "border": f"1px solid {COLORS['border']}",
            "padding": "8px 12px",
            "textAlign": "center",
        },
        "style_table": {"overflowX": "auto"},
        "sort_action": "native",
    }


# ── Layout ────────────────────────────────────────────────────────────────────


def skiing_tab():
    return html.Div(
        [
            # ── Overall summary cards ──────────────────────────────────────────
            html.Div(id="skiing-summary-cards"),
            # ── Season selector ────────────────────────────────────────────────
            html.H3(
                "By Season",
                style={
                    "color": ACCENT,
                    "marginBottom": "12px",
                    "marginTop": "24px",
                    "fontSize": "0.95rem",
                },
            ),
            dcc.Dropdown(
                id="skiing-season-dropdown",
                placeholder="Select a season...",
                style={"marginBottom": "16px", "color": "#000"},
            ),
            html.Div(id="skiing-season-detail"),
            # ── Session selector ───────────────────────────────────────────────
            html.H3(
                "Sessions",
                style={
                    "color": ACCENT,
                    "marginBottom": "12px",
                    "marginTop": "24px",
                    "fontSize": "0.95rem",
                },
            ),
            html.H4(
                "Last 6 Months of data",
                style={
                    "color": ACCENT,
                    "marginBottom": "5px",
                    "marginTop": "5px",
                    "fontSize": "0.80rem",
                },
            ),
            dcc.Dropdown(
                id="skiing-session-dropdown",
                placeholder="Select a session...",
                style={"marginBottom": "16px", "color": "#000"},
            ),
            html.Div(id="skiing-session-detail"),
            # ── Route map (static, hidden until session selected) ──────────────
            dcc.Store(id="skiing-source-file"),
            html.Div(
                id="skiing-map-section",
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
                        id="skiing-map-mode",
                        options=[
                            {"label": "Elevation", "value": "elevation"},
                            {"label": "Speed", "value": "speed"},
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
                    html.Div(dcc.Graph(id="skiing-route-map"), style=CARD_STYLE),
                ],
            ),
        ]
    )


# ── Callbacks ─────────────────────────────────────────────────────────────────


@callback(
    Output("skiing-summary-cards", "children"),
    Output("skiing-season-dropdown", "options"),
    Output("skiing-season-dropdown", "value"),
    Input("tabs", "value"),
)
def update_skiing_overview(tab):
    if tab != "Ski":
        return [], [], None

    sp = skiing(user_id=get_user_id())
    if sp.skiing.is_empty():
        return (
            [html.Div("No skiing data found.", style={"color": COLORS["muted"]})],
            [],
            None,
        )

    stats = sp.summary_stats()
    cards = html.Div(
        style={
            "display": "flex",
            "gap": "12px",
            "flexWrap": "wrap",
            "marginBottom": "8px",
        },
        children=[
            _stat_card("Seasons", stats.get("total_seasons", 0)),
            _stat_card("Days", stats.get("total_days", 0)),
            _stat_card("Total Laps", stats.get("total_laps", 0)),
            _stat_card("Total Descent", f"{stats.get('total_descent_ft', 0):,}", "ft"),
            _stat_card("Max Speed", stats.get("max_speed_mph", "—"), "mph"),
        ],
    )

    annual = sp.annual_summary()
    season_options = [
        {"label": r["season"], "value": r["season"]}
        for r in annual.sort("season", descending=True).to_dicts()
    ]
    default_season = season_options[0]["value"] if season_options else None

    return cards, season_options, default_season


@callback(
    Output("skiing-season-detail", "children"),
    Output("skiing-session-dropdown", "options"),
    Output("skiing-session-dropdown", "value"),
    Input("skiing-season-dropdown", "value"),
)
def update_skiing_season(season_value):
    if not season_value:
        return [], [], None

    sp = skiing(user_id=get_user_id())
    if sp.skiing.is_empty():
        return [], [], None

    # Season detail cards
    annual = sp.annual_summary()
    season_rows = annual.filter(annual["season"] == season_value)
    if season_rows.is_empty():
        season_cards = []
    else:
        r = season_rows.to_dicts()[0]
        descent_ft = int(r["total_descent"] * 3.28084) if r.get("total_descent") else 0
        max_spd = round(r["max_speed"] * 2.23694, 1) if r.get("max_speed") else "—"
        season_cards = html.Div(
            style={
                "display": "flex",
                "gap": "12px",
                "flexWrap": "wrap",
                "marginBottom": "8px",
            },
            children=[
                _stat_card(
                    "First Day", str(r["first_day"]) if r.get("first_day") else "—"
                ),
                _stat_card(
                    "Last Day", str(r["last_day"]) if r.get("last_day") else "—"
                ),
                _stat_card("Total Days", r.get("total_days", 0)),
                _stat_card("Ski Days", r.get("ski_days", 0)),
                _stat_card("BC Days", r.get("bc_days", 0)),
                _stat_card("Laps", int(r["num_laps"]) if r.get("num_laps") else 0),
                _stat_card("Descent", f"{descent_ft:,}", "ft"),
                _stat_card("Max Speed", max_spd, "mph"),
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
            ],
        )

    # Session dropdown filtered to this season
    all_sessions = sp.list_sessions()
    season_sessions = [s for s in all_sessions if s["season"] == season_value]
    # Dropdown options don't need the season key
    options = [{"label": s["label"], "value": s["value"]} for s in season_sessions]
    default = options[0]["value"] if options else None

    return season_cards, options, default


@callback(
    Output("skiing-session-detail", "children"),
    Output("skiing-source-file", "data"),
    Output("skiing-map-section", "style"),
    Input("skiing-session-dropdown", "value"),
)
def update_skiing_session(source_file):
    hidden = {"display": "none"}
    visible = {}

    if not source_file:
        return [], None, hidden

    sp = skiing(user_id=get_user_id())
    if sp.skiing.is_empty():
        return [], None, hidden

    import polars as pl

    session = sp.skiing.filter(pl.col("source_file") == source_file)
    if session.is_empty():
        return [], None, hidden

    r = session.to_dicts()[0]

    descent_ft = round(r["total_descent"] * 3.28084) if r.get("total_descent") else None
    ascent_ft = round(r["total_ascent"] * 3.28084) if r.get("total_ascent") else None
    max_spd = (
        round(r["enhanced_max_speed"] * 2.23694, 1)
        if r.get("enhanced_max_speed")
        else "—"
    )
    avg_spd = (
        round(r["enhanced_avg_speed"] * 2.23694, 1)
        if r.get("enhanced_avg_speed")
        else "—"
    )
    laps = int(r["num_laps"]) if r.get("num_laps") else 0
    profile = r.get("sport_profile_name") or "Ski"

    cards = html.Div(
        style={
            "display": "flex",
            "gap": "12px",
            "flexWrap": "wrap",
            "marginBottom": "16px",
        },
        children=[
            _stat_card("Date", str(r["DT_DENVER"])),
            _stat_card("Profile", profile),
            _stat_card("Laps", laps),
            _stat_card("Descent", f"{descent_ft:,} ft" if descent_ft else "—"),
            _stat_card("Ascent", f"{ascent_ft:,} ft" if ascent_ft else "—"),
            _stat_card("Moving", _fmt_time(r.get("total_moving_time"))),
            _stat_card("Elapsed", _fmt_time(r.get("total_elapsed_time"))),
            _stat_card("Max Speed", max_spd, "mph"),
            _stat_card("Avg Speed", avg_spd, "mph"),
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
    Output("skiing-route-map", "figure"),
    Input("skiing-source-file", "data"),
    Input("skiing-map-mode", "value"),
)
def update_skiing_route_map(source_file, color_mode):
    fig = go.Figure()
    base_layout = {
        "paper_bgcolor": "rgba(0,0,0,0)",
        "font_color": COLORS["text"],
        "height": 500,
        "margin": {"t": 0, "b": 0, "l": 0, "r": 0},
        "showlegend": False,
    }

    if not source_file:
        fig.update_layout(**base_layout)
        return fig

    sp = skiing(user_id=get_user_id())
    route = sp.get_ski_route(source_file)

    if not route["lat"]:
        fig.update_layout(
            **base_layout,
            annotations=[
                {
                    "text": "No GPS data for this session",
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

    # Base route line — dark blue for contrast on OSM's light background
    fig.add_trace(
        go.Scattermap(
            lat=route["lat"],
            lon=route["lon"],
            mode="lines",
            line={"color": "#006064", "width": 3},
            hoverinfo="skip",
            showlegend=False,
        )
    )

    if color_mode == "elevation":
        elev = route["elevation_ft"]
        if any(e is not None for e in elev):
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

    elif color_mode == "speed":
        spd = route["speed_mph"]
        if any(s is not None for s in spd):
            fig.add_trace(
                go.Scattermap(
                    lat=route["lat"],
                    lon=route["lon"],
                    mode="markers",
                    marker={
                        "size": 4,
                        "color": spd,
                        "colorscale": "YlOrRd",
                        "showscale": True,
                        "colorbar": {"title": "mph", "thickness": 10, "len": 0.5},
                    },
                    hovertemplate="%{marker.color:.1f} mph<extra></extra>",
                    showlegend=False,
                )
            )

    elif color_mode == "heart_rate":
        hr = route["heart_rate"]
        if any(h is not None for h in hr):
            fig.add_trace(
                go.Scattermap(
                    lat=route["lat"],
                    lon=route["lon"],
                    mode="markers",
                    marker={
                        "size": 4,
                        "color": hr,
                        "colorscale": "RdPu",
                        "showscale": True,
                        "colorbar": {"title": "bpm", "thickness": 10, "len": 0.5},
                    },
                    hovertemplate="%{marker.color:.0f} bpm<extra></extra>",
                    showlegend=False,
                )
            )

    # Start (green) and end (red) markers
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
        **base_layout,
        map={
            "style": "open-street-map",
            "center": {"lat": center_lat, "lon": center_lon},
            "zoom": 13,
        },
    )
    return fig
