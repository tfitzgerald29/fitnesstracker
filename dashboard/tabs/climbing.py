import os

import plotly.graph_objects as go
import polars as pl
from dash import Input, Output, callback, dash_table, dcc, html

from ..config import CARD_STYLE, COLORS, MERGED_PATH

FEET_PER_ROUTE = 45


def _load_climbing_data():
    """Load session and split data filtered to rock climbing."""
    sessions_path = os.path.join(MERGED_PATH, "session_mesgs.parquet")
    splits_path = os.path.join(MERGED_PATH, "split_mesgs.parquet")
    summaries_path = os.path.join(MERGED_PATH, "split_summary_mesgs.parquet")

    if not os.path.exists(sessions_path):
        return None, None, None

    sessions = pl.read_parquet(sessions_path).filter(pl.col("sport") == "rock_climbing")
    if sessions.is_empty():
        return sessions, pl.DataFrame(), pl.DataFrame()

    climbing_files = set(sessions["source_file"].unique().to_list())

    splits = pl.DataFrame()
    if os.path.exists(splits_path):
        splits = pl.read_parquet(splits_path).filter(pl.col("source_file").is_in(climbing_files))

    summaries = pl.DataFrame()
    if os.path.exists(summaries_path):
        summaries = pl.read_parquet(summaries_path).filter(pl.col("source_file").is_in(climbing_files))

    return sessions, splits, summaries


def _count_routes(splits, source_file=None):
    """Count climb_active splits, optionally filtered to a source file."""
    if splits.is_empty():
        return 0
    f = splits.filter(pl.col("split_type") == "climb_active")
    if source_file is not None:
        f = f.filter(pl.col("source_file") == source_file)
    return len(f)


def _stat_card(label, value, sub=""):
    children = [
        html.Div(
            str(value),
            style={"fontSize": "1.5rem", "fontWeight": "bold", "color": COLORS["accent"]},
        ),
        html.Div(
            label,
            style={"fontSize": "0.8rem", "color": COLORS["muted"], "marginTop": "4px"},
        ),
    ]
    if sub:
        children.append(
            html.Div(sub, style={"fontSize": "0.7rem", "color": COLORS["muted"], "marginTop": "2px"})
        )
    return html.Div(
        style={**CARD_STYLE, "display": "inline-block", "textAlign": "center", "padding": "16px 28px", "minWidth": "140px"},
        children=children,
    )


def _format_duration(seconds):
    """Format seconds as Xh Ym or Ym Zs."""
    if seconds is None:
        return "—"
    seconds = float(seconds)
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    if h > 0:
        return f"{h}h {m}m"
    return f"{m}m {s}s"


def climbing_tab():
    return html.Div(
        [
            html.Div(id="climbing-summary-cards"),
            html.H3(
                "Monthly Trends",
                style={"color": COLORS["accent"], "marginBottom": "12px", "marginTop": "24px", "fontSize": "0.95rem"},
            ),
            html.Div(dcc.Graph(id="climbing-trends-chart"), style=CARD_STYLE),
            html.H3(
                "Sessions",
                style={"color": COLORS["accent"], "marginBottom": "12px", "marginTop": "24px", "fontSize": "0.95rem"},
            ),
            dcc.Dropdown(
                id="climbing-session-dropdown",
                placeholder="Select a climbing session...",
                style={"marginBottom": "16px", "color": "#000"},
            ),
            html.Div(id="climbing-session-detail"),
        ]
    )


@callback(
    Output("climbing-summary-cards", "children"),
    Output("climbing-session-dropdown", "options"),
    Output("climbing-session-dropdown", "value"),
    Output("climbing-trends-chart", "figure"),
    Input("tabs", "value"),
)
def update_climbing_overview(tab):
    if tab != "climbing":
        return [], [], None, go.Figure()

    sessions, splits, _summaries = _load_climbing_data()

    if sessions is None or sessions.is_empty():
        return [html.Div("No climbing data found.", style={"color": COLORS["muted"]})], [], None, go.Figure()

    # -- Summary cards --
    total_sessions = len(sessions)
    total_routes = _count_routes(splits)
    total_hours = round(sessions["total_timer_time"].sum() / 3600, 1)
    total_calories = int(sessions["total_calories"].drop_nulls().sum()) if "total_calories" in sessions.columns else 0
    avg_routes = round(total_routes / total_sessions, 1) if total_sessions > 0 else 0
    total_ascent = total_routes * FEET_PER_ROUTE

    cards = html.Div(
        style={"display": "flex", "gap": "12px", "flexWrap": "wrap", "marginBottom": "8px"},
        children=[
            _stat_card("Sessions", total_sessions),
            _stat_card("Total Routes", total_routes),
            _stat_card("Avg Routes/Session", avg_routes),
            _stat_card("Total Hours", total_hours),
            _stat_card("Total Calories", f"{total_calories:,}"),
            _stat_card("Total Ascent", f"{total_ascent:,}", "ft"),
        ],
    )

    # -- Session dropdown (sorted most recent first) --
    ts_col = "timestamp"
    df = sessions.clone()
    if df[ts_col].dtype.time_zone is None:
        df = df.with_columns(pl.col(ts_col).dt.replace_time_zone("UTC"))
    df = df.with_columns(pl.col(ts_col).dt.convert_time_zone("America/Denver"))
    df = df.sort(ts_col, descending=True)

    options = []
    for r in df.to_dicts():
        dt = r[ts_col]
        duration = _format_duration(r.get("total_timer_time"))
        n_routes = _count_routes(splits, r["source_file"])
        label = f"{dt.strftime('%Y-%m-%d')} — {n_routes} routes, {duration}"
        options.append({"label": label, "value": r["source_file"]})

    # Default to most recent session
    default_value = options[0]["value"] if options else None

    # -- Monthly trends chart --
    df = df.with_columns(
        pl.col(ts_col).dt.strftime("%Y-%m").alias("month"),
    )
    monthly = df.group_by("month").agg(
        pl.len().alias("sessions"),
        (pl.col("total_timer_time").sum() / 3600).round(1).alias("hours"),
    ).sort("month")

    # Count routes per month
    if not splits.is_empty():
        active_splits = splits.filter(pl.col("split_type") == "climb_active")
        active_splits = active_splits.with_columns(
            pl.col("start_time").dt.convert_time_zone("America/Denver").dt.strftime("%Y-%m").alias("month")
        )
        routes_monthly = active_splits.group_by("month").agg(pl.len().alias("routes"))
        monthly = monthly.join(routes_monthly, on="month", how="left").with_columns(
            pl.col("routes").fill_null(0)
        )
    else:
        monthly = monthly.with_columns(pl.lit(0).alias("routes"))

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=monthly["month"].to_list(),
        y=monthly["routes"].to_list(),
        name="Routes",
        marker_color=COLORS["accent"],
    ))
    fig.add_trace(go.Scatter(
        x=monthly["month"].to_list(),
        y=monthly["hours"].to_list(),
        name="Hours",
        yaxis="y2",
        mode="lines+markers",
        line={"color": "#FF6B35", "width": 2},
    ))
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        yaxis={"title": "Routes", "gridcolor": COLORS["border"]},
        yaxis2={"title": "Hours", "overlaying": "y", "side": "right", "gridcolor": COLORS["border"]},
        xaxis={"gridcolor": COLORS["border"]},
        legend={"orientation": "h", "y": 1.12},
        margin={"t": 40, "b": 40, "l": 50, "r": 50},
    )

    return cards, options, default_value, fig


@callback(
    Output("climbing-session-detail", "children"),
    Input("climbing-session-dropdown", "value"),
)
def update_climbing_session(source_file):
    if not source_file:
        return []

    sessions, splits, _summaries = _load_climbing_data()
    if sessions is None or sessions.is_empty():
        return []

    # Session summary
    session = sessions.filter(pl.col("source_file") == source_file)
    if session.is_empty():
        return []

    r = session.to_dicts()[0]
    total_time = _format_duration(r.get("total_timer_time"))
    calories = int(r["total_calories"]) if r.get("total_calories") else "—"
    avg_hr = int(r["avg_heart_rate"]) if r.get("avg_heart_rate") else "—"
    max_hr = int(r["max_heart_rate"]) if r.get("max_heart_rate") else "—"
    n_routes = _count_routes(splits, source_file)
    ascent_ft = n_routes * FEET_PER_ROUTE

    # Compute total climb time and rest time from splits
    climb_time_s = 0
    rest_time_s = 0
    if not splits.is_empty():
        session_splits_for_time = splits.filter(pl.col("source_file") == source_file)
        if not session_splits_for_time.is_empty():
            climb_time_s = session_splits_for_time.filter(
                pl.col("split_type") == "climb_active"
            )["total_timer_time"].sum()
            rest_time_s = session_splits_for_time.filter(
                pl.col("split_type") == "climb_rest"
            )["total_timer_time"].sum()

    session_cards = html.Div(
        style={"display": "flex", "gap": "12px", "flexWrap": "wrap", "marginBottom": "16px"},
        children=[
            _stat_card("Duration", total_time),
            _stat_card("Routes", n_routes),
            _stat_card("Climb Time", _format_duration(climb_time_s)),
            _stat_card("Rest Time", _format_duration(rest_time_s)),
            _stat_card("Ascent", f"{ascent_ft:,}", "ft"),
            _stat_card("Calories", calories),
            _stat_card("Avg HR", avg_hr, "bpm"),
            _stat_card("Max HR", max_hr, "bpm"),
        ],
    )

    # Route breakdown table
    if splits.is_empty():
        return [session_cards, html.Div("No route data available.", style={"color": COLORS["muted"]})]

    session_splits = splits.filter(pl.col("source_file") == source_file).sort("start_time")
    if session_splits.is_empty():
        return [session_cards]

    # Build route table - number the climbs
    rows = []
    climb_num = 0
    for s in session_splits.to_dicts():
        is_climb = s["split_type"] == "climb_active"
        if is_climb:
            climb_num += 1

        rows.append({
            "#": climb_num if is_climb else "",
            "Type": "Climb" if is_climb else "Rest",
            "Duration": _format_duration(s.get("total_timer_time")),
            "Ascent (ft)": FEET_PER_ROUTE if is_climb else "—",
            "Calories": int(s["total_calories"]) if s.get("total_calories") else "—",
        })

    table = dash_table.DataTable(
        data=rows,
        columns=[{"name": c, "id": c} for c in rows[0].keys()],
        style_header={
            "backgroundColor": COLORS["card"],
            "color": COLORS["text"],
            "fontWeight": "bold",
            "border": f"1px solid {COLORS['border']}",
        },
        style_cell={
            "backgroundColor": COLORS["bg"],
            "color": COLORS["text"],
            "border": f"1px solid {COLORS['border']}",
            "padding": "8px 12px",
            "textAlign": "center",
        },
        style_data_conditional=[
            {
                "if": {"filter_query": '{Type} = "Climb"'},
                "backgroundColor": COLORS["card"],
                "fontWeight": "bold",
            },
        ],
        style_table={"overflowX": "auto"},
    )

    return [session_cards, html.Div(table, style=CARD_STYLE)]
