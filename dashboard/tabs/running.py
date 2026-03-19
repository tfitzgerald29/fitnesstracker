import polars as pl
from dash import Input, Output, callback, html

from backend.running_processor import RunningProcessor
from ..config import CARD_STYLE, COLORS, get_user_id

ACCENT = "#E91E63"  # running pink


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


# ── Layout ────────────────────────────────────────────────────────────────────


def running_tab():
    return html.Div(
        [
            html.Div(id="running-summary-cards"),
            html.H3(
                "Runs",
                style={
                    "color": ACCENT,
                    "marginBottom": "12px",
                    "marginTop": "24px",
                    "fontSize": "0.95rem",
                },
            ),
            html.Div(id="running-session-list"),
        ]
    )


# ── Callbacks ─────────────────────────────────────────────────────────────────


@callback(
    Output("running-summary-cards", "children"),
    Output("running-session-list", "children"),
    Input("tabs", "value"),
)
def update_running_overview(tab):
    if tab != "running":
        return [], []

    rp = RunningProcessor(user_id=get_user_id())

    if rp.running.is_empty():
        return [
            html.Div("No running data found.", style={"color": COLORS["muted"]})
        ], []

    stats = rp.summary_stats()
    cards = html.Div(
        style={
            "display": "flex",
            "gap": "12px",
            "flexWrap": "wrap",
            "marginBottom": "8px",
        },
        children=[
            _stat_card("Runs", stats.get("total_runs", 0)),
            _stat_card("Total Miles", stats.get("total_miles", 0)),
            _stat_card("Total Hours", stats.get("total_hours", 0)),
        ],
    )

    # Session list
    df = rp.running.sort("timestamp", descending=True)
    rows = []
    for r in df.to_dicts():
        dist_mi = (
            round(r["total_distance"] / 1609.344, 2) if r.get("total_distance") else "—"
        )
        pace_str = "—"
        if (
            r.get("total_distance")
            and r.get("total_timer_time")
            and r["total_distance"] > 0
        ):
            secs_per_mile = r["total_timer_time"] / (r["total_distance"] / 1609.344)
            pace_str = f"{int(secs_per_mile // 60)}:{int(secs_per_mile % 60):02d} /mi"
        rows.append(
            {
                "Date": r["timestamp"].strftime("%Y-%m-%d"),
                "Profile": r.get("sport_profile_name") or "—",
                "Distance (mi)": dist_mi,
                "Duration": _fmt_time(r.get("total_timer_time")),
                "Avg Pace": pace_str,
                "Avg HR": int(r["avg_heart_rate"]) if r.get("avg_heart_rate") else "—",
                "Calories": int(r["total_calories"])
                if r.get("total_calories")
                else "—",
            }
        )

    from dash import dash_table

    table = dash_table.DataTable(
        data=rows,
        columns=[{"name": k, "id": k} for k in rows[0].keys()],
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
        style_table={"overflowX": "auto"},
        sort_action="native",
    )

    return cards, html.Div(table, style=CARD_STYLE)
