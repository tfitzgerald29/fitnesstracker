import polars as pl
from dash import Input, Output, callback, dash_table, html

from backend.schemas import load_sessions
from backend.storage import storage
from ..config import CARD_STYLE, COLORS, get_user_id

ACCENT = "#AB47BC"  # racket / pickleball purple


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


def _load_pickleball(user_id=None) -> pl.DataFrame:
    """Load racket/pickleball sessions from the shared session parquet."""
    merged = storage.merged_path(user_id)
    parquet_path = storage.path_join(merged, "session_mesgs.parquet")
    if not storage.path_exists(parquet_path):
        return pl.DataFrame()
    # Pickleball lives under sport="racket", sub_sport="pickleball"
    df = pl.read_parquet(parquet_path).filter(
        (pl.col("sport") == "racket") & (pl.col("sub_sport") == "pickleball")
    )
    if df.is_empty():
        return df
    ts_col = "timestamp"
    if df[ts_col].dtype.time_zone is None:
        df = df.with_columns(pl.col(ts_col).dt.replace_time_zone("UTC"))
    return df.with_columns(
        pl.col(ts_col).dt.convert_time_zone("America/Denver").alias(ts_col)
    )


# ── Layout ────────────────────────────────────────────────────────────────────


def pickleball_tab():
    return html.Div(
        [
            html.Div(id="pickleball-summary-cards"),
            html.H3(
                "Sessions",
                style={
                    "color": ACCENT,
                    "marginBottom": "12px",
                    "marginTop": "24px",
                    "fontSize": "0.95rem",
                },
            ),
            html.Div(id="pickleball-session-list"),
        ]
    )


# ── Callbacks ─────────────────────────────────────────────────────────────────


@callback(
    Output("pickleball-summary-cards", "children"),
    Output("pickleball-session-list", "children"),
    Input("tabs", "value"),
)
def update_pickleball_overview(tab):
    if tab != "pickleball":
        return [], []

    df = _load_pickleball(user_id=get_user_id())

    if df.is_empty():
        return [
            html.Div("No pickleball data found.", style={"color": COLORS["muted"]})
        ], []

    total_sessions = len(df)
    total_hours = round(df["total_timer_time"].sum() / 3600, 1)
    total_calories = (
        int(df["total_calories"].drop_nulls().sum())
        if "total_calories" in df.columns
        else 0
    )
    avg_hr = (
        round(df["avg_heart_rate"].drop_nulls().mean())
        if "avg_heart_rate" in df.columns
        else None
    )

    cards = html.Div(
        style={
            "display": "flex",
            "gap": "12px",
            "flexWrap": "wrap",
            "marginBottom": "8px",
        },
        children=[
            _stat_card("Sessions", total_sessions),
            _stat_card("Total Hours", total_hours),
            _stat_card("Total Calories", f"{total_calories:,}"),
            _stat_card("Avg HR", int(avg_hr) if avg_hr else "—", "bpm"),
        ],
    )

    rows = []
    for r in df.sort("timestamp", descending=True).to_dicts():
        rows.append(
            {
                "Date": r["timestamp"].strftime("%Y-%m-%d"),
                "Duration": _fmt_time(r.get("total_timer_time")),
                "Avg HR": int(r["avg_heart_rate"]) if r.get("avg_heart_rate") else "—",
                "Max HR": int(r["max_heart_rate"]) if r.get("max_heart_rate") else "—",
                "Calories": int(r["total_calories"])
                if r.get("total_calories")
                else "—",
            }
        )

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
