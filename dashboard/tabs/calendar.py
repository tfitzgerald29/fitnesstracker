import json
import os

import polars as pl
from dash import html

from ..config import CARD_STYLE, COLORS, MERGED_PATH, WT_DATA_FILE

SPORT_COLORS = {
    "cycling": "#2196F3",
    "training": "#FF9800",
    "weight_lifting": "#FF9800",
    "rock_climbing": "#4CAF50",
    "running": "#E91E63",
    "hiking": "#8BC34A",
    "alpine_skiing": "#00BCD4",
    "generic": "#9E9E9E",
    "racket": "#AB47BC",
}

SPORT_LABELS = {
    "cycling": "Cycling",
    "training": "Lifting",
    "weight_lifting": "Lifting",
    "rock_climbing": "Rock Climbing",
    "running": "Running",
    "hiking": "Hiking",
    "alpine_skiing": "Skiing",
    "generic": "Other",
    "racket": "Racket",
}

NORMALIZE = {"training": "weight_lifting"}


def _fmt_duration(seconds):
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    if h > 0:
        return f"{h}h {m}m"
    return f"{m}m"


def _load_events():
    """Load all activities and return (events_list, raw_rows_for_totals)."""
    events = []
    raw = []

    # Build a lookup of JSON lifting data keyed by date
    wt_by_date = {}
    if os.path.exists(WT_DATA_FILE):
        with open(WT_DATA_FILE) as f:
            wt_data = json.load(f)
        for entry in wt_data:
            wt_by_date[entry["date"]] = len(entry.get("exercises", []))

    # Track which dates already have a Garmin "training" session
    garmin_lifting_dates = set()

    parquet_path = os.path.join(MERGED_PATH, "session_mesgs.parquet")
    if os.path.exists(parquet_path):
        df = pl.read_parquet(
            parquet_path,
            columns=["sport", "timestamp", "total_timer_time", "total_distance"],
        )
        if df["timestamp"].dtype.time_zone is None:
            df = df.with_columns(pl.col("timestamp").dt.replace_time_zone("UTC"))
        df = df.with_columns(
            pl.col("timestamp").dt.convert_time_zone("America/Denver")
        )

        for row in df.to_dicts():
            sport = row["sport"] or "generic"
            label = SPORT_LABELS.get(sport, sport.replace("_", " ").title())
            seconds = row["total_timer_time"] or 0
            duration = _fmt_duration(seconds)
            dist = row.get("total_distance") or 0
            miles = dist / 1609.344 if dist > 0 else 0

            dt = row["timestamp"]
            date_str = dt.strftime("%Y-%m-%d")

            if sport == "training":
                garmin_lifting_dates.add(date_str)
                n_ex = wt_by_date.get(date_str)
                if n_ex:
                    title = f"Lifting ({duration}, {n_ex} exercises)"
                else:
                    title = f"Lifting ({duration})"
                events.append({
                    "title": title,
                    "start": date_str,
                    "allDay": True,
                    "color": SPORT_COLORS["weight_lifting"],
                })
            elif sport == "cycling" and miles > 1:
                title = f"{label} ({duration}, {miles:.1f}mi)"
                events.append({
                    "title": title,
                    "start": date_str,
                    "allDay": True,
                    "color": SPORT_COLORS.get(sport, "#9E9E9E"),
                })
            else:
                title = f"{label} ({duration})"
                events.append({
                    "title": title,
                    "start": date_str,
                    "allDay": True,
                    "color": SPORT_COLORS.get(sport, "#9E9E9E"),
                })

            raw.append({
                "sport": NORMALIZE.get(sport, sport),
                "date": date_str,
                "seconds": seconds,
                "miles": miles,
            })

    # Add JSON-only lifting entries (no matching Garmin session)
    for dt_str, n_ex in wt_by_date.items():
        if dt_str not in garmin_lifting_dates:
            events.append({
                "title": f"Lifting ({n_ex} exercises)",
                "start": dt_str,
                "allDay": True,
                "color": SPORT_COLORS["weight_lifting"],
            })
            raw.append({
                "sport": "weight_lifting",
                "date": dt_str,
                "seconds": 0,
                "miles": 0,
            })

    return events, raw


def calendar_tab():
    events, raw = _load_events()

    return html.Div([
        # Data for JS to read
        html.Script(id="fc-events-data", type="application/json", children=json.dumps(events)),
        html.Script(id="fc-raw-data", type="application/json", children=json.dumps(raw)),
        html.Div(
            style={**CARD_STYLE},
            children=[
                html.Div(id="fc-container"),
            ],
        ),
        # Legend
        html.Div(
            style={
                "display": "flex",
                "gap": "16px",
                "flexWrap": "wrap",
                "marginTop": "8px",
            },
            children=[
                html.Div(
                    style={"display": "flex", "alignItems": "center", "gap": "6px"},
                    children=[
                        html.Div(style={
                            "width": "12px",
                            "height": "12px",
                            "borderRadius": "2px",
                            "backgroundColor": color,
                        }),
                        html.Span(label, style={
                            "fontSize": "0.8rem",
                            "color": COLORS["muted"],
                        }),
                    ],
                )
                for label, color in [
                    ("Cycling", SPORT_COLORS["cycling"]),
                    ("Lifting", SPORT_COLORS["weight_lifting"]),
                    ("Rock Climbing", SPORT_COLORS["rock_climbing"]),
                    ("Running", SPORT_COLORS["running"]),
                    ("Hiking", SPORT_COLORS["hiking"]),
                    ("Skiing", SPORT_COLORS["alpine_skiing"]),
                ]
            ],
        ),
    ])
