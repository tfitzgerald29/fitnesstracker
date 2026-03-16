import json

from dash import html

from backend.SportSummarizer import SportSummarizer
from ..config import CARD_STYLE, COLORS, MERGED_PATH

# Sport → display color — kept here for the legend rendering
SPORT_COLORS = {
    "cycling": "#2196F3",
    "weight_lifting": "#FF9800",
    "rock_climbing": "#4CAF50",
    "running": "#E91E63",
    "hiking": "#8BC34A",
    "alpine_skiing": "#00BCD4",
}


def _load_events():
    """Load all activities and return (events_list, raw_rows_for_totals)."""
    return SportSummarizer(MERGED_PATH).get_calendar_events()


def calendar_tab():
    events, raw = _load_events()

    # Find the latest event date so the calendar opens to the right month
    latest_date = max((e["start"] for e in events), default=None) if events else None

    return html.Div(
        [
            # Data for JS to read
            html.Script(
                id="fc-events-data",
                type="application/json",
                children=json.dumps(events),
            ),
            html.Script(
                id="fc-raw-data", type="application/json", children=json.dumps(raw)
            ),
            html.Script(
                id="fc-initial-date",
                type="application/json",
                children=json.dumps(latest_date),
            ),
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
                            html.Div(
                                style={
                                    "width": "12px",
                                    "height": "12px",
                                    "borderRadius": "2px",
                                    "backgroundColor": color,
                                }
                            ),
                            html.Span(
                                label,
                                style={
                                    "fontSize": "0.8rem",
                                    "color": COLORS["muted"],
                                },
                            ),
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
        ]
    )
