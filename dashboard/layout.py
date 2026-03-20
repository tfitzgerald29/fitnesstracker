from dash import dcc, html

from .config import COLORS
from .tab_ui import SCROLLABLE_TABS_STYLE, make_tab


def create_layout():
    return html.Div(
        style={
            "backgroundColor": COLORS["bg"],
            "minHeight": "100vh",
            "color": COLORS["text"],
        },
        children=[
            html.Div(
                style={
                    "backgroundColor": COLORS["card"],
                    "borderBottom": f"1px solid {COLORS['border']}",
                    "padding": "16px 24px",
                    "display": "flex",
                    "alignItems": "center",
                    "gap": "32px",
                    "flexWrap": "wrap",
                    "rowGap": "12px",
                },
                children=[
                    html.H1(
                        "Tyler's Activities",
                        style={
                            "fontSize": "1.4rem",
                            "fontWeight": "600",
                            "color": "#fff",
                            "margin": 0,
                            "flex": "0 0 auto",
                        },
                    ),
                    html.Div(
                        style={
                            "flex": "1 1 780px",
                            "minWidth": "0",
                        },
                        children=dcc.Tabs(
                            id="tabs",
                            value="calendar",
                            mobile_breakpoint=0,
                            children=[
                                make_tab("Calendar", "calendar", COLORS["accent"]),
                                make_tab("Sleep", "sleep", "#00BCD4"),
                                make_tab("Sport Summary", "sports", COLORS["accent"]),
                                make_tab("Cycling", "cycling", COLORS["accent"]),
                                make_tab(
                                    "Weight Training", "weights", COLORS["accent"]
                                ),
                                make_tab("Rock Climbing", "climbing", COLORS["accent"]),
                                make_tab("Skiing", "Ski", COLORS["accent"]),
                                make_tab("Hiking", "hiking", "#8BC34A"),
                                make_tab("Running", "running", "#E91E63"),
                                make_tab("Pickleball", "pickleball", "#AB47BC"),
                            ],
                            style={**SCROLLABLE_TABS_STYLE},
                            colors={
                                "border": "transparent",
                                "primary": COLORS["accent"],
                                "background": "transparent",
                            },
                        ),
                    ),
                ],
            ),
            html.Div(
                id="tab-content",
                style={"padding": "24px", "maxWidth": "1400px", "margin": "0 auto"},
            ),
        ],
    )
