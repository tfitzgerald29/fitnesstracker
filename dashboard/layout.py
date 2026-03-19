from dash import dcc, html

from .config import COLORS


TAB_STYLE = {
    "padding": "6px 20px",
    "lineHeight": "28px",
    "display": "flex",
    "alignItems": "center",
    "justifyContent": "center",
    "flex": "0 0 auto",
}


def _tab(label: str, value: str, selected_border: str) -> dcc.Tab:
    style = {**TAB_STYLE}
    if " " in label:
        style["whiteSpace"] = "nowrap"

    selected_style = {**style, "borderTop": f"2px solid {selected_border}"}

    return dcc.Tab(
        label=label,
        value=value,
        style=style,
        selected_style=selected_style,
    )


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
                            children=[
                                _tab("Calendar", "calendar", COLORS["accent"]),
                                _tab("Sport Summary", "sports", COLORS["accent"]),
                                _tab("Cycling", "cycling", COLORS["accent"]),
                                _tab("Weight Training", "weights", COLORS["accent"]),
                                _tab("Rock Climbing", "climbing", COLORS["accent"]),
                                _tab("Skiing", "Ski", COLORS["accent"]),
                                _tab("Hiking", "hiking", "#8BC34A"),
                                _tab("Running", "running", "#E91E63"),
                                _tab("Pickleball", "pickleball", "#AB47BC"),
                                _tab("Sleep", "sleep", "#00BCD4"),
                            ],
                            style={
                                "height": "40px",
                                "display": "flex",
                                "alignItems": "center",
                                "overflowX": "auto",
                                "overflowY": "hidden",
                                "whiteSpace": "nowrap",
                                "WebkitOverflowScrolling": "touch",
                                "scrollbarWidth": "thin",
                            },
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
