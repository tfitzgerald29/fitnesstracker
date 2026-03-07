from dash import dcc, html

from .config import COLORS


def create_layout():
    return html.Div(
        style={"backgroundColor": COLORS["bg"], "minHeight": "100vh", "color": COLORS["text"]},
        children=[
            html.Div(
                style={
                    "backgroundColor": COLORS["card"],
                    "borderBottom": f"1px solid {COLORS['border']}",
                    "padding": "16px 24px",
                    "display": "flex",
                    "alignItems": "center",
                    "gap": "32px",
                },
                children=[
                    html.H1(
                        "Tyler's Activities",
                        style={"fontSize": "1.4rem", "fontWeight": "600", "color": "#fff", "margin": 0},
                    ),
                    dcc.Tabs(
                        id="tabs",
                        value="sports",
                        children=[
                            dcc.Tab(
                                label="Sport Summary", value="sports",
                                style={"padding": "6px 16px", "lineHeight": "28px"},
                                selected_style={"padding": "6px 16px", "lineHeight": "28px", "borderTop": f"2px solid {COLORS['accent']}"},
                            ),
                            dcc.Tab(
                                label="Cycling", value="cycling",
                                style={"padding": "6px 16px", "lineHeight": "28px"},
                                selected_style={"padding": "6px 16px", "lineHeight": "28px", "borderTop": f"2px solid {COLORS['accent']}"},
                            ),
                            dcc.Tab(
                                label="Weight Training", value="weights",
                                style={"padding": "6px 16px", "lineHeight": "28px"},
                                selected_style={"padding": "6px 16px", "lineHeight": "28px", "borderTop": f"2px solid {COLORS['accent']}"},
                            ),
                        ],
                        style={"height": "40px", "display": "flex", "alignItems": "center"},
                        colors={
                            "border": "transparent",
                            "primary": COLORS["accent"],
                            "background": "transparent",
                        },
                    ),
                ],
            ),
            html.Div(
                id="tab-content",
                style={"padding": "24px", "maxWidth": "1400px", "margin": "0 auto"},
            ),
        ],
    )
