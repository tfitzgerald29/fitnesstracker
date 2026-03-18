from dash import Input, Output, State, callback, dcc, html

from ..config import COLORS, get_user_id

from .cycling_cp import cycling_cp_layout  # noqa: F401 (registers callbacks)
from .cycling_covariate import cycling_covariate_layout  # noqa: F401 (registers callbacks)
from .cycling_overview import cycling_overview_layout  # noqa: F401
from .cycling_rides import cycling_rides_layout  # noqa: F401


def cycling_tab():
    return html.Div(
        [
            dcc.Tabs(
                id="cycling-subtabs",
                value="overview",
                children=[
                    dcc.Tab(
                        label="Overview",
                        value="overview",
                        style={"padding": "6px 16px", "lineHeight": "28px"},
                        selected_style={
                            "padding": "6px 16px",
                            "lineHeight": "28px",
                            "borderTop": f"2px solid {COLORS['accent']}",
                        },
                    ),
                    dcc.Tab(
                        label="Critical Power",
                        value="cp",
                        style={"padding": "6px 16px", "lineHeight": "28px"},
                        selected_style={
                            "padding": "6px 16px",
                            "lineHeight": "28px",
                            "borderTop": f"2px solid {COLORS['accent']}",
                        },
                    ),
                    dcc.Tab(
                        label="Peak Power Analysis",
                        value="covariate",
                        style={"padding": "6px 16px", "lineHeight": "28px"},
                        selected_style={
                            "padding": "6px 16px",
                            "lineHeight": "28px",
                            "borderTop": f"2px solid {COLORS['accent']}",
                        },
                    ),
                    dcc.Tab(
                        label="Rides",
                        value="rides",
                        style={"padding": "6px 16px", "lineHeight": "28px"},
                        selected_style={
                            "padding": "6px 16px",
                            "lineHeight": "28px",
                            "borderTop": f"2px solid {COLORS['accent']}",
                        },
                    ),
                ],
                style={"height": "40px", "marginBottom": "16px"},
                colors={
                    "border": "transparent",
                    "primary": COLORS["accent"],
                    "background": "transparent",
                },
            ),
            html.Div(id="overview-content"),
            html.Div(id="cp-content"),
            html.Div(id="rides-content"),
            html.Div(id="covariate-content"),
        ]
    )


@callback(
    Output("overview-content", "children"),
    Output("overview-content", "style"),
    Output("cp-content", "children"),
    Output("cp-content", "style"),
    Output("rides-content", "children"),
    Output("rides-content", "style"),
    Output("covariate-content", "children"),
    Output("covariate-content", "style"),
    Input("cycling-subtabs", "value"),
    Input("user-store", "data"),
)
def render_cycling_subtab(subtab, user_data):
    hide = {"display": "none"}
    show = {"display": "block"}
    user_id = get_user_id(user_data)
    return (
        cycling_overview_layout() if subtab == "overview" else None,
        show if subtab == "overview" else hide,
        cycling_cp_layout() if subtab == "cp" else None,
        show if subtab == "cp" else hide,
        cycling_rides_layout(user_id=user_id) if subtab == "rides" else None,
        show if subtab == "rides" else hide,
        cycling_covariate_layout() if subtab == "covariate" else None,
        show if subtab == "covariate" else hide,
    )
