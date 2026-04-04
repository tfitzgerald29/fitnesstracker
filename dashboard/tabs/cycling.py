from dash import Input, Output, callback, dcc, html

from ..config import COLORS
from ..tab_ui import SCROLLABLE_TABS_STYLE, make_tab

from .cycling_cp import cycling_cp_layout  # noqa: F401 (registers callbacks)
from .cycling_covariate import cycling_covariate_layout  # noqa: F401 (registers callbacks)
from .cycling_overview import cycling_overview_layout  # noqa: F401
from .cycling_rides import cycling_rides_layout  # noqa: F401
from .cycling_training_load import cycling_training_load_layout  # noqa: F401


def cycling_tab():
    return html.Div(
        [
            dcc.Tabs(
                id="cycling-subtabs",
                value="overview",
                mobile_breakpoint=0,
                children=[
                    make_tab("Overview", "overview", COLORS["accent"]),
                    make_tab("Training Load", "training-load", COLORS["accent"]),
                    make_tab("Critical Power", "cp", COLORS["accent"]),
                    make_tab("Peak Power Analysis", "covariate", COLORS["accent"]),
                    make_tab("Rides", "rides", COLORS["accent"]),
                ],
                style={**SCROLLABLE_TABS_STYLE, "marginBottom": "16px"},
                colors={
                    "border": "transparent",
                    "primary": COLORS["accent"],
                    "background": "transparent",
                },
            ),
            html.Div(id="overview-content", children=cycling_overview_layout()),
            html.Div(
                id="training-load-content",
                children=cycling_training_load_layout(),
                style={"display": "none"},
            ),
            html.Div(
                id="cp-content",
                children=cycling_cp_layout(),
                style={"display": "none"},
            ),
            html.Div(
                id="rides-content",
                children=cycling_rides_layout(),
                style={"display": "none"},
            ),
            html.Div(
                id="covariate-content",
                children=cycling_covariate_layout(),
                style={"display": "none"},
            ),
        ]
    )


@callback(
    Output("overview-content", "style"),
    Output("training-load-content", "style"),
    Output("cp-content", "style"),
    Output("rides-content", "style"),
    Output("covariate-content", "style"),
    Input("cycling-subtabs", "value"),
)
def render_cycling_subtab(subtab):
    hide = {"display": "none"}
    show = {"display": "block"}

    return (
        show if subtab == "overview" else hide,
        show if subtab == "training-load" else hide,
        show if subtab == "cp" else hide,
        show if subtab == "rides" else hide,
        show if subtab == "covariate" else hide,
    )
