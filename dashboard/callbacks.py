from dash import Input, Output, callback, html

from .tabs import (
    calendar_tab,
    climbing_tab,
    cycling_tab,
    hiking_tab,
    pickleball_tab,
    running_tab,
    skiing_tab,
    sports_tab,
    weights_tab,
)


@callback(Output("tab-content", "children"), Input("tabs", "value"))
def render_tab(tab):
    if tab == "calendar":
        return calendar_tab()
    elif tab == "cycling":
        return cycling_tab()
    elif tab == "climbing":
        return climbing_tab()
    elif tab == "hiking":
        return hiking_tab()
    elif tab == "running":
        return running_tab()
    elif tab == "pickleball":
        return pickleball_tab()
    elif tab == "sports":
        return sports_tab()
    elif tab == "weights":
        return weights_tab()
    elif tab == "Ski":
        return skiing_tab()
