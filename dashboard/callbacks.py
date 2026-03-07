from dash import Input, Output, callback, html

from .tabs import cycling_tab, sports_tab, weights_tab


@callback(Output("tab-content", "children"), Input("tabs", "value"))
def render_tab(tab):
    if tab == "cycling":
        return cycling_tab()
    elif tab == "sports":
        return sports_tab()
    elif tab == "weights":
        return weights_tab()
