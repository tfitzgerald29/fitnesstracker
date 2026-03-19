from dash import dcc


TAB_ITEM_STYLE = {
    "padding": "6px 12px",
    "lineHeight": "28px",
    "fontSize": "0.85rem",
    "display": "flex",
    "alignItems": "center",
    "justifyContent": "center",
    "flex": "0 0 auto",
    "whiteSpace": "nowrap",
}

SCROLLABLE_TABS_STYLE = {
    "display": "flex",
    "alignItems": "center",
    "justifyContent": "flex-start",
    "overflowX": "auto",
    "overflowY": "hidden",
    "whiteSpace": "nowrap",
    "WebkitOverflowScrolling": "touch",
    "scrollbarWidth": "thin",
    "width": "100%",
}


def make_tab(label: str, value: str, selected_border: str) -> dcc.Tab:
    selected_style = {**TAB_ITEM_STYLE, "borderTop": f"2px solid {selected_border}"}
    return dcc.Tab(
        label=label,
        value=value,
        style=TAB_ITEM_STYLE,
        selected_style=selected_style,
    )
