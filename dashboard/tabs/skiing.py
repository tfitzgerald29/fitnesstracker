from dash import Input, Output, callback, dash_table, html

from backend.skiing_processor import skiing
from ..config import CARD_STYLE, COLORS, MERGED_PATH


def skiing_tab():
    return html.Div(
        [
            html.H3(
                "Ski Run Summary",
                style={
                    "color": COLORS["accent"],
                    "marginBottom": "12px",
                    "fontSize": "0.95rem",
                },
            ),
            html.Div(id="skiing-table", style=CARD_STYLE),
        ]
    )


@callback(
    Output("skiing-table", "children"),
    Input("tabs", "value"),
)
def update_skiing_table(tab):
    if tab != "Ski":
        return []

    sp = skiing(mergedfiles_path=MERGED_PATH)
    df = sp.run_summary()

    if df.is_empty():
        return html.Div("No skiing data found.", style={"color": COLORS["muted"]})

    rows = df.sort("DT_DENVER", descending=True).to_dicts()
    for r in rows:
        if r.get("DT_DENVER") is not None:
            r["DT_DENVER"] = str(r["DT_DENVER"])
        if r.get("TOTAL_DESCENT") is not None:
            r["TOTAL_DESCENT"] = f"{r['TOTAL_DESCENT']:,.0f}"

    columns = [
        {"name": "Date", "id": "DT_DENVER"},
        {"name": "Runs", "id": "NUMBER_OF_RUNS"},
        {"name": "Total Descent (ft)", "id": "TOTAL_DESCENT"},
        {"name": "Elapsed Time", "id": "ELAPSED_TIME"},
        {"name": "Moving Time", "id": "total_moving_time"},
    ]

    return dash_table.DataTable(
        data=rows,
        columns=columns,
        style_header={
            "backgroundColor": COLORS["card"],
            "color": COLORS["text"],
            "fontWeight": "bold",
            "border": f"1px solid {COLORS['border']}",
        },
        style_cell={
            "backgroundColor": COLORS["bg"],
            "color": COLORS["text"],
            "border": f"1px solid {COLORS['border']}",
            "padding": "8px 12px",
            "textAlign": "center",
        },
        style_table={"overflowX": "auto"},
        sort_action="native",
    )
