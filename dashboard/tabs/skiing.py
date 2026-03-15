from dash import Input, Output, callback, dash_table, html

from backend.skiing_processor import skiing
from ..config import CARD_STYLE, COLORS, MERGED_PATH


def _fmt_time(seconds):
    if seconds is None:
        return ""
    s = int(seconds)
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    if h:
        return f"{h}h {m}m {sec}s"
    return f"{m}m {sec}s"


def skiing_tab():
    return html.Div(
        [
            html.H3(
                "Annual Ski Summary",
                style={
                    "color": COLORS["accent"],
                    "marginBottom": "12px",
                    "fontSize": "0.95rem",
                },
            ),
            html.Div(id="skiing-annual-table", style=CARD_STYLE),
            html.H3(
                "Ski Day Summary",
                style={
                    "color": COLORS["accent"],
                    "marginBottom": "12px",
                    "marginTop": "24px",
                    "fontSize": "0.95rem",
                },
            ),
            html.Div(id="skiing-table", style=CARD_STYLE),
        ]
    )


@callback(
    Output("skiing-annual-table", "children"),
    Input("tabs", "value"),
)
def update_skiing_annual_table(tab):
    if tab != "Ski":
        return []

    sp = skiing(mergedfiles_path=MERGED_PATH)
    df = sp.annual_summary()

    if df.is_empty():
        return html.Div("No skiing data found.", style={"color": COLORS["muted"]})

    display_rows = []
    for r in df.to_dicts():
        row = {
            "Season": r.get("season", ""),
            "First Day": str(r["first_day"]) if r.get("first_day") else "",
            "Last Day": str(r["last_day"]) if r.get("last_day") else "",
            "Total Days": r.get("total_days", ""),
            "Ski Days": r.get("ski_days", ""),
            "BC Days": r.get("bc_days", ""),
            "Laps": int(r.get("num_laps") or 0),
            "Distance (mi)": round(r["total_distance"] / 1609.344, 1)
            if r.get("total_distance")
            else "",
            "Descent (ft)": f"{int(r['total_descent'] * 3.28084):,}"
            if r.get("total_descent")
            else "",
            "Elapsed": _fmt_time(r.get("total_elapsed_time")),
            "Moving": _fmt_time(r.get("total_moving_time")),
            "Max Speed": round(r["max_speed"] * 2.23694, 1)
            if r.get("max_speed")
            else "",
            "Avg HR": int(r["avg_heart_rate"]) if r.get("avg_heart_rate") else "",
            "Max HR": int(r["max_heart_rate"]) if r.get("max_heart_rate") else "",
        }
        display_rows.append(row)

    columns = [{"name": k, "id": k} for k in display_rows[0]]

    return dash_table.DataTable(
        data=display_rows,
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

    display_rows = []
    for r in df.to_dicts():
        row = {
            "Date": str(r.get("DT_DENVER", "")),
            "Laps": int(r.get("num_laps") or 0),
            "Avg HR": int(r["avg_heart_rate"]) if r.get("avg_heart_rate") else "",
            "Max HR": int(r["max_heart_rate"]) if r.get("max_heart_rate") else "",
            "Elapsed": _fmt_time(r.get("total_elapsed_time")),
            "Moving": _fmt_time(r.get("total_moving_time")),
            "Distance (mi)": round(r["total_distance"] / 1609.344, 1)
            if r.get("total_distance")
            else "",
            "Avg Speed": round(r["enhanced_avg_speed"] * 2.23694, 1)
            if r.get("enhanced_avg_speed")
            else "",
            "Max Speed": round(r["enhanced_max_speed"] * 2.23694, 1)
            if r.get("enhanced_max_speed")
            else "",
            "Ascent (ft)": f"{int(r['total_ascent'] * 3.28084):,}"
            if r.get("total_ascent")
            else "",
            "Descent (ft)": f"{int(r['total_descent'] * 3.28084):,}"
            if r.get("total_descent")
            else "",
        }
        display_rows.append(row)

    columns = [{"name": k, "id": k} for k in display_rows[0]]

    return dash_table.DataTable(
        data=display_rows,
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
