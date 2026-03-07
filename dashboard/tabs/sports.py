from dash import Input, Output, callback, dash_table, html

from backend.SportSummarizer import SportSummarizer

from ..config import CARD_STYLE, COLORS, MERGED_PATH

from dash import dcc


def sports_tab():
    return html.Div([
        dcc.RadioItems(
            id="sport-group-by",
            options=[
                {"label": "Annual", "value": "year"},
                {"label": "Monthly", "value": "month"},
                {"label": "Weekly", "value": "week"},
            ],
            value="year",
            inline=True,
            labelStyle={"marginRight": "12px", "cursor": "pointer"},
            inputStyle={"marginRight": "4px"},
            style={"marginBottom": "12px"},
        ),
        html.Div(id="sport-summary-table", style=CARD_STYLE),
    ])


@callback(Output("sport-summary-table", "children"), Input("sport-group-by", "value"))
def update_sport_summary(group_by):
    ss = SportSummarizer(mergedfiles_path=MERGED_PATH)
    df = ss.summarize_hours_by_sport(group_by=group_by)

    if df is None or df.is_empty():
        return html.P("No data available.", style={"color": COLORS["muted"], "padding": "20px"})

    records = df.to_pandas().sort_index(ascending=False).to_dict("records")
    columns = [{"name": c.replace("_", " ").title(), "id": c} for c in df.columns]

    return dash_table.DataTable(
        data=records,
        columns=columns,
        style_header={
            "backgroundColor": COLORS["card"],
            "color": COLORS["muted"],
            "fontWeight": "500",
            "borderBottom": f"1px solid {COLORS['border']}",
            "textTransform": "capitalize",
        },
        style_cell={
            "backgroundColor": COLORS["card"],
            "color": COLORS["text"],
            "border": f"1px solid {COLORS['border']}",
            "padding": "8px 12px",
            "fontSize": "0.85rem",
            "fontFamily": "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif",
        },
        style_data_conditional=[
            {"if": {"row_index": "odd"}, "backgroundColor": "#1e2130"},
        ],
        page_size=20,
    )
