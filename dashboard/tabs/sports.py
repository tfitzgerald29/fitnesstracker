import plotly.graph_objects as go
from dash import Input, Output, callback, dash_table, dcc, html
from plotly.subplots import make_subplots

from backend.SportSummarizer import SportSummarizer

from ..config import CARD_STYLE, COLORS, MERGED_PATH


def _stat_card(label, value, sub=""):
    children = [
        html.Div(
            str(value),
            style={
                "fontSize": "1.5rem",
                "fontWeight": "bold",
                "color": COLORS["accent"],
            },
        ),
        html.Div(
            label,
            style={"fontSize": "0.8rem", "color": COLORS["muted"], "marginTop": "4px"},
        ),
    ]
    if sub:
        children.append(
            html.Div(
                sub,
                style={"fontSize": "0.7rem", "color": COLORS["muted"], "marginTop": "2px"},
            )
        )
    return html.Div(
        style={
            **CARD_STYLE,
            "display": "inline-block",
            "textAlign": "center",
            "padding": "16px 28px",
            "minWidth": "140px",
        },
        children=children,
    )


def sports_tab():
    return html.Div(
        [
            # Summary cards
            html.Div(id="sport-summary-cards"),
            # Chart + table controls
            html.H3(
                "Activity Hours",
                style={
                    "color": COLORS["accent"],
                    "marginBottom": "12px",
                    "marginTop": "24px",
                    "fontSize": "0.95rem",
                },
            ),
            dcc.RadioItems(
                id="sport-group-by",
                options=[
                    {"label": "Annual", "value": "year"},
                    {"label": "Monthly", "value": "month"},
                    {"label": "Weekly", "value": "week"},
                ],
                value="year",
                inline=True,
                labelStyle={
                    "marginRight": "12px",
                    "cursor": "pointer",
                    "color": COLORS["text"],
                },
                inputStyle={"marginRight": "4px"},
                style={"marginBottom": "12px"},
            ),
            # Total hours chart
            html.Div(dcc.Graph(id="sport-total-chart"), style=CARD_STYLE),
            # Per-sport small multiples chart
            html.Div(dcc.Graph(id="sport-chart"), style=CARD_STYLE),
            # Table
            html.H3(
                "Detail",
                style={
                    "color": COLORS["accent"],
                    "marginBottom": "12px",
                    "marginTop": "24px",
                    "fontSize": "0.95rem",
                },
            ),
            html.Div(id="sport-summary-table", style=CARD_STYLE),
        ]
    )


@callback(
    Output("sport-summary-cards", "children"),
    Input("sport-group-by", "value"),  # trigger on load
)
def update_summary_cards(_group_by):
    ss = SportSummarizer(mergedfiles_path=MERGED_PATH)
    stats = ss.get_summary_stats()

    cards = [
        _stat_card("Hours YTD", stats["total_hours_ytd"]),
        _stat_card("Activities YTD", stats["total_activities_ytd"]),
    ]

    for s in stats["sports"]:
        name = s["sport"].replace("_", " ").title()
        cards.append(
            _stat_card(
                name,
                f"{s['hours']} hr",
                sub=f"{s['hours_per_week']} hr/wk  |  {s['activities']} activities",
            )
        )

    return html.Div(
        style={"display": "flex", "flexWrap": "wrap", "gap": "8px"},
        children=cards,
    )


@callback(
    Output("sport-total-chart", "figure"),
    Input("sport-group-by", "value"),
)
def update_total_chart(group_by):
    ss = SportSummarizer(mergedfiles_path=MERGED_PATH)
    data = ss.get_chart_data(group_by=group_by)

    if not data:
        fig = go.Figure()
        fig.update_layout(
            paper_bgcolor=COLORS["card"],
            plot_bgcolor=COLORS["card"],
            font_color=COLORS["text"],
        )
        return fig

    # Sum hours across all sports per period
    totals = {}
    for row in data:
        label = row["label"]
        totals[label] = totals.get(label, 0) + row["hours"]

    labels = sorted(totals.keys())
    hours = [round(totals[l], 1) for l in labels]

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=labels,
            y=hours,
            marker_color="rgba(255,255,255,0.6)",
            text=[f"{h}" for h in hours],
            textposition="outside",
            textfont_size=9,
            hovertemplate="%{x}: %{y} hr<extra></extra>",
        )
    )

    max_val = max(hours) if hours else 1
    fig.update_layout(
        paper_bgcolor=COLORS["card"],
        plot_bgcolor=COLORS["card"],
        font_color=COLORS["text"],
        xaxis=dict(gridcolor=COLORS["border"]),
        yaxis=dict(gridcolor=COLORS["border"], title="Hours", range=[0, max_val * 1.3]),
        height=300,
        margin=dict(t=30, b=40, l=60, r=30),
        showlegend=False,
        title=dict(text="Total Hours", font=dict(size=12, color=COLORS["muted"])),
    )

    return fig


@callback(
    Output("sport-chart", "figure"),
    Input("sport-group-by", "value"),
)
def update_sport_chart(group_by):
    ss = SportSummarizer(mergedfiles_path=MERGED_PATH)
    data = ss.get_chart_data(group_by=group_by)

    if not data:
        fig = go.Figure()
        fig.update_layout(
            paper_bgcolor=COLORS["card"],
            plot_bgcolor=COLORS["card"],
            font_color=COLORS["text"],
        )
        return fig

    # Group by sport
    sports = {}
    for row in data:
        sport = row["sport"]
        if sport not in sports:
            sports[sport] = {"labels": [], "hours": [], "activities": []}
        sports[sport]["labels"].append(row["label"])
        sports[sport]["hours"].append(row["hours"])
        sports[sport]["activities"].append(row["activities"])

    # Preferred order
    preferred = ["cycling", "weight_lifting", "rock_climbing"]
    ordered = [s for s in preferred if s in sports]
    ordered += sorted(s for s in sports if s not in ordered)

    n_sports = len(ordered)
    fig = make_subplots(
        rows=n_sports,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.06,
        subplot_titles=[s.replace("_", " ").title() for s in ordered],
    )

    for i, sport in enumerate(ordered, 1):
        s = sports[sport]
        fig.add_trace(
            go.Bar(
                x=s["labels"],
                y=s["hours"],
                name=sport.replace("_", " ").title(),
                marker_color="rgba(255,255,255,0.6)",
                text=[f"{h}" for h in s["hours"]],
                textposition="outside",
                textfont_size=9,
                hovertemplate="%{x}: %{y} hr<extra></extra>",
                showlegend=False,
            ),
            row=i,
            col=1,
        )

        # Set y range with headroom for outside labels
        max_val = max(s["hours"]) if s["hours"] else 1
        fig.update_yaxes(
            range=[0, max_val * 1.35],
            gridcolor=COLORS["border"],
            title_text="Hours",
            row=i,
            col=1,
        )
        fig.update_xaxes(gridcolor=COLORS["border"], row=i, col=1)

    fig.update_layout(
        paper_bgcolor=COLORS["card"],
        plot_bgcolor=COLORS["card"],
        font_color=COLORS["text"],
        height=max(250 * n_sports, 300),
        margin=dict(t=30, b=40, l=60, r=30),
        showlegend=False,
    )

    # Style subplot titles
    for ann in fig.layout.annotations:
        ann.font.color = COLORS["muted"]
        ann.font.size = 11

    return fig


@callback(Output("sport-summary-table", "children"), Input("sport-group-by", "value"))
def update_sport_summary(group_by):
    ss = SportSummarizer(mergedfiles_path=MERGED_PATH)
    df = ss.summarize_hours_by_sport(group_by=group_by)

    if df is None or df.is_empty():
        return html.P(
            "No data available.",
            style={"color": COLORS["muted"], "padding": "20px"},
        )

    records = df.reverse().to_dicts()
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
