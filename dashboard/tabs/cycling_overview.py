import plotly.graph_objects as go
from dash import Input, Output, callback, dcc, html
from plotly.subplots import make_subplots

from backend.cycling_processor import CyclingProcessor

from ..config import CARD_STYLE, COLORS


def _x_labels(df, group_by):
    if group_by == "year":
        return df["year"].cast(str).to_list()
    elif group_by == "month":
        return [f"{r['year']}-{r['month']:02d}" for r in df.to_dicts()]
    else:
        return df["week_starting"].to_list()


def cycling_overview_layout():
    return html.Div(
        [
            # Cycling summary section
            html.H3(
                "Cycling Summary",
                style={
                    "color": COLORS["accent"],
                    "marginBottom": "12px",
                    "fontSize": "0.95rem",
                },
            ),
            dcc.RadioItems(
                id="cycling-summary-group",
                options=[
                    {"label": "Annual", "value": "year"},
                    {"label": "Monthly", "value": "month"},
                    {"label": "Weekly", "value": "week"},
                ],
                value="month",
                inline=True,
                labelStyle={
                    "marginRight": "12px",
                    "cursor": "pointer",
                    "color": COLORS["text"],
                },
                inputStyle={"marginRight": "4px"},
                style={"marginBottom": "12px"},
            ),
            html.Div(dcc.Graph(id="cycling-summary-chart"), style=CARD_STYLE),
            # Training load section
            html.H3(
                "Training Load",
                style={
                    "color": COLORS["accent"],
                    "marginBottom": "12px",
                    "marginTop": "24px",
                    "fontSize": "0.95rem",
                },
            ),
            html.Div(
                style={"display": "flex", "gap": "8px", "marginBottom": "12px"},
                children=[
                    dcc.RadioItems(
                        id="date-range",
                        options=[
                            {"label": "3M", "value": "3"},
                            {"label": "6M", "value": "6"},
                            {"label": "1Y", "value": "12"},
                            {"label": "All", "value": "all"},
                        ],
                        value="12",
                        inline=True,
                        labelStyle={
                            "marginRight": "12px",
                            "cursor": "pointer",
                            "color": COLORS["text"],
                        },
                        inputStyle={"marginRight": "4px"},
                    ),
                    dcc.Checklist(
                        id="show-forecast",
                        options=[{"label": " Show Forecast", "value": "yes"}],
                        value=["yes"],
                        style={"marginLeft": "24px"},
                        labelStyle={"color": COLORS["text"]},
                        inputStyle={"marginRight": "4px"},
                    ),
                ],
            ),
            html.Div(dcc.Graph(id="training-load-chart"), style=CARD_STYLE),
        ]
    )


@callback(
    Output("cycling-summary-chart", "figure"),
    Input("cycling-summary-group", "value"),
)
def update_cycling_summary(group_by):
    cp = CyclingProcessor()
    df = cp.summarize_cycling(group_by=group_by)

    if df.is_empty():
        fig = go.Figure()
        fig.update_layout(
            paper_bgcolor=COLORS["card"],
            plot_bgcolor=COLORS["card"],
            font_color=COLORS["text"],
            annotations=[
                {
                    "text": "No cycling data",
                    "showarrow": False,
                    "font": {"size": 14, "color": COLORS["muted"]},
                }
            ],
        )
        return fig

    x = _x_labels(df, group_by)
    miles = df["miles"].to_list()
    hours = df["hours"].to_list()
    tss = df["tss"].to_list()
    rides = df["rides"].to_list()

    fig = make_subplots(
        rows=3,
        cols=1,
        vertical_spacing=0.12,
        subplot_titles=["Miles", "Hours", "TSS"],
    )

    fig.add_trace(
        go.Bar(
            x=x,
            y=miles,
            name="Miles",
            marker_color="#2196F3",
            text=miles,
            textposition="outside",
            textfont_size=10,
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Bar(
            x=x,
            y=hours,
            name="Hours",
            marker_color="#4CAF50",
            text=hours,
            textposition="outside",
            textfont_size=10,
        ),
        row=2,
        col=1,
    )
    fig.add_trace(
        go.Bar(
            x=x,
            y=tss,
            name="TSS",
            marker_color="#FF9800",
            text=tss,
            textposition="outside",
            textfont_size=10,
        ),
        row=3,
        col=1,
    )

    fig.update_layout(
        paper_bgcolor=COLORS["card"],
        plot_bgcolor=COLORS["card"],
        font_color=COLORS["text"],
        showlegend=False,
        height=900,
        margin=dict(t=40, b=60, l=60, r=30),
    )

    # Set y-axis range with 30% headroom so "outside" labels don't collide
    for i, series in enumerate([miles, hours, tss], start=1):
        max_val = max(series) if series else 1
        fig.update_xaxes(gridcolor=COLORS["border"], row=i, col=1)
        fig.update_yaxes(
            gridcolor=COLORS["border"],
            automargin=True,
            range=[0, max_val * 1.3],
            row=i,
            col=1,
        )

    # Style subplot titles
    for ann in fig.layout.annotations:
        ann.font.color = COLORS["muted"]
        ann.font.size = 12

    return fig


@callback(
    Output("training-load-chart", "figure"),
    Input("date-range", "value"),
    Input("show-forecast", "value"),
)
def update_training_load(date_range, show_forecast):
    start_date = None
    if date_range != "all":
        from datetime import date, timedelta

        months = int(date_range)
        d = date.today()
        start_date = (d.replace(day=1) - timedelta(days=months * 30)).isoformat()

    cp = CyclingProcessor()
    fig = cp.plot_training_load(
        start_date=start_date, include_forecast="yes" in (show_forecast or [])
    )

    fig.update_layout(
        paper_bgcolor=COLORS["card"],
        plot_bgcolor=COLORS["card"],
        font_color=COLORS["text"],
        xaxis=dict(gridcolor=COLORS["border"]),
        yaxis=dict(gridcolor=COLORS["border"]),
        height=550,
    )
    return fig
