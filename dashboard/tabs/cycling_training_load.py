from datetime import date, timedelta

import polars as pl
from dash import Input, Output, State, callback, ctx, dash_table, dcc, html

from backend.cycling_processor import CyclingProcessor

from ..config import CARD_STYLE, COLORS, get_user_id


def _forecast_input_rows(days: int = 21, blocks: int = 3) -> list[dict]:
    today = date.today()
    rows: list[dict] = []
    row_count = days // blocks
    for row_idx in range(row_count):
        row: dict[str, str | float | None] = {}
        for block_idx in range(1, blocks + 1):
            day_offset = row_idx + (block_idx - 1) * row_count + 1
            day_date = today + timedelta(days=day_offset)
            row[f"date_{block_idx}"] = day_date.isoformat()
            row[f"day_{block_idx}"] = day_date.strftime("%a")
            row[f"tss_{block_idx}"] = None
        rows.append(row)
    return rows


def _forecast_table_columns(blocks: int = 3) -> list[dict]:
    columns: list[dict] = []
    for block_idx in range(1, blocks + 1):
        columns.extend(
            [
                {
                    "name": f"Date {block_idx}",
                    "id": f"date_{block_idx}",
                    "editable": False,
                },
                {
                    "name": f"Day {block_idx}",
                    "id": f"day_{block_idx}",
                    "editable": False,
                },
                {
                    "name": f"TSS {block_idx}",
                    "id": f"tss_{block_idx}",
                    "type": "numeric",
                },
            ]
        )
    return columns


def _parse_tss_overrides(rows: list[dict] | None) -> dict[str, float]:
    if not rows:
        return {}

    overrides: dict[str, float] = {}
    for row in rows:
        for block_idx in range(1, 4):
            day = row.get(f"date_{block_idx}")
            tss_value = row.get(f"tss_{block_idx}")
            if not day or tss_value in (None, ""):
                continue
            try:
                overrides[str(day)] = float(tss_value)
            except (TypeError, ValueError):
                continue
    return overrides


def _clear_tss_inputs(rows: list[dict] | None) -> list[dict]:
    if not rows:
        return _forecast_input_rows()

    cleared_rows = []
    for row in rows:
        cleared_row = row.copy()
        for block_idx in range(1, 4):
            cleared_row[f"tss_{block_idx}"] = None
        cleared_rows.append(cleared_row)
    return cleared_rows


def _clear_forecast_button_style(has_data: bool) -> dict:
    return {
        **CARD_STYLE,
        "width": "fit-content",
        "padding": "8px 14px",
        "cursor": "pointer",
        "border": f"1px solid {COLORS['border']}",
        "backgroundColor": "#FFFFFF" if has_data else COLORS["bg"],
        "color": "#111111" if has_data else COLORS["muted"],
    }


def _apply_forecast_button_style(has_unsaved_changes: bool) -> dict:
    return {
        **CARD_STYLE,
        "width": "fit-content",
        "padding": "8px 14px",
        "cursor": "pointer",
        "border": f"1px solid {COLORS['border']}",
        "backgroundColor": "#FFFFFF" if has_unsaved_changes else COLORS["card"],
        "color": "#111111" if has_unsaved_changes else COLORS["text"],
    }


def _metric_value(
    df, day: date, column: str, projection_only: bool = False
) -> float | None:
    if df.is_empty() or column not in df.columns:
        return None

    day_row = df.filter(pl.col("date") == day)
    if projection_only:
        day_row = day_row.filter(pl.col("is_projection"))
    if day_row.height > 0:
        return float(day_row[column][0])

    if projection_only:
        fallback = df.filter(pl.col("is_projection")).tail(1)
    else:
        fallback = df.filter(~pl.col("is_projection")).tail(1)
    if fallback.height == 0:
        return None
    return float(fallback[column][0])


def _fmt_metric(value: float | None) -> str:
    if value is None:
        return "--"
    return f"{value:.1f}"


def _delta_direction(delta: float | None, epsilon: float = 0.05) -> tuple[str, str]:
    if delta is None:
        return "--", COLORS["muted"]
    if delta > epsilon:
        return "↑", "#4CAF50"
    if delta < -epsilon:
        return "↓", "#F44336"
    return "→", COLORS["muted"]


def _forecast_comparison_content(cp: CyclingProcessor, tss_overrides: dict | None):
    today = date.today()
    forecast_end = today + timedelta(days=21)

    ctl_atl = cp.compute_ctl_atl()
    forecast = cp.compute_ctl_atl_forecast(tss_overrides=tss_overrides)

    current_ctl = _metric_value(ctl_atl, today, "ctl")
    current_atl = _metric_value(ctl_atl, today, "atl")
    current_tsb = _metric_value(ctl_atl, today, "tsb")

    forecast_ctl = _metric_value(
        forecast, forecast_end, "ctl_forecast", projection_only=True
    )
    forecast_atl = _metric_value(
        forecast, forecast_end, "atl_forecast", projection_only=True
    )
    forecast_tsb = _metric_value(
        forecast, forecast_end, "tsb_forecast", projection_only=True
    )

    metrics = [
        ("CTL (Fitness)", current_ctl, forecast_ctl, "#2196F3"),
        ("ATL (Fatigue)", current_atl, forecast_atl, "#F44336"),
        ("TSB (Form)", current_tsb, forecast_tsb, "#4CAF50"),
    ]

    has_values = any(current is not None for _, current, _, _ in metrics)
    if not has_values:
        empty = html.Div(
            "No training load data available.",
            style={**CARD_STYLE, "color": COLORS["muted"], "padding": "12px"},
        )
        return empty, html.Div()

    card_items = []
    for name, current, forecast_val, color in metrics:
        delta_value = (
            forecast_val - current
            if current is not None and forecast_val is not None
            else None
        )
        arrow, arrow_color = _delta_direction(delta_value)
        card_items.append(
            html.Div(
                style={
                    **CARD_STYLE,
                    "display": "inline-block",
                    "padding": "12px 16px",
                    "minWidth": "180px",
                },
                children=[
                    html.Div(
                        name,
                        style={
                            "fontSize": "0.78rem",
                            "color": COLORS["muted"],
                            "marginBottom": "6px",
                        },
                    ),
                    html.Div(
                        f"Now {_fmt_metric(current)}",
                        style={
                            "fontSize": "1.0rem",
                            "fontWeight": "600",
                            "color": color,
                        },
                    ),
                    html.Div(
                        f"Day 21 {_fmt_metric(forecast_val)}",
                        style={
                            "fontSize": "0.84rem",
                            "color": COLORS["text"],
                            "marginTop": "4px",
                        },
                    ),
                    html.Div(
                        [
                            html.Span("Delta ", style={"color": COLORS["muted"]}),
                            html.Span(
                                arrow,
                                style={"color": arrow_color, "fontWeight": "700"},
                            ),
                            html.Span(
                                f" {delta_value:+.1f}"
                                if delta_value is not None
                                else " --",
                                style={"color": COLORS["muted"]},
                            ),
                        ],
                        style={"fontSize": "0.8rem", "marginTop": "2px"},
                    ),
                ],
            )
        )

    cards = html.Div(
        style={
            "display": "flex",
            "flexWrap": "wrap",
            "gap": "12px",
            "rowGap": "10px",
            "marginBottom": "12px",
        },
        children=card_items,
    )

    rows = []
    for name, current, forecast_val, _ in metrics:
        delta_value = (
            forecast_val - current
            if current is not None and forecast_val is not None
            else None
        )
        arrow, arrow_color = _delta_direction(delta_value)
        rows.append(
            html.Tr(
                [
                    html.Td(
                        name, style={"padding": "8px 10px", "color": COLORS["text"]}
                    ),
                    html.Td(
                        _fmt_metric(current),
                        style={
                            "padding": "8px 10px",
                            "color": COLORS["text"],
                            "textAlign": "right",
                        },
                    ),
                    html.Td(
                        _fmt_metric(forecast_val),
                        style={
                            "padding": "8px 10px",
                            "color": COLORS["text"],
                            "textAlign": "right",
                        },
                    ),
                    html.Td(
                        [
                            html.Span(
                                arrow, style={"color": arrow_color, "fontWeight": "700"}
                            ),
                            html.Span(
                                f" {delta_value:+.1f}"
                                if delta_value is not None
                                else " --",
                                style={"color": COLORS["muted"]},
                            ),
                        ],
                        style={
                            "padding": "8px 10px",
                            "textAlign": "right",
                        },
                    ),
                ]
            )
        )

    table = html.Div(
        style={**CARD_STYLE, "padding": "8px 10px", "marginBottom": "12px"},
        children=[
            html.Table(
                style={
                    "width": "100%",
                    "borderCollapse": "collapse",
                    "fontSize": "0.84rem",
                },
                children=[
                    html.Thead(
                        html.Tr(
                            [
                                html.Th(
                                    "Metric",
                                    style={
                                        "padding": "8px 10px",
                                        "textAlign": "left",
                                        "color": COLORS["muted"],
                                        "borderBottom": f"1px solid {COLORS['border']}",
                                    },
                                ),
                                html.Th(
                                    "Current",
                                    style={
                                        "padding": "8px 10px",
                                        "textAlign": "right",
                                        "color": COLORS["muted"],
                                        "borderBottom": f"1px solid {COLORS['border']}",
                                    },
                                ),
                                html.Th(
                                    "Day 21 Forecast",
                                    style={
                                        "padding": "8px 10px",
                                        "textAlign": "right",
                                        "color": COLORS["muted"],
                                        "borderBottom": f"1px solid {COLORS['border']}",
                                    },
                                ),
                                html.Th(
                                    "Change",
                                    style={
                                        "padding": "8px 10px",
                                        "textAlign": "right",
                                        "color": COLORS["muted"],
                                        "borderBottom": f"1px solid {COLORS['border']}",
                                    },
                                ),
                            ]
                        )
                    ),
                    html.Tbody(rows),
                ],
            )
        ],
    )

    return cards, table


def cycling_training_load_layout():
    return html.Div(
        [
            html.Div(
                style={
                    "display": "flex",
                    "flexWrap": "wrap",
                    "alignItems": "center",
                    "gap": "8px",
                    "rowGap": "8px",
                    "marginBottom": "12px",
                },
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
                        style={"marginLeft": "8px"},
                        labelStyle={"color": COLORS["text"]},
                        inputStyle={"marginRight": "4px"},
                    ),
                ],
            ),
            html.Div(
                style={
                    "marginBottom": "12px",
                    "display": "flex",
                    "flexDirection": "column",
                    "gap": "8px",
                },
                children=[
                    html.Div(
                        "Forecast TSS Input (next 21 days)",
                        style={"color": COLORS["muted"], "fontSize": "0.8rem"},
                    ),
                    dash_table.DataTable(
                        id="training-load-forecast-table",
                        data=_forecast_input_rows(),
                        columns=_forecast_table_columns(),
                        editable=True,
                        style_header={
                            "backgroundColor": COLORS["card"],
                            "color": COLORS["muted"],
                            "fontWeight": "500",
                            "borderBottom": f"1px solid {COLORS['border']}",
                        },
                        style_cell={
                            "backgroundColor": COLORS["card"],
                            "color": COLORS["text"],
                            "border": f"1px solid {COLORS['border']}",
                            "padding": "6px 8px",
                            "fontSize": "0.82rem",
                            "textAlign": "left",
                            "whiteSpace": "nowrap",
                        },
                        style_cell_conditional=[
                            {
                                "if": {"column_id": "date_1"},
                                "width": "96px",
                                "minWidth": "96px",
                                "maxWidth": "96px",
                            },
                            {
                                "if": {"column_id": "day_1"},
                                "width": "56px",
                                "minWidth": "56px",
                                "maxWidth": "56px",
                            },
                            {
                                "if": {"column_id": "tss_1"},
                                "width": "72px",
                                "minWidth": "72px",
                                "maxWidth": "72px",
                                "textAlign": "right",
                            },
                            {
                                "if": {"column_id": "date_2"},
                                "width": "96px",
                                "minWidth": "96px",
                                "maxWidth": "96px",
                            },
                            {
                                "if": {"column_id": "day_2"},
                                "width": "56px",
                                "minWidth": "56px",
                                "maxWidth": "56px",
                            },
                            {
                                "if": {"column_id": "tss_2"},
                                "width": "72px",
                                "minWidth": "72px",
                                "maxWidth": "72px",
                                "textAlign": "right",
                            },
                            {
                                "if": {"column_id": "date_3"},
                                "width": "96px",
                                "minWidth": "96px",
                                "maxWidth": "96px",
                            },
                            {
                                "if": {"column_id": "day_3"},
                                "width": "56px",
                                "minWidth": "56px",
                                "maxWidth": "56px",
                            },
                            {
                                "if": {"column_id": "tss_3"},
                                "width": "72px",
                                "minWidth": "72px",
                                "maxWidth": "72px",
                                "textAlign": "right",
                            },
                        ],
                        style_data_conditional=[
                            {"if": {"row_index": "odd"}, "backgroundColor": "#1e2130"},
                        ],
                        style_table={"overflowX": "auto", "width": "100%"},
                    ),
                    html.Button(
                        "Apply Forecast",
                        id="apply-forecast-btn",
                        n_clicks=0,
                        style=_apply_forecast_button_style(False),
                    ),
                    html.Button(
                        "Clear Forecast",
                        id="clear-forecast-btn",
                        n_clicks=0,
                        style=_clear_forecast_button_style(False),
                    ),
                ],
            ),
            dcc.Store(id="training-load-forecast-overrides", data={}),
            html.H3(
                "Forecast Comparison",
                style={
                    "color": COLORS["accent"],
                    "marginBottom": "12px",
                    "fontSize": "0.95rem",
                },
            ),
            html.Div(id="training-load-comparison-cards"),
            html.Div(id="training-load-comparison-table"),
            html.Div(dcc.Graph(id="training-load-chart"), style=CARD_STYLE),
        ]
    )


@callback(
    Output("training-load-forecast-overrides", "data"),
    Output("training-load-forecast-table", "data"),
    Input("apply-forecast-btn", "n_clicks"),
    Input("clear-forecast-btn", "n_clicks"),
    State("training-load-forecast-table", "data"),
    prevent_initial_call=True,
)
def apply_training_load_forecast(_apply_clicks, _clear_clicks, forecast_rows):
    if ctx.triggered_id == "clear-forecast-btn":
        return {}, _clear_tss_inputs(forecast_rows)
    return _parse_tss_overrides(forecast_rows), forecast_rows


@callback(
    Output("clear-forecast-btn", "style"),
    Input("training-load-forecast-table", "data"),
)
def update_clear_forecast_button_style(forecast_rows):
    has_data = False
    if forecast_rows:
        for row in forecast_rows:
            for block_idx in range(1, 4):
                tss_value = row.get(f"tss_{block_idx}")
                if tss_value is not None and str(tss_value).strip() != "":
                    has_data = True
                    break
            if has_data:
                break
    return _clear_forecast_button_style(has_data)


@callback(
    Output("apply-forecast-btn", "style"),
    Input("training-load-forecast-table", "data"),
    Input("training-load-forecast-overrides", "data"),
)
def update_apply_forecast_button_style(forecast_rows, applied_overrides):
    current_overrides = _parse_tss_overrides(forecast_rows)
    applied = applied_overrides or {}
    has_unsaved_changes = current_overrides != applied
    return _apply_forecast_button_style(has_unsaved_changes)


@callback(
    Output("training-load-chart", "figure"),
    Output("training-load-comparison-cards", "children"),
    Output("training-load-comparison-table", "children"),
    Input("date-range", "value"),
    Input("show-forecast", "value"),
    Input("training-load-forecast-overrides", "data"),
)
def update_training_load(date_range, show_forecast, tss_overrides):
    start_date = None
    if date_range != "all":
        months = int(date_range)
        d = date.today()
        start_date = (d.replace(day=1) - timedelta(days=months * 30)).isoformat()

    cp = CyclingProcessor(user_id=get_user_id())
    fig = cp.plot_training_load(
        start_date=start_date,
        include_forecast="yes" in (show_forecast or []),
        tss_overrides=tss_overrides,
    )
    comparison_cards, comparison_table = _forecast_comparison_content(cp, tss_overrides)

    fig.update_layout(
        paper_bgcolor=COLORS["card"],
        plot_bgcolor=COLORS["card"],
        font_color=COLORS["text"],
        xaxis=dict(gridcolor=COLORS["border"], automargin=True),
        yaxis=dict(gridcolor=COLORS["border"], automargin=True),
        yaxis2=dict(automargin=True),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            font=dict(size=10),
        ),
        margin=dict(t=40, b=70, l=44, r=36),
        autosize=True,
        height=550,
    )
    return fig, comparison_cards, comparison_table
