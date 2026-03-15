import json
import os
from datetime import date

import plotly.graph_objects as go
from dash import Input, Output, State, callback, ctx, dcc, html, no_update

from ..config import BODY_WEIGHT_LB, CARD_STYLE, COLORS, WT_DATA_FILE, WT_DRAFT_FILE


def _load_data():
    with open(WT_DATA_FILE, "r") as f:
        return json.load(f)


def _load_draft():
    if os.path.exists(WT_DRAFT_FILE):
        with open(WT_DRAFT_FILE, "r") as f:
            return json.load(f)
    return {"date": date.today().isoformat(), "exercises": []}


def _save_draft(draft):
    os.makedirs(os.path.dirname(WT_DRAFT_FILE), exist_ok=True)
    with open(WT_DRAFT_FILE, "w") as f:
        json.dump(draft, f, indent=2)


def _get_exercise_names(data):
    names = set()
    for entry in data:
        for ex in entry["exercises"]:
            names.add(ex["name"])
    return sorted(names)


BW_PLUS_EXERCISES = {"pull_ups", "dips", "pullups", "chin_ups"}


def _bw(entry):
    """Get body weight for an entry, falling back to config default."""
    return entry.get("body_weight") or BODY_WEIGHT_LB


def _set_weight(s, bw, exercise_name=None):
    """Get effective weight for a set. BW exercises use body weight,
    BW+ exercises (pullups, dips) add body weight to entered weight."""
    if s["weight"] == 0:
        return bw
    if exercise_name and exercise_name in BW_PLUS_EXERCISES:
        return bw + s["weight"]
    return s["weight"]


def _render_draft(draft):
    """Render the current draft workout as HTML."""
    if not draft or not draft.get("exercises"):
        return html.Div("No exercises added yet.", style={"color": COLORS["muted"]})

    exercises = []
    for i, ex in enumerate(draft["exercises"]):
        sets_text = " | ".join(
            f"{s['reps']}x{s['weight']}lb" if s["weight"] > 0 else f"{s['reps']}xBW"
            for s in ex["sets"]
        )
        exercises.append(
            html.Div(
                style={
                    "display": "flex",
                    "alignItems": "center",
                    "gap": "16px",
                    "marginBottom": "4px",
                },
                children=[
                    html.Span(
                        ex["name"].replace("_", " ").title(),
                        style={
                            "fontWeight": "500",
                            "minWidth": "160px",
                            "color": "#ccc",
                        },
                    ),
                    html.Span(
                        sets_text,
                        style={"color": COLORS["muted"], "fontSize": "0.85rem"},
                    ),
                ],
            )
        )

    return html.Div(
        [
            html.H3(
                draft["date"],
                style={
                    "color": COLORS["accent"],
                    "marginBottom": "12px",
                    "fontSize": "0.95rem",
                },
            ),
            html.Div(exercises),
        ]
    )


# ── Layout ──────────────────────────────────────────────────


def _weights_log():
    """Existing workout log and progress charts."""
    data = _load_data()
    exercise_names = _get_exercise_names(data)
    sorted_data = sorted(data, key=lambda x: x["date"], reverse=True)

    comparison = html.Div(
        style=CARD_STYLE,
        children=[
            html.H3(
                "Exercise Progress",
                style={
                    "color": COLORS["accent"],
                    "marginBottom": "12px",
                    "fontSize": "0.95rem",
                },
            ),
            dcc.Dropdown(
                id="exercise-select",
                options=[
                    {"label": n.replace("_", " ").title(), "value": n}
                    for n in exercise_names
                ],
                value=[
                    n for n in ["inclined_bench_press", "squat"] if n in exercise_names
                ]
                or (exercise_names[:1] if exercise_names else []),
                multi=True,
                placeholder="Select exercises to compare...",
                style={"backgroundColor": COLORS["card"], "marginBottom": "12px"},
            ),
            html.Div(id="exercise-progress-table"),
        ],
    )

    # ── Session Volume bar chart ──
    session_dates = []
    session_volumes = []
    for entry in sorted(data, key=lambda x: x["date"]):
        bw = _bw(entry)
        total_vol = 0
        for ex in entry["exercises"]:
            total_vol += sum(
                _set_weight(s, bw, ex["name"]) * s["reps"] for s in ex["sets"]
            )
        session_dates.append(entry["date"])
        session_volumes.append(total_vol)

    session_fig = go.Figure(
        go.Bar(
            x=session_dates,
            y=session_volumes,
            marker_color="#2196F3",
            text=[f"{int(v):,}" for v in session_volumes],
            textposition="outside",
            textfont_size=9,
        )
    )
    max_vol = max(session_volumes) if session_volumes else 1
    session_fig.update_layout(
        paper_bgcolor=COLORS["card"],
        plot_bgcolor=COLORS["card"],
        font_color=COLORS["text"],
        xaxis=dict(gridcolor=COLORS["border"], title="Date"),
        yaxis=dict(
            gridcolor=COLORS["border"],
            title="Total Volume (lb×reps)",
            range=[0, max_vol * 1.25],
        ),
        height=350,
        margin=dict(t=20, b=60, l=60, r=30),
        showlegend=False,
    )

    session_chart = html.Div(
        style=CARD_STYLE,
        children=[
            html.H3(
                "Session Volume",
                style={
                    "color": COLORS["accent"],
                    "marginBottom": "4px",
                    "fontSize": "0.95rem",
                },
            ),
            html.Div(
                "Total volume across all exercises per workout",
                style={
                    "color": COLORS["muted"],
                    "fontSize": "0.7rem",
                    "marginBottom": "12px",
                },
            ),
            dcc.Graph(id="session-volume-chart", figure=session_fig),
        ],
    )

    return html.Div([comparison, session_chart])


def _weights_personal_records():
    """Personal records and estimated 1RM table."""
    data = _load_data()

    pr_data = {}  # {exercise: {max_wt, date, e1rm, e1rm_wt, e1rm_reps, e1rm_date}}
    for entry in data:
        bw = _bw(entry)
        for ex in entry["exercises"]:
            name = ex["name"]
            for s in ex["sets"]:
                wt = _set_weight(s, bw, name)
                if name not in pr_data or wt > pr_data[name]["max_wt"]:
                    pr_data[name] = {
                        "max_wt": wt,
                        "date": entry["date"],
                    }
                # Best estimated 1RM (Epley: wt * (1 + reps/30))
                e1rm = wt * (1 + s["reps"] / 30)
                if "e1rm" not in pr_data[name] or e1rm > pr_data[name]["e1rm"]:
                    pr_data[name]["e1rm"] = e1rm
                    pr_data[name]["e1rm_wt"] = wt
                    pr_data[name]["e1rm_reps"] = s["reps"]
                    pr_data[name]["e1rm_date"] = entry["date"]

    cell_style = {
        "padding": "8px 12px",
        "borderBottom": f"1px solid {COLORS['border']}",
        "fontSize": "0.85rem",
    }
    header_style = {
        **cell_style,
        "color": COLORS["muted"],
        "fontWeight": "600",
        "fontSize": "0.75rem",
    }

    pr_header = html.Tr(
        [
            html.Th("Exercise", style={**header_style, "textAlign": "left"}),
            html.Th("Max Weight", style={**header_style, "textAlign": "center"}),
            html.Th("Date", style={**header_style, "textAlign": "center"}),
            html.Th("Est. 1RM", style={**header_style, "textAlign": "center"}),
            html.Th("Based On", style={**header_style, "textAlign": "center"}),
            html.Th("1RM Date", style={**header_style, "textAlign": "center"}),
        ]
    )

    pr_rows = []
    for name in sorted(pr_data.keys()):
        p = pr_data[name]
        label = name.replace("_", " ").title()
        pr_rows.append(
            html.Tr(
                [
                    html.Td(
                        label,
                        style={
                            **cell_style,
                            "textAlign": "left",
                            "fontWeight": "500",
                            "color": "#ccc",
                            "whiteSpace": "nowrap",
                        },
                    ),
                    html.Td(
                        f"{p['max_wt']}lb",
                        style={**cell_style, "textAlign": "center", "color": "#fff"},
                    ),
                    html.Td(
                        p["date"],
                        style={
                            **cell_style,
                            "textAlign": "center",
                            "color": COLORS["muted"],
                            "fontSize": "0.75rem",
                        },
                    ),
                    html.Td(
                        f"{p['e1rm']:.0f}lb",
                        style={
                            **cell_style,
                            "textAlign": "center",
                            "color": COLORS["accent"],
                            "fontWeight": "600",
                        },
                    ),
                    html.Td(
                        f"{p['e1rm_reps']}x{p['e1rm_wt']}lb",
                        style={
                            **cell_style,
                            "textAlign": "center",
                            "color": COLORS["muted"],
                            "fontSize": "0.75rem",
                        },
                    ),
                    html.Td(
                        p["e1rm_date"],
                        style={
                            **cell_style,
                            "textAlign": "center",
                            "color": COLORS["muted"],
                            "fontSize": "0.75rem",
                        },
                    ),
                ]
            )
        )

    pr_table = html.Table(
        [html.Thead(pr_header), html.Tbody(pr_rows)],
        style={"width": "100%", "borderCollapse": "collapse"},
    )

    return html.Div(
        style=CARD_STYLE,
        children=[
            html.H3(
                "Personal Records & Estimated 1RM",
                style={
                    "color": COLORS["accent"],
                    "marginBottom": "4px",
                    "fontSize": "0.95rem",
                },
            ),
            html.Div(
                "1RM estimated via Epley formula: weight × (1 + reps/30)",
                style={
                    "color": COLORS["muted"],
                    "fontSize": "0.7rem",
                    "marginBottom": "12px",
                },
            ),
            html.Div(pr_table, style={"overflowX": "auto"}),
        ],
    )


def _weights_session_detail():
    """Session detail view with date selector."""
    data = _load_data()
    sorted_data = sorted(data, key=lambda x: x["date"], reverse=True)

    return html.Div(
        [
            dcc.Dropdown(
                id="workout-selector",
                options=[
                    {"label": entry["date"], "value": i}
                    for i, entry in enumerate(sorted_data)
                ],
                value=0 if sorted_data else None,
                placeholder="Search by date...",
                searchable=True,
                style={"marginBottom": "16px", "color": "#000"},
            ),
            html.Div(id="workout-detail-content"),
        ]
    )


def _weights_entry():
    """Workout entry form."""
    data = _load_data()
    exercise_names = _get_exercise_names(data)
    draft = _load_draft()

    input_style = {
        "backgroundColor": COLORS["card"],
        "color": COLORS["text"],
        "border": f"1px solid {COLORS['border']}",
        "borderRadius": "4px",
        "padding": "8px",
        "width": "100%",
    }

    btn_style = {
        "backgroundColor": COLORS["accent"],
        "color": "#fff",
        "border": "none",
        "borderRadius": "4px",
        "padding": "8px 16px",
        "cursor": "pointer",
        "fontSize": "0.85rem",
    }

    btn_secondary = {
        **btn_style,
        "backgroundColor": COLORS["border"],
    }

    return html.Div(
        [
            # Stores (placed first to avoid Dash 4 re-render issues)
            dcc.Store(id="wt-sets-store", data=[]),
            # Date picker and body weight
            html.Div(
                style={"display": "flex", "gap": "16px", "marginBottom": "16px"},
                children=[
                    html.Div(
                        style={"maxWidth": "200px", "color": "#000"},
                        children=[
                            html.Label(
                                "Date",
                                style={
                                    "color": COLORS["muted"],
                                    "fontSize": "0.8rem",
                                    "marginBottom": "4px",
                                    "display": "block",
                                },
                            ),
                            dcc.DatePickerSingle(
                                id="wt-date",
                                date=draft.get("date", date.today().isoformat()),
                                display_format="YYYY-MM-DD",
                            ),
                        ],
                    ),
                    html.Div(
                        style={"maxWidth": "140px"},
                        children=[
                            html.Label(
                                "Body Weight (lb)",
                                style={
                                    "color": COLORS["muted"],
                                    "fontSize": "0.8rem",
                                    "marginBottom": "4px",
                                    "display": "block",
                                },
                            ),
                            dcc.Input(
                                id="wt-body-weight",
                                type="number",
                                value=draft.get("body_weight", BODY_WEIGHT_LB),
                                placeholder=str(BODY_WEIGHT_LB),
                                style=input_style,
                                debounce=True,
                            ),
                        ],
                    ),
                ],
            ),
            # Exercise input
            html.Div(
                style={**CARD_STYLE, "marginBottom": "16px"},
                children=[
                    html.H3(
                        "Add Exercise",
                        style={
                            "color": COLORS["accent"],
                            "marginBottom": "12px",
                            "fontSize": "0.95rem",
                        },
                    ),
                    html.Label(
                        "Exercise",
                        style={
                            "color": COLORS["muted"],
                            "fontSize": "0.8rem",
                            "marginBottom": "4px",
                            "display": "block",
                        },
                    ),
                    html.Div(
                        style={"display": "flex", "gap": "8px", "marginBottom": "12px"},
                        children=[
                            html.Div(
                                style={"flex": "1"},
                                children=[
                                    dcc.Dropdown(
                                        id="wt-exercise-name",
                                        options=[
                                            {
                                                "label": n.replace("_", " ").title(),
                                                "value": n,
                                            }
                                            for n in exercise_names
                                        ],
                                        placeholder="Select existing...",
                                        searchable=True,
                                        style={"color": "#000"},
                                    ),
                                ],
                            ),
                            html.Div(
                                style={"flex": "1"},
                                children=[
                                    dcc.Input(
                                        id="wt-new-exercise",
                                        type="text",
                                        placeholder="Or type new name...",
                                        style=input_style,
                                        debounce=True,
                                    ),
                                ],
                            ),
                        ],
                    ),
                    # Sets input row
                    html.Div(
                        style={
                            "display": "flex",
                            "gap": "8px",
                            "alignItems": "flex-end",
                            "marginBottom": "12px",
                        },
                        children=[
                            html.Div(
                                style={"flex": "1"},
                                children=[
                                    html.Label(
                                        "Weight (lb)",
                                        style={
                                            "color": COLORS["muted"],
                                            "fontSize": "0.8rem",
                                            "display": "block",
                                            "marginBottom": "4px",
                                        },
                                    ),
                                    dcc.Input(
                                        id="wt-weight",
                                        type="text",
                                        placeholder="135 or 130,150,150",
                                        style=input_style,
                                        debounce=True,
                                    ),
                                ],
                            ),
                            html.Div(
                                style={"flex": "1"},
                                children=[
                                    html.Label(
                                        "Reps",
                                        style={
                                            "color": COLORS["muted"],
                                            "fontSize": "0.8rem",
                                            "display": "block",
                                            "marginBottom": "4px",
                                        },
                                    ),
                                    dcc.Input(
                                        id="wt-reps",
                                        type="text",
                                        placeholder="10 or 8,9,10",
                                        style=input_style,
                                        debounce=True,
                                    ),
                                ],
                            ),
                            html.Button(
                                "Add Set", id="wt-add-set", n_clicks=0, style=btn_style
                            ),
                        ],
                    ),
                    # Current sets display
                    html.Div(id="wt-current-sets", style={"marginBottom": "12px"}),
                    # Add exercise / clear exercise buttons
                    html.Div(
                        style={"display": "flex", "gap": "8px"},
                        children=[
                            html.Button(
                                "Add Exercise to Workout",
                                id="wt-add-exercise",
                                n_clicks=0,
                                style=btn_style,
                            ),
                            html.Button(
                                "Clear Exercise",
                                id="wt-clear-exercise",
                                n_clicks=0,
                                style=btn_secondary,
                            ),
                        ],
                    ),
                ],
            ),
            # Draft workout preview
            html.Div(
                style=CARD_STYLE,
                children=[
                    html.H3(
                        "Current Workout",
                        style={
                            "color": COLORS["accent"],
                            "marginBottom": "12px",
                            "fontSize": "0.95rem",
                        },
                    ),
                    html.Div(id="wt-draft-preview", children=_render_draft(draft)),
                    html.Div(
                        style={"display": "flex", "gap": "8px", "marginTop": "16px"},
                        children=[
                            html.Button(
                                "Publish Workout",
                                id="wt-publish",
                                n_clicks=0,
                                style=btn_style,
                            ),
                            html.Button(
                                "Clear Draft",
                                id="wt-clear-draft",
                                n_clicks=0,
                                style=btn_secondary,
                            ),
                        ],
                    ),
                    html.Div(id="wt-publish-status", style={"marginTop": "8px"}),
                ],
            ),
        ]
    )


def weights_tab():
    return html.Div(
        [
            dcc.Tabs(
                id="weights-subtabs",
                value="log",
                children=[
                    dcc.Tab(
                        label="Overview",
                        value="log",
                        style={"padding": "6px 16px", "lineHeight": "28px"},
                        selected_style={
                            "padding": "6px 16px",
                            "lineHeight": "28px",
                            "borderTop": f"2px solid {COLORS['accent']}",
                        },
                    ),
                    dcc.Tab(
                        label="Personal Records",
                        value="pr",
                        style={"padding": "6px 16px", "lineHeight": "28px"},
                        selected_style={
                            "padding": "6px 16px",
                            "lineHeight": "28px",
                            "borderTop": f"2px solid {COLORS['accent']}",
                        },
                    ),
                    dcc.Tab(
                        label="Lifting Session Detail",
                        value="session",
                        style={"padding": "6px 16px", "lineHeight": "28px"},
                        selected_style={
                            "padding": "6px 16px",
                            "lineHeight": "28px",
                            "borderTop": f"2px solid {COLORS['accent']}",
                        },
                    ),
                    dcc.Tab(
                        label="New Entry",
                        value="entry",
                        style={"padding": "6px 16px", "lineHeight": "28px"},
                        selected_style={
                            "padding": "6px 16px",
                            "lineHeight": "28px",
                            "borderTop": f"2px solid {COLORS['accent']}",
                        },
                    ),
                ],
                style={"height": "40px", "marginBottom": "16px"},
                colors={
                    "border": "transparent",
                    "primary": COLORS["accent"],
                    "background": "transparent",
                },
            ),
            # All subtabs rendered statically; visibility toggled via callback
            html.Div(id="weights-log-container", children=_weights_log()),
            html.Div(
                id="weights-pr-container",
                children=_weights_personal_records(),
                style={"display": "none"},
            ),
            html.Div(
                id="weights-session-container",
                children=_weights_session_detail(),
                style={"display": "none"},
            ),
            html.Div(
                id="weights-entry-container",
                children=_weights_entry(),
                style={"display": "none"},
            ),
        ]
    )


# ── Callbacks ───────────────────────────────────────────────


@callback(
    Output("weights-log-container", "style"),
    Output("weights-pr-container", "style"),
    Output("weights-session-container", "style"),
    Output("weights-entry-container", "style"),
    Output("wt-date", "date"),
    Input("weights-subtabs", "value"),
)
def render_weights_subtab(subtab):
    hide = {"display": "none"}
    show = {"display": "block"}
    if subtab == "pr":
        return hide, show, hide, hide, no_update
    if subtab == "session":
        return hide, hide, show, hide, no_update
    if subtab == "entry":
        draft = _load_draft()
        return hide, hide, hide, show, draft.get("date", date.today().isoformat())
    return show, hide, hide, hide, no_update


@callback(
    Output("exercise-progress-table", "children"),
    Input("exercise-select", "value"),
)
def update_exercise_progress(selected):
    if not selected:
        return html.Div(
            "Select an exercise to see progress",
            style={"color": COLORS["muted"], "padding": "16px"},
        )

    if isinstance(selected, str):
        selected = [selected]

    data = _load_data()

    # Build {exercise_name: [(date, volume, max_weight), ...]} sorted by date
    exercise_data = {}
    for exercise_name in selected:
        entries = []
        for entry in sorted(data, key=lambda x: x["date"]):
            bw = _bw(entry)
            for ex in entry["exercises"]:
                if ex["name"] == exercise_name:
                    volume = sum(
                        _set_weight(s, bw, exercise_name) * s["reps"]
                        for s in ex["sets"]
                    )
                    max_wt = max(_set_weight(s, bw, exercise_name) for s in ex["sets"])
                    entries.append((entry["date"], volume, max_wt))
        exercise_data[exercise_name] = entries

    # Find the max number of occurrences across selected exercises
    max_cols = max((len(v) for v in exercise_data.values()), default=0)

    if max_cols == 0:
        return html.Div(
            "No data for selected exercises.",
            style={"color": COLORS["muted"], "padding": "16px"},
        )

    cell_style = {
        "padding": "8px 12px",
        "borderBottom": f"1px solid {COLORS['border']}",
        "fontSize": "0.85rem",
        "textAlign": "center",
    }

    header_style = {
        **cell_style,
        "color": COLORS["muted"],
        "fontWeight": "600",
        "fontSize": "0.75rem",
    }

    # Header row: Exercise | 1 | 2 | 3 | ...
    header = html.Tr(
        [html.Th("Exercise", style={**header_style, "textAlign": "left"})]
        + [html.Th(str(i + 1), style=header_style) for i in range(max_cols)]
    )

    # One row per exercise
    rows = []
    for exercise_name in selected:
        entries = exercise_data[exercise_name]
        label = exercise_name.replace("_", " ").title()
        cells = [
            html.Td(
                label,
                style={
                    **cell_style,
                    "textAlign": "left",
                    "fontWeight": "500",
                    "color": "#ccc",
                    "whiteSpace": "nowrap",
                },
            )
        ]
        for date_str, volume, max_wt in entries:
            cells.append(
                html.Td(
                    [
                        html.Div(f"{int(volume):,}"),
                        html.Div(
                            date_str,
                            style={
                                "fontSize": "0.65rem",
                                "color": COLORS["muted"],
                                "marginTop": "2px",
                            },
                        ),
                    ],
                    style=cell_style,
                )
            )
        # Pad remaining columns
        for _ in range(max_cols - len(entries)):
            cells.append(html.Td("—", style={**cell_style, "color": COLORS["muted"]}))
        rows.append(html.Tr(cells))

    # Sub-header with "index date" label
    date_note = html.Div(
        "Column index = occurrence # (hover cells for date)",
        style={
            "color": COLORS["muted"],
            "fontSize": "0.75rem",
            "marginBottom": "8px",
        },
    )

    table = html.Table(
        [html.Thead(header), html.Tbody(rows)],
        style={
            "width": "100%",
            "borderCollapse": "collapse",
            "overflowX": "auto",
        },
    )

    # ── Growth table (moving average % change) ──
    # Each cell shows the average % change from index 1 up to that transition
    growth_cols = max_cols - 1
    if growth_cols > 0:
        growth_header = html.Tr(
            [html.Th("Exercise", style={**header_style, "textAlign": "left"})]
            + [html.Th(str(i + 2), style=header_style) for i in range(growth_cols)]
        )

        growth_rows = []
        for exercise_name in selected:
            entries = exercise_data[exercise_name]
            label = exercise_name.replace("_", " ").title()
            cells = [
                html.Td(
                    label,
                    style={
                        **cell_style,
                        "textAlign": "left",
                        "fontWeight": "500",
                        "color": "#ccc",
                        "whiteSpace": "nowrap",
                    },
                )
            ]
            # % change from baseline (index 1), volume delta, and max weight
            baseline_vol = entries[0][1] if entries else 0
            for j in range(growth_cols):
                if j + 1 < len(entries) and baseline_vol > 0:
                    curr_vol = entries[j + 1][1]
                    curr_max_wt = entries[j + 1][2]
                    vol_delta = curr_vol - baseline_vol
                    pct = (vol_delta / baseline_vol) * 100
                    pct_sign = "+" if pct >= 0 else ""
                    vol_sign = "+" if vol_delta >= 0 else ""
                    color = "#4CAF50" if pct >= 0 else "#FF5252"
                    cells.append(
                        html.Td(
                            [
                                html.Div(
                                    f"{pct_sign}{pct:.1f}%",
                                    style={"color": color},
                                ),
                                html.Div(
                                    f"{vol_sign}{int(vol_delta):,} vol",
                                    style={
                                        "fontSize": "0.7rem",
                                        "color": COLORS["muted"],
                                        "marginTop": "2px",
                                    },
                                ),
                                html.Div(
                                    f"{int(curr_max_wt)}lb max",
                                    style={
                                        "fontSize": "0.7rem",
                                        "color": COLORS["muted"],
                                        "marginTop": "1px",
                                    },
                                ),
                            ],
                            style=cell_style,
                        )
                    )
                else:
                    cells.append(
                        html.Td("—", style={**cell_style, "color": COLORS["muted"]})
                    )
            growth_rows.append(html.Tr(cells))

        growth_table = html.Table(
            [html.Thead(growth_header), html.Tbody(growth_rows)],
            style={
                "width": "100%",
                "borderCollapse": "collapse",
                "overflowX": "auto",
            },
        )

        growth_section = html.Div(
            [
                html.H3(
                    "Avg Volume Growth",
                    style={
                        "color": COLORS["accent"],
                        "marginBottom": "8px",
                        "marginTop": "20px",
                        "fontSize": "0.95rem",
                    },
                ),
                html.Div(
                    "% change from first occurrence (baseline)",
                    style={
                        "color": COLORS["muted"],
                        "fontSize": "0.75rem",
                        "marginBottom": "8px",
                    },
                ),
                html.Div(growth_table, style={"overflowX": "auto"}),
            ]
        )
    else:
        growth_section = html.Div()

    return html.Div(
        [date_note, html.Div(table, style={"overflowX": "auto"}), growth_section],
    )


@callback(
    Output("workout-detail-content", "children"),
    Input("workout-selector", "value"),
)
def update_workout_detail(selected_index):
    if selected_index is None:
        return html.Div(
            "Select a workout to view details.", style={"color": COLORS["muted"]}
        )

    data = _load_data()
    sorted_data = sorted(data, key=lambda x: x["date"], reverse=True)

    if selected_index < 0 or selected_index >= len(sorted_data):
        return html.Div("Workout not found.", style={"color": COLORS["muted"]})

    entry = sorted_data[selected_index]

    exercises = []
    for ex in entry["exercises"]:
        sets_text = " | ".join(
            f"{s['reps']}x{s['weight']}lb" if s["weight"] > 0 else f"{s['reps']}xBW"
            for s in ex["sets"]
        )
        exercises.append(
            html.Div(
                style={
                    "display": "flex",
                    "alignItems": "center",
                    "gap": "16px",
                    "marginBottom": "4px",
                },
                children=[
                    html.Span(
                        ex["name"].replace("_", " ").title(),
                        style={
                            "fontWeight": "500",
                            "minWidth": "160px",
                            "color": "#ccc",
                        },
                    ),
                    html.Span(
                        sets_text,
                        style={"color": COLORS["muted"], "fontSize": "0.85rem"},
                    ),
                ],
            )
        )

    return html.Div(
        style=CARD_STYLE,
        children=[
            html.H3(
                entry["date"],
                style={
                    "color": COLORS["accent"],
                    "marginBottom": "12px",
                    "fontSize": "0.95rem",
                },
            ),
            html.Div(exercises),
        ],
    )


@callback(
    Output("wt-sets-store", "data"),
    Output("wt-current-sets", "children"),
    Output("wt-weight", "value"),
    Output("wt-reps", "value"),
    Input("wt-add-set", "n_clicks"),
    Input("wt-add-exercise", "n_clicks"),
    Input("wt-clear-exercise", "n_clicks"),
    State("wt-weight", "value"),
    State("wt-reps", "value"),
    State("wt-sets-store", "data"),
    prevent_initial_call=True,
)
def handle_sets(
    add_set_clicks,
    add_exercise_clicks,
    clear_exercise_clicks,
    weight,
    reps,
    current_sets,
):
    triggered = ctx.triggered_id
    click_map = {
        "wt-add-set": add_set_clicks,
        "wt-add-exercise": add_exercise_clicks,
        "wt-clear-exercise": clear_exercise_clicks,
    }
    if not triggered or not click_map.get(triggered):
        return no_update, no_update, no_update, no_update

    if triggered in ("wt-add-exercise", "wt-clear-exercise"):
        # Clear sets after adding or clearing exercise
        return [], html.Div(), None, ""

    if triggered == "wt-add-set":
        if not weight or not reps:
            return current_sets, _render_sets(current_sets), weight, reps

        # Parse comma-separated values
        try:
            weights = [int(w.strip()) for w in str(weight).split(",")]
            rep_list = [int(r.strip()) for r in str(reps).split(",")]
        except (ValueError, AttributeError):
            return current_sets, _render_sets(current_sets), weight, reps

        # If one weight but multiple reps, repeat the weight
        if len(weights) == 1 and len(rep_list) > 1:
            weights = weights * len(rep_list)
        # If one rep count but multiple weights, repeat the reps
        if len(rep_list) == 1 and len(weights) > 1:
            rep_list = rep_list * len(weights)

        # Mismatched lengths (neither is 1)
        if len(weights) != len(rep_list):
            return current_sets, _render_sets(current_sets), weight, reps

        for w, r in zip(weights, rep_list):
            new_set = {"set": len(current_sets) + 1, "weight": w, "reps": r}
            current_sets = current_sets + [new_set]
        return current_sets, _render_sets(current_sets), None, ""

    return no_update, no_update, no_update, no_update


def _render_sets(sets):
    if not sets:
        return html.Div()
    text = " | ".join(
        (
            f"Set {s['set']}: {s['reps']}x{s['weight']}lb"
            if s["weight"] > 0
            else f"Set {s['set']}: {s['reps']}xBW"
        )
        for s in sets
    )
    return html.Div(text, style={"color": COLORS["text"], "fontSize": "0.85rem"})


@callback(
    Output("wt-draft-preview", "children"),
    Output("wt-publish-status", "children"),
    Output("wt-exercise-name", "value"),
    Output("wt-new-exercise", "value"),
    Input("wt-add-exercise", "n_clicks"),
    Input("wt-publish", "n_clicks"),
    Input("wt-clear-draft", "n_clicks"),
    Input("wt-clear-exercise", "n_clicks"),
    State("wt-exercise-name", "value"),
    State("wt-new-exercise", "value"),
    State("wt-sets-store", "data"),
    State("wt-date", "date"),
    State("wt-body-weight", "value"),
    prevent_initial_call=True,
)
def handle_workout(
    add_ex_clicks,
    publish_clicks,
    clear_clicks,
    clear_exercise_clicks,
    exercise_name,
    new_exercise,
    sets,
    workout_date,
    body_weight,
):
    triggered = ctx.triggered_id
    click_map = {
        "wt-add-exercise": add_ex_clicks,
        "wt-publish": publish_clicks,
        "wt-clear-draft": clear_clicks,
        "wt-clear-exercise": clear_exercise_clicks,
    }
    if not triggered or not click_map.get(triggered):
        return no_update, no_update, no_update, no_update

    if triggered == "wt-add-exercise":
        # New exercise name takes priority; normalize to snake_case
        name = (
            new_exercise.strip().lower().replace(" ", "_")
            if new_exercise and new_exercise.strip()
            else exercise_name
        )
        if not name or not sets:
            draft = _load_draft()
            return (
                _render_draft(draft),
                html.Div(
                    "Please select an exercise and add at least one set.",
                    style={"color": "#FF5252"},
                ),
                exercise_name,
                new_exercise,
            )

        draft = _load_draft()
        draft["date"] = workout_date or date.today().isoformat()
        draft["body_weight"] = body_weight or BODY_WEIGHT_LB
        draft["exercises"].append({"name": name, "sets": sets})
        _save_draft(draft)
        return _render_draft(draft), html.Div(), None, ""

    if triggered == "wt-publish":
        draft = _load_draft()
        if not draft.get("exercises"):
            return (
                _render_draft(draft),
                html.Div("No exercises to publish.", style={"color": "#FF5252"}),
                no_update,
                no_update,
            )

        # Load existing data and append
        data = _load_data()
        data.append(draft)
        with open(WT_DATA_FILE, "w") as f:
            json.dump(data, f, indent=2)

        # Clear draft
        if os.path.exists(WT_DRAFT_FILE):
            os.remove(WT_DRAFT_FILE)

        empty_draft = {"date": date.today().isoformat(), "exercises": []}
        return (
            _render_draft(empty_draft),
            html.Div("Workout published!", style={"color": "#4CAF50"}),
            None,
            "",
        )

    if triggered == "wt-clear-draft":
        if os.path.exists(WT_DRAFT_FILE):
            os.remove(WT_DRAFT_FILE)
        empty_draft = {"date": date.today().isoformat(), "exercises": []}
        return (
            _render_draft(empty_draft),
            html.Div("Draft cleared.", style={"color": COLORS["muted"]}),
            None,
            "",
        )

    if triggered == "wt-clear-exercise":
        draft = _load_draft()
        return _render_draft(draft), html.Div(), None, ""

    return no_update, no_update, no_update, no_update
