import json
import os
from datetime import date

import plotly.graph_objects as go
from dash import Input, Output, State, callback, ctx, dcc, html, no_update

from ..config import CARD_STYLE, COLORS, WT_DATA_FILE, WT_DRAFT_FILE


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
                style={"display": "flex", "alignItems": "center", "gap": "16px", "marginBottom": "4px"},
                children=[
                    html.Span(
                        ex["name"].replace("_", " ").title(),
                        style={"fontWeight": "500", "minWidth": "160px", "color": "#ccc"},
                    ),
                    html.Span(sets_text, style={"color": COLORS["muted"], "fontSize": "0.85rem"}),
                ],
            )
        )

    return html.Div([
        html.H3(draft["date"], style={"color": COLORS["accent"], "marginBottom": "12px", "fontSize": "0.95rem"}),
        html.Div(exercises),
    ])


# ── Layout ──────────────────────────────────────────────────

def _weights_log():
    """Existing workout log and progress charts."""
    data = _load_data()
    exercise_names = _get_exercise_names(data)
    sorted_data = sorted(data, key=lambda x: x["date"], reverse=True)

    comparison = html.Div(
        style=CARD_STYLE,
        children=[
            html.H3("Exercise Progress", style={"color": COLORS["accent"], "marginBottom": "12px", "fontSize": "0.95rem"}),
            dcc.Dropdown(
                id="exercise-select",
                options=[{"label": n.replace("_", " ").title(), "value": n} for n in exercise_names],
                value=exercise_names[0] if exercise_names else None,
                multi=True,
                placeholder="Select exercises to compare...",
                style={"backgroundColor": COLORS["card"], "marginBottom": "12px"},
            ),
            dcc.Graph(id="exercise-progress-chart"),
        ],
    )

    entries = []
    for entry in sorted_data:
        exercises = []
        for ex in entry["exercises"]:
            sets_text = " | ".join(
                f"{s['reps']}x{s['weight']}lb" if s["weight"] > 0 else f"{s['reps']}xBW"
                for s in ex["sets"]
            )
            exercises.append(
                html.Div(
                    style={"display": "flex", "alignItems": "center", "gap": "16px", "marginBottom": "4px"},
                    children=[
                        html.Span(
                            ex["name"].replace("_", " ").title(),
                            style={"fontWeight": "500", "minWidth": "160px", "color": "#ccc"},
                        ),
                        html.Span(sets_text, style={"color": COLORS["muted"], "fontSize": "0.85rem"}),
                    ],
                )
            )

        entries.append(
            html.Div(
                style=CARD_STYLE,
                children=[
                    html.H3(entry["date"], style={"color": COLORS["accent"], "marginBottom": "12px", "fontSize": "0.95rem"}),
                    html.Div(exercises),
                ],
            )
        )

    return html.Div([comparison, html.Div(entries)])


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

    return html.Div([
        # Date picker
        html.Div(
            style={"marginBottom": "16px"},
            children=[
                html.Label("Date", style={"color": COLORS["muted"], "fontSize": "0.8rem", "marginBottom": "4px", "display": "block"}),
                dcc.DatePickerSingle(
                    id="wt-date",
                    date=draft.get("date", date.today().isoformat()),
                    display_format="YYYY-MM-DD",
                    style={"width": "100%"},
                ),
            ],
        ),

        # Exercise input
        html.Div(
            style={**CARD_STYLE, "marginBottom": "16px"},
            children=[
                html.H3("Add Exercise", style={"color": COLORS["accent"], "marginBottom": "12px", "fontSize": "0.95rem"}),
                html.Label("Exercise", style={"color": COLORS["muted"], "fontSize": "0.8rem", "marginBottom": "4px", "display": "block"}),
                html.Div(
                    style={"display": "flex", "gap": "8px", "marginBottom": "12px"},
                    children=[
                        html.Div(style={"flex": "1"}, children=[
                            dcc.Dropdown(
                                id="wt-exercise-name",
                                options=[{"label": n.replace("_", " ").title(), "value": n} for n in exercise_names],
                                placeholder="Select existing...",
                                style={"color": "#000"},
                            ),
                        ]),
                        html.Div(style={"flex": "1"}, children=[
                            dcc.Input(
                                id="wt-new-exercise",
                                type="text",
                                placeholder="Or type new name...",
                                style=input_style,
                                debounce=True,
                            ),
                        ]),
                    ],
                ),

                # Sets input row
                html.Div(
                    style={"display": "flex", "gap": "8px", "alignItems": "flex-end", "marginBottom": "12px"},
                    children=[
                        html.Div(style={"flex": "1"}, children=[
                            html.Label("Weight (lb)", style={"color": COLORS["muted"], "fontSize": "0.8rem", "display": "block", "marginBottom": "4px"}),
                            dcc.Input(id="wt-weight", type="text", placeholder="135 or 130,150,150", style=input_style, debounce=True),
                        ]),
                        html.Div(style={"flex": "1"}, children=[
                            html.Label("Reps", style={"color": COLORS["muted"], "fontSize": "0.8rem", "display": "block", "marginBottom": "4px"}),
                            dcc.Input(id="wt-reps", type="text", placeholder="10 or 8,9,10", style=input_style, debounce=True),
                        ]),
                        html.Button("Add Set", id="wt-add-set", n_clicks=0, style=btn_style),
                    ],
                ),

                # Current sets display
                html.Div(id="wt-current-sets", style={"marginBottom": "12px"}),

                # Add exercise / clear exercise buttons
                html.Div(
                    style={"display": "flex", "gap": "8px"},
                    children=[
                        html.Button("Add Exercise to Workout", id="wt-add-exercise", n_clicks=0, style=btn_style),
                        html.Button("Clear Exercise", id="wt-clear-exercise", n_clicks=0, style=btn_secondary),
                    ],
                ),
            ],
        ),

        # Store for current exercise sets
        dcc.Store(id="wt-sets-store", data=[]),

        # Draft workout preview
        html.Div(
            style=CARD_STYLE,
            children=[
                html.H3("Current Workout", style={"color": COLORS["accent"], "marginBottom": "12px", "fontSize": "0.95rem"}),
                html.Div(id="wt-draft-preview", children=_render_draft(draft)),
                html.Div(
                    style={"display": "flex", "gap": "8px", "marginTop": "16px"},
                    children=[
                        html.Button("Publish Workout", id="wt-publish", n_clicks=0, style=btn_style),
                        html.Button("Clear Draft", id="wt-clear-draft", n_clicks=0, style=btn_secondary),
                    ],
                ),
                html.Div(id="wt-publish-status", style={"marginTop": "8px"}),
            ],
        ),
    ])


def weights_tab():
    return html.Div([
        dcc.Tabs(
            id="weights-subtabs",
            value="log",
            children=[
                dcc.Tab(
                    label="Log", value="log",
                    style={"padding": "6px 16px", "lineHeight": "28px"},
                    selected_style={"padding": "6px 16px", "lineHeight": "28px", "borderTop": f"2px solid {COLORS['accent']}"},
                ),
                dcc.Tab(
                    label="New Entry", value="entry",
                    style={"padding": "6px 16px", "lineHeight": "28px"},
                    selected_style={"padding": "6px 16px", "lineHeight": "28px", "borderTop": f"2px solid {COLORS['accent']}"},
                ),
            ],
            style={"height": "40px", "marginBottom": "16px"},
            colors={
                "border": "transparent",
                "primary": COLORS["accent"],
                "background": "transparent",
            },
        ),
        html.Div(id="weights-subtab-content"),
    ])


# ── Callbacks ───────────────────────────────────────────────

@callback(
    Output("weights-subtab-content", "children"),
    Input("weights-subtabs", "value"),
)
def render_weights_subtab(subtab):
    if subtab == "entry":
        return _weights_entry()
    return _weights_log()


@callback(
    Output("exercise-progress-chart", "figure"),
    Input("exercise-select", "value"),
)
def update_exercise_progress(selected):
    fig = go.Figure()

    if not selected:
        fig.update_layout(
            paper_bgcolor=COLORS["card"],
            plot_bgcolor=COLORS["card"],
            font_color=COLORS["text"],
            annotations=[{"text": "Select an exercise to see progress", "showarrow": False, "font": {"size": 14, "color": COLORS["muted"]}}],
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            height=350,
        )
        return fig

    if isinstance(selected, str):
        selected = [selected]

    data = _load_data()

    for exercise_name in selected:
        dates = []
        max_weights = []
        total_volumes = []

        for entry in sorted(data, key=lambda x: x["date"]):
            for ex in entry["exercises"]:
                if ex["name"] == exercise_name:
                    weights = [s["weight"] for s in ex["sets"]]
                    volume = sum(s["weight"] * s["reps"] for s in ex["sets"])
                    dates.append(entry["date"])
                    max_weights.append(max(weights))
                    total_volumes.append(volume)

        label = exercise_name.replace("_", " ").title()

        fig.add_trace(go.Scatter(
            x=dates, y=max_weights, name=f"{label} (max weight)",
            mode="lines+markers", line=dict(width=2),
        ))

        fig.add_trace(go.Bar(
            x=dates, y=total_volumes, name=f"{label} (volume)",
            opacity=0.3, yaxis="y2",
        ))

    fig.update_layout(
        paper_bgcolor=COLORS["card"],
        plot_bgcolor=COLORS["card"],
        font_color=COLORS["text"],
        xaxis=dict(title="Date", gridcolor=COLORS["border"]),
        yaxis=dict(title="Max Weight (lb)", gridcolor=COLORS["border"]),
        yaxis2=dict(title="Total Volume (lb x reps)", overlaying="y", side="right", showgrid=False),
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=400,
        margin=dict(t=40, b=50, l=60, r=60),
    )

    return fig


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
def handle_sets(add_set_clicks, add_exercise_clicks, clear_exercise_clicks, weight, reps, current_sets):
    triggered = ctx.triggered_id

    if triggered in ("wt-add-exercise", "wt-clear-exercise"):
        # Clear sets after adding or clearing exercise
        return [], html.Div(), "", ""

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
        return current_sets, _render_sets(current_sets), "", ""

    return no_update, no_update, no_update, no_update


def _render_sets(sets):
    if not sets:
        return html.Div()
    text = " | ".join(
        f"Set {s['set']}: {s['reps']}x{s['weight']}lb" if s["weight"] > 0 else f"Set {s['set']}: {s['reps']}xBW"
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
    prevent_initial_call=True,
)
def handle_workout(add_ex_clicks, publish_clicks, clear_clicks, clear_exercise_clicks, exercise_name, new_exercise, sets, workout_date):
    triggered = ctx.triggered_id

    if triggered == "wt-add-exercise":
        # New exercise name takes priority; normalize to snake_case
        name = new_exercise.strip().lower().replace(" ", "_") if new_exercise and new_exercise.strip() else exercise_name
        if not name or not sets:
            draft = _load_draft()
            return _render_draft(draft), html.Div("Please select an exercise and add at least one set.", style={"color": "#FF5252"}), exercise_name, new_exercise

        draft = _load_draft()
        draft["date"] = workout_date or date.today().isoformat()
        draft["exercises"].append({"name": name, "sets": sets})
        _save_draft(draft)
        return _render_draft(draft), html.Div(), None, ""

    if triggered == "wt-publish":
        draft = _load_draft()
        if not draft.get("exercises"):
            return _render_draft(draft), html.Div("No exercises to publish.", style={"color": "#FF5252"}), no_update, no_update

        # Load existing data and append
        data = _load_data()
        data.append(draft)
        with open(WT_DATA_FILE, "w") as f:
            json.dump(data, f, indent=2)

        # Clear draft
        if os.path.exists(WT_DRAFT_FILE):
            os.remove(WT_DRAFT_FILE)

        empty_draft = {"date": date.today().isoformat(), "exercises": []}
        return _render_draft(empty_draft), html.Div("Workout published!", style={"color": "#4CAF50"}), None, ""

    if triggered == "wt-clear-draft":
        if os.path.exists(WT_DRAFT_FILE):
            os.remove(WT_DRAFT_FILE)
        empty_draft = {"date": date.today().isoformat(), "exercises": []}
        return _render_draft(empty_draft), html.Div("Draft cleared.", style={"color": COLORS["muted"]}), None, ""

    if triggered == "wt-clear-exercise":
        draft = _load_draft()
        return _render_draft(draft), html.Div(), None, ""

    return no_update, no_update, no_update, no_update
