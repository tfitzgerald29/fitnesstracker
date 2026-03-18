import plotly.graph_objects as go
from dash import Input, Output, State, callback, dcc, html

from backend.cycling_processor import CyclingProcessor

from ..config import CARD_STYLE, COLORS, get_user_id

COVARIATE_LABELS = {
    "tss_per_100": "TSS (per 100 pts)",
    "sleep_score": "Sleep Score (prior night)",
}


def _covariate_label(name):
    return COVARIATE_LABELS.get(name, name)


# ── Layout ────────────────────────────────────────────────────────────────────


def cycling_covariate_layout():
    return html.Div(
        [
            html.H3(
                "Peak Power Covariate Analysis",
                style={
                    "color": COLORS["accent"],
                    "marginBottom": "12px",
                    "fontSize": "0.95rem",
                },
            ),
            dcc.RadioItems(
                id="cp-sleep-toggle",
                options=[
                    {"label": "Without sleep score", "value": "off"},
                    {"label": "With sleep score", "value": "on"},
                ],
                value="off",
                inline=True,
                labelStyle={
                    "marginRight": "12px",
                    "cursor": "pointer",
                    "color": COLORS["text"],
                },
                inputStyle={"marginRight": "4px"},
                style={"marginBottom": "12px"},
            ),
            html.Div(id="cp-covariate-results"),
            html.Div(dcc.Graph(id="cp-covariate-chart"), style=CARD_STYLE),
        ]
    )


# ── Callback ──────────────────────────────────────────────────────────────────


@callback(
    Output("cp-covariate-results", "children"),
    Output("cp-covariate-chart", "figure"),
    Input("cp-sleep-toggle", "value"),
    State("user-store", "data"),
)
def update_cp_covariates(sleep_toggle, user_data):
    include_sleep = sleep_toggle == "on"
    processor = CyclingProcessor(user_id=get_user_id(user_data))
    result = processor.cp_covariate_analysis(include_sleep=include_sleep)

    empty_fig = go.Figure()
    empty_fig.update_layout(
        template="plotly_dark",
        paper_bgcolor=COLORS["card"],
        plot_bgcolor=COLORS["card"],
    )

    if result["models"] is None:
        empty_fig.add_annotation(
            text="Not enough data for covariate analysis (need 5+ months)",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
            font=dict(color=COLORS["muted"]),
        )
        return html.Div(), empty_fig

    def _build_table(model_result, dep_var):
        cell_style = {"textAlign": "right", "padding": "4px 8px"}
        header = html.Tr(
            [
                html.Th("Covariate", style={"textAlign": "left", "padding": "4px 8px"}),
                html.Th("OLS", style=cell_style),
                html.Th("Median", style=cell_style),
                html.Th("Mean", style=cell_style),
                html.Th("95% CI (boot)", style=cell_style),
            ]
        )
        rows = []
        for c in model_result["coefficients"]:
            label = _covariate_label(c["name"])
            if c["name"] == "const":
                label = "Intercept (avg month)"
            median_val = c.get("coef_median")
            mean_val = c.get("coef_mean")
            ci_low = c["ci_low"]
            ci_high = c["ci_high"]
            crosses_zero = ci_low <= 0 <= ci_high
            sig_color = COLORS["muted"] if crosses_zero else COLORS["accent"]
            rows.append(
                html.Tr(
                    [
                        html.Td(label, style={"padding": "4px 8px"}),
                        html.Td(f"{c['coef']:.1f}", style=cell_style),
                        html.Td(
                            f"{median_val:.1f}" if median_val is not None else "-",
                            style=cell_style,
                        ),
                        html.Td(
                            f"{mean_val:.1f}" if mean_val is not None else "-",
                            style=cell_style,
                        ),
                        html.Td(
                            f"[{ci_low:.1f}, {ci_high:.1f}]",
                            style={
                                **cell_style,
                                "fontSize": "0.8rem",
                                "color": sig_color,
                            },
                        ),
                    ]
                )
            )

        n_boot = model_result.get("n_bootstrap", "")
        boot_label = f"  boot={n_boot}" if n_boot else ""
        r2_text = (
            f"R\u00b2={model_result['r2']:.3f}  "
            f"Adj R\u00b2={model_result['r2_adj']:.3f}  "
            f"n={model_result['n']}{boot_label}"
        )

        return html.Div(
            [
                html.Div(
                    f"Peak {dep_var}",
                    style={
                        "fontWeight": "bold",
                        "color": COLORS["accent"],
                        "marginBottom": "4px",
                    },
                ),
                html.Div(
                    r2_text,
                    style={
                        "fontSize": "0.8rem",
                        "color": COLORS["muted"],
                        "marginBottom": "6px",
                    },
                ),
                html.Table(
                    [html.Thead(header), html.Tbody(rows)],
                    style={
                        "fontSize": "0.85rem",
                        "color": COLORS["text"],
                        "width": "100%",
                    },
                ),
            ],
            style={"flex": "1", **CARD_STYLE},
        )

    tables = [_build_table(mod, label) for label, mod in result["models"].items()]

    summary = html.Div(
        style={"display": "flex", "gap": "12px", "marginBottom": "12px"},
        children=tables,
    )

    # Correlation heatmap
    corr = result["correlation"]
    labels = [_covariate_label(c) for c in corr["columns"]]

    fig = go.Figure(
        data=go.Heatmap(
            z=corr["values"],
            x=labels,
            y=labels,
            colorscale="RdBu_r",
            zmid=0,
            zmin=-1,
            zmax=1,
            text=[[f"{v:.2f}" for v in row] for row in corr["values"]],
            texttemplate="%{text}",
            textfont={"size": 11},
            hovertemplate="%{x} vs %{y}: %{z:.3f}<extra></extra>",
        )
    )

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor=COLORS["card"],
        plot_bgcolor=COLORS["card"],
        margin=dict(l=120, r=20, t=30, b=80),
        height=350,
        title=dict(text="Correlation Matrix", font=dict(size=13)),
        xaxis=dict(tickangle=-35),
    )

    return summary, fig
