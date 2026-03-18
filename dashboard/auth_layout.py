from dash import dcc, html

from .config import CARD_STYLE, COLORS

# ── Shared styles ──────────────────────────────────────────────────────────────
_INPUT_STYLE = {
    "width": "100%",
    "padding": "10px 12px",
    "backgroundColor": COLORS["bg"],
    "border": f"1px solid {COLORS['border']}",
    "borderRadius": "6px",
    "color": COLORS["text"],
    "fontSize": "0.95rem",
    "boxSizing": "border-box",
    "marginBottom": "12px",
}

_BTN_PRIMARY = {
    "width": "100%",
    "padding": "11px",
    "backgroundColor": COLORS["accent"],
    "color": "#fff",
    "border": "none",
    "borderRadius": "6px",
    "fontSize": "1rem",
    "fontWeight": "600",
    "cursor": "pointer",
    "marginBottom": "12px",
}

_BTN_SECONDARY = {
    **_BTN_PRIMARY,
    "backgroundColor": COLORS["card"],
    "border": f"1px solid {COLORS['border']}",
    "color": COLORS["text"],
}


# ── Layout ─────────────────────────────────────────────────────────────────────
def login_layout():
    return html.Div(
        style={
            "display": "flex",
            "justifyContent": "center",
            "alignItems": "center",
            "minHeight": "70vh",
        },
        children=[
            html.Div(
                style={
                    **CARD_STYLE,
                    "padding": "40px",
                    "maxWidth": "400px",
                    "width": "100%",
                },
                children=[
                    html.H2(
                        "Sign in",
                        style={
                            "color": COLORS["text"],
                            "marginBottom": "4px",
                            "fontSize": "1.4rem",
                            "textAlign": "center",
                        },
                    ),
                    html.P(
                        "Access your fitness dashboard.",
                        style={
                            "color": COLORS["muted"],
                            "marginBottom": "24px",
                            "fontSize": "0.9rem",
                            "textAlign": "center",
                        },
                    ),
                    # ── Email / password form ──────────────────────────────
                    dcc.Input(
                        id="auth-email",
                        type="email",
                        placeholder="Email",
                        debounce=False,
                        style=_INPUT_STYLE,
                    ),
                    dcc.Input(
                        id="auth-password",
                        type="password",
                        placeholder="Password",
                        debounce=False,
                        style=_INPUT_STYLE,
                    ),
                    # Error / success message
                    html.Div(
                        id="auth-message",
                        style={"marginBottom": "12px", "fontSize": "0.88rem"},
                    ),
                    html.Button(
                        "Sign in",
                        id="btn-signin",
                        n_clicks=0,
                        style=_BTN_PRIMARY,
                    ),
                    html.Button(
                        "Create account",
                        id="btn-signup",
                        n_clicks=0,
                        style=_BTN_SECONDARY,
                    ),
                ],
            )
        ],
    )
