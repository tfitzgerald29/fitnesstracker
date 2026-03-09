import os

_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MERGED_PATH = os.path.join(_BASE_DIR, "mergedfiles")
WT_DATA_FILE = os.path.join(_BASE_DIR, "weighttraining_data", "weighttraining_data.json")
WT_DRAFT_FILE = os.path.join(_BASE_DIR, "weighttraining_data", "draft_workout.json")
BODY_WEIGHT_LB = 133

COLORS = {
    "bg": "#0f1117",
    "card": "#1a1d27",
    "border": "#2a2d37",
    "text": "#e0e0e0",
    "muted": "#888",
    "accent": "#2196F3",
}

CARD_STYLE = {
    "backgroundColor": COLORS["card"],
    "border": f"1px solid {COLORS['border']}",
    "borderRadius": "8px",
    "padding": "16px",
    "marginBottom": "16px",
}
