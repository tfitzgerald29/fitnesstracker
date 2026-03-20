import os

from backend.storage import storage

_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MERGED_PATH = storage.merged_path()
WT_DATA_FILE = storage.wt_data_file()
WT_DRAFT_FILE = storage.wt_draft_file()
BODY_WEIGHT_LB = 133


def get_user_id(*args) -> str | None:
    """Local mode only: always returns None."""
    return None


COLORS = {
    "bg": "#0f1117",
    "card": "#1a1d27",
    "border": "#2a2d37",
    "text": "#e0e0e0",
    "muted": "#888",
    "accent": "#2196F3",
    "bk": "#000000",
}

CARD_STYLE = {
    "backgroundColor": COLORS["card"],
    "border": f"1px solid {COLORS['border']}",
    "borderRadius": "8px",
    "padding": "16px",
    "marginBottom": "16px",
}
