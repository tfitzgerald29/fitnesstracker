import os
import threading
import webbrowser

import dash

from backend.FitFileProcessor import FitFileProcessor
from dashboard.layout import create_layout
import dashboard.callbacks  # noqa: F401 - registers tab router + imports dashboard.tabs

# ── FIT file ingestion ────────────────────────────────────────────────────────
# Run on startup to pick up any new .fit/.zip files dropped in ~/Downloads.
# Skipped on Plotly Cloud (source folder doesn't exist there).
#
# Reloader guard: Werkzeug's debug reloader spawns a child process and sets
# WERKZEUG_RUN_MAIN="true" in it.  The parent watcher process either leaves the
# variable unset or sets it to "".  We skip the pipeline unless we're the child
# ("true") or a production import (gunicorn imports app as a module, so
# __name__ != "__main__").
_in_reloader_parent = (
    __name__ == "__main__" and os.environ.get("WERKZEUG_RUN_MAIN") != "true"
)

_fp = FitFileProcessor()
if os.path.isdir(_fp.source_folder) and not _in_reloader_parent:
    _fp.run()

# ── Dash app ──────────────────────────────────────────────────────────────────
app = dash.Dash(__name__, suppress_callback_exceptions=True)
app.title = "Tyler's Activities"
app.layout = create_layout()

server = app.server  # required for Plotly Cloud / gunicorn

if __name__ == "__main__":
    PORT = 8051
    # Open the browser only in the child process (WERKZEUG_RUN_MAIN="true"),
    # not in the parent watcher, so it opens exactly once.
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        threading.Timer(
            1.5, lambda: webbrowser.open(f"http://localhost:{PORT}")
        ).start()
    app.run(debug=True, port=PORT)
