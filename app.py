import os
import threading
import webbrowser

import dash

from backend.storage import storage
from dashboard.layout import create_layout
import dashboard.callbacks  # noqa: F401 - registers tab router + imports dashboard.tabs

# ── FIT file ingestion ────────────────────────────────────────────────────────
# Local mode only: run on startup to pick up any new .fit/.zip files dropped
# in ~/Downloads. In cloud (S3) mode, ingestion is triggered per-user via the
# upload tab — there is no shared source folder to scan.
#
# FitFileProcessor and SleepProcessor are imported lazily here so that
# garmin_fit_sdk and other heavy local-only dependencies are never loaded
# in S3/cloud mode.
#
# Reloader guard: Werkzeug's debug reloader spawns a child process and sets
# WERKZEUG_RUN_MAIN="true" in it.  The parent watcher process either leaves the
# variable unset or sets it to "".  We skip the pipeline unless we're the child
# ("true") or a production import (gunicorn imports app as a module, so
# __name__ != "__main__").
_in_reloader_parent = (
    __name__ == "__main__" and os.environ.get("WERKZEUG_RUN_MAIN") != "true"
)

if not storage.is_s3():
    from backend.FitFileProcessor import FitFileProcessor
    from backend.sleep_processor import SleepProcessor

    _fp = FitFileProcessor()
    if os.path.isdir(_fp.source_folder) and not _in_reloader_parent:
        _fp.run()

    _sp = SleepProcessor()
    if os.path.isdir(_sp.source_folder) and not _in_reloader_parent:
        _sp.run()

# ── Dash app ──────────────────────────────────────────────────────────────────
app = dash.Dash(__name__, suppress_callback_exceptions=True)
app.title = "Tyler's Activities"
app.layout = create_layout()

server = app.server  # required for Plotly Cloud / gunicorn

PORT = 8051


# Open the browser only on the very first startup, not on every hot-reload.
# In debug mode Werkzeug spawns a reloader child and sets WERKZEUG_RUN_MAIN="true".
# We open the browser from the parent process (where the env var is NOT set) using
# a timer long enough for the child to bind the port.  The reloader child never
# opens the browser, so reloads don't trigger extra tabs.
if __name__ == "__main__":
    if os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        threading.Timer(
            2.0, lambda: webbrowser.open(f"http://localhost:{PORT}")
        ).start()
    app.run(debug=True, port=PORT)
