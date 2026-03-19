import os
import threading
import webbrowser

import dash

from backend.FitFileProcessor import FitFileProcessor
from backend.sleep_processor import SleepProcessor
from backend.storage import storage
from dashboard.layout import create_layout
import dashboard.callbacks  # noqa: F401 - registers tab router + imports dashboard.tabs

# ── Dash app ──────────────────────────────────────────────────────────────────
app = dash.Dash(
    __name__,
    suppress_callback_exceptions=True,
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)
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
    _in_reloader_parent = (
        __name__ == "__main__" and os.environ.get("WERKZEUG_RUN_MAIN") != "true"
    )
    _fp = FitFileProcessor()
    if os.path.isdir(_fp.source_folder) and not _in_reloader_parent:
        _fp.run()

    _sp = SleepProcessor()
    if os.path.isdir(_sp.source_folder) and not _in_reloader_parent:
        _sp.run()

    if os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        threading.Timer(
            2.0, lambda: webbrowser.open(f"http://localhost:{PORT}")
        ).start()
    app.run(debug=True, port=PORT)
