import dash

from dashboard.layout import create_layout
import dashboard.callbacks  # noqa: F401 - registers tab router
import dashboard.tabs  # noqa: F401 - registers tab-specific callbacks

app = dash.Dash(__name__, suppress_callback_exceptions=True)
app.title = "Tyler's Activities"
app.layout = create_layout()

if __name__ == "__main__":
    app.run(debug=True, port=8050)
