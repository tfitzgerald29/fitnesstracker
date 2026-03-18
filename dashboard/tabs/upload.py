import base64

from dash import Input, Output, State, callback, dcc, html, no_update

from backend.FitFileProcessor import FitFileProcessor
from backend.storage import storage

from ..config import CARD_STYLE, COLORS

# ── Layout ─────────────────────────────────────────────────────────────────────


def upload_tab():
    return html.Div(
        [
            html.H2(
                "Upload Activity Files",
                style={
                    "color": COLORS["text"],
                    "marginBottom": "24px",
                    "fontSize": "1.3rem",
                },
            ),
            html.Div(
                style={**CARD_STYLE, "maxWidth": "600px"},
                children=[
                    dcc.Upload(
                        id="fit-upload",
                        children=html.Div(
                            [
                                "Drag and drop or ",
                                html.A(
                                    "select files",
                                    style={
                                        "color": COLORS["accent"],
                                        "cursor": "pointer",
                                    },
                                ),
                                html.Br(),
                                html.Span(
                                    "Accepts .fit and .zip files",
                                    style={
                                        "color": COLORS["muted"],
                                        "fontSize": "0.85rem",
                                    },
                                ),
                            ]
                        ),
                        style={
                            "width": "100%",
                            "height": "120px",
                            "lineHeight": "1.6",
                            "borderWidth": "2px",
                            "borderStyle": "dashed",
                            "borderRadius": "8px",
                            "borderColor": COLORS["border"],
                            "textAlign": "center",
                            "display": "flex",
                            "alignItems": "center",
                            "justifyContent": "center",
                            "color": COLORS["text"],
                            "cursor": "pointer",
                        },
                        multiple=True,
                        accept=".fit,.zip",
                    ),
                    html.Div(
                        id="upload-status",
                        style={"marginTop": "16px", "fontSize": "0.9rem"},
                    ),
                ],
            ),
        ]
    )


# ── Callback ───────────────────────────────────────────────────────────────────


@callback(
    Output("upload-status", "children"),
    Input("fit-upload", "contents"),
    State("fit-upload", "filename"),
    State("user-store", "data"),
    prevent_initial_call=True,
)
def handle_upload(contents_list, filenames, user_data):
    if not contents_list or not user_data or not user_data.get("user_id"):
        return no_update

    user_id = user_data["user_id"]
    fp = FitFileProcessor(
        mergedfiles_path=storage.merged_path(user_id),
        processedpath=storage.processed_path(user_id),
    )

    results = []
    for content, filename in zip(contents_list, filenames):
        # dcc.Upload delivers content as "data:<mime>;base64,<data>"
        _, b64 = content.split(",", 1)
        raw_bytes = base64.b64decode(b64)

        summary = fp.process_uploaded_file(filename, raw_bytes)
        processed = summary["new_files_processed"]
        errors = summary["processing_error_files"]
        schema_issues = summary["schema_mismatch_files"]

        # Invalidate parquet cache so next callback reads fresh data from S3
        if not errors and processed > 0:
            storage.invalidate_cache(user_id)

        if errors:
            results.append(
                html.Div(
                    f"{filename} — failed: {errors[0]['error']}",
                    style={"color": "#f44336"},
                )
            )
        elif schema_issues:
            results.append(
                html.Div(
                    f"{filename} — processed with schema warnings",
                    style={"color": "#FF9800"},
                )
            )
        else:
            results.append(
                html.Div(
                    f"{filename} — {processed} file(s) processed successfully",
                    style={"color": "#4CAF50"},
                )
            )

    return results
