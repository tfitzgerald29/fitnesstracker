"""
backend.schemas — per-sport schema definitions and safe parquet loaders.

Public API
----------
load_sessions(sport, parquet_path)        → pl.DataFrame
load_records(sport, parquet_path, ...)    → pl.DataFrame
load_splits(parquet_path, ...)            → pl.DataFrame
load_split_summaries(parquet_path, ...)   → pl.DataFrame

INGEST_COLUMNS: dict[msg_type → list[str]]
    Column padding lists for use in FitFileProcessor.  Replaces
    expected_columns.json as the single source of truth.  Each list is the
    union of all sport schemas for that message type, minus ``source_file``
    (which is pipeline-injected, not a FIT field).

Schema dicts (column → polars dtype)
-------------------------------------
from backend.schemas import cycling, skiing, climbing, running, hiking, sleep
cycling.SESSION   # session_mesgs schema for cycling
cycling.RECORD    # record_mesgs schema for cycling

from backend.schemas.base import SESSION_BASE, RECORD_BASE, SPLIT_BASE
"""

from . import climbing, cycling, hiking, running, skiing, sleep
from .base import RECORD_BASE, SPLIT_BASE, SPLIT_SUMMARY_BASE
from .loader import load_records, load_sessions, load_split_summaries, load_splits

# ── Ingest column lists (write-time row padding) ──────────────────────────────
# Union of all sport schemas for each message type, excluding the
# pipeline-injected ``source_file`` column (not a FIT field).


def _union_cols(*schema_dicts) -> list[str]:
    """Return sorted union of column names across all provided schema dicts,
    excluding ``source_file``."""
    cols: set[str] = set()
    for d in schema_dicts:
        cols.update(d.keys())
    cols.discard("source_file")
    return sorted(cols)


INGEST_COLUMNS: dict[str, list[str]] = {
    # Device metadata — stable Garmin fields, no sport schema needed
    "file_id_mesgs": [
        "garmin_product",
        "manufacturer",
        "number",
        "product",
        "serial_number",
        "time_created",
        "type",
    ],
    "activity_mesgs": [
        "event",
        "event_group",
        "event_type",
        "local_timestamp",
        "num_sessions",
        "timestamp",
        "total_timer_time",
        "type",
    ],
    # Sport session data — union of all per-sport SESSION schemas
    "session_mesgs": _union_cols(
        cycling.SESSION,
        skiing.SESSION,
        climbing.SESSION,
        running.SESSION,
        hiking.SESSION,
    ),
    # Record (per-second) data — union of all per-sport RECORD schemas
    "record_mesgs": _union_cols(
        cycling.RECORD,
        running.RECORD,
        hiking.RECORD,
        RECORD_BASE,
    ),
    "split_mesgs": _union_cols(SPLIT_BASE),
    "split_summary_mesgs": _union_cols(SPLIT_SUMMARY_BASE),
}

__all__ = [
    "load_sessions",
    "load_records",
    "load_splits",
    "load_split_summaries",
    "INGEST_COLUMNS",
    "sleep",
]
