"""
Schema for merged sleep rows in mergedfiles/sleep.parquet.

This schema is shared by both Garmin JSON ingestion (*_sleepData.json) and
the key/value Sleep.csv import path.
"""

import polars as pl

SLEEP: dict[str, pl.DataType] = {
    "calendar_date": pl.Utf8,
    "sleep_start_gmt": pl.Utf8,
    "sleep_end_gmt": pl.Utf8,
    "deep_sec": pl.Int64,
    "light_sec": pl.Int64,
    "rem_sec": pl.Int64,
    "awake_sec": pl.Int64,
    "total_sleep_sec": pl.Int64,
    "total_in_bed_sec": pl.Int64,
    "sleep_efficiency_pct": pl.Float64,
    "deep_hrs": pl.Float64,
    "light_hrs": pl.Float64,
    "rem_hrs": pl.Float64,
    "awake_hrs": pl.Float64,
    "total_sleep_hrs": pl.Float64,
    "avg_spo2": pl.Float64,
    "lowest_spo2": pl.Int32,
    "avg_hr": pl.Float64,
    "avg_respiration": pl.Float64,
    "lowest_respiration": pl.Float64,
    "highest_respiration": pl.Float64,
    "awake_count": pl.Int64,
    "restless_moments": pl.Int64,
    "avg_sleep_stress": pl.Float64,
    "score_overall": pl.Int32,
    "score_quality": pl.Int32,
    "score_duration": pl.Int32,
    "score_recovery": pl.Int32,
    "score_deep": pl.Int32,
    "score_rem": pl.Int32,
    "feedback": pl.Utf8,
}
