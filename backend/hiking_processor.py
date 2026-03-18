"""
Read-only query layer for hiking sessions.

Does not inherit from FitFileProcessor — FIT ingestion is handled
separately at startup via FitFileProcessor.run() in app.py.
"""

import os

import polars as pl

from .schemas import load_sessions
from .storage import storage


class HikingProcessor:
    def __init__(self, mergedfiles_path=None, user_id=None) -> None:
        self.mergedfiles_path = mergedfiles_path or storage.merged_path(user_id)
        self.hiking = self._load_hiking_sessions()

    def _load_hiking_sessions(self) -> pl.DataFrame:
        parquet_path = storage.path_join(self.mergedfiles_path, "session_mesgs.parquet")
        df = load_sessions("hiking", parquet_path)
        if df.is_empty():
            return df
        ts_col = "timestamp"
        if df[ts_col].dtype.time_zone is None:
            df = df.with_columns(pl.col(ts_col).dt.replace_time_zone("UTC"))
        return df.with_columns(
            pl.col(ts_col).dt.convert_time_zone("America/Denver").alias(ts_col)
        )

    def list_hikes(self) -> list[dict]:
        """Return hikes sorted most-recent-first for a dropdown.

        Trimmed to the last year to match the record_mesgs window so every
        hike shown has route/elevation detail data available.
        """
        from datetime import date, timedelta

        cutoff = date.today() - timedelta(days=182)
        df = self.hiking.filter(pl.col("timestamp").dt.date() >= cutoff)
        df = df.sort("timestamp", descending=True)
        result = []
        for r in df.to_dicts():
            dt = r["timestamp"]
            dist_mi = (
                round(r["total_distance"] / 1609.344, 1)
                if r.get("total_distance")
                else 0
            )
            label = f"{dt.strftime('%Y-%m-%d')} — {dist_mi} mi"
            result.append({"label": label, "value": r["source_file"]})
        return result

    def summary_stats(self) -> dict:
        """High-level totals across all hikes."""
        df = self.hiking
        if df.is_empty():
            return {}
        return {
            "total_hikes": len(df),
            "total_miles": round(df["total_distance"].sum() / 1609.344, 1),
            "total_hours": round(df["total_timer_time"].sum() / 3600, 1),
            "total_ascent_ft": round((df["total_ascent"].drop_nulls().sum()) * 3.28084),
        }

    def get_hike_route(self, source_file: str) -> dict:
        """Return GPS route data for a hike.

        Returns ``{lat, lon, elevation_ft, heart_rate}``.
        lat/lon are converted from FIT semicircles to decimal degrees.
        elevation_ft is ``enhanced_altitude`` converted from metres to feet.
        heart_rate is the raw bpm series (None where missing).
        """
        records_path = storage.path_join(self.mergedfiles_path, "record_mesgs.parquet")
        empty: dict = {"lat": [], "lon": [], "elevation_ft": [], "heart_rate": []}
        if not storage.path_exists(records_path):
            return empty

        SEMICIRCLES_TO_DEGREES = 180.0 / (2**31)

        records = (
            storage.read_parquet(
                records_path,
                columns=[
                    "source_file",
                    "timestamp",
                    "position_lat",
                    "position_long",
                    "enhanced_altitude",
                    "heart_rate",
                ],
            )
            .filter(pl.col("source_file") == source_file)
            .sort("timestamp")
        )

        if records.is_empty():
            return empty

        gps = records.filter(pl.col("position_lat").is_not_null())
        if gps.is_empty():
            return empty

        lat = (gps["position_lat"] * SEMICIRCLES_TO_DEGREES).to_list()
        lon = (gps["position_long"] * SEMICIRCLES_TO_DEGREES).to_list()
        elevation_ft = (
            (gps["enhanced_altitude"] * 3.28084).to_list()
            if "enhanced_altitude" in gps.columns
            else [None] * len(lat)
        )
        heart_rate = (
            gps["heart_rate"].to_list()
            if "heart_rate" in gps.columns
            else [None] * len(lat)
        )

        return {
            "lat": lat,
            "lon": lon,
            "elevation_ft": elevation_ft,
            "heart_rate": heart_rate,
        }

    def monthly_summary(self) -> pl.DataFrame:
        """Hikes grouped by month: count, miles, hours, ascent."""
        df = self.hiking.with_columns(
            pl.col("timestamp").dt.strftime("%Y-%m").alias("month")
        )
        return (
            df.group_by("month")
            .agg(
                pl.len().alias("hikes"),
                (pl.col("total_distance").sum() / 1609.344).round(1).alias("miles"),
                (pl.col("total_timer_time").sum() / 3600).round(1).alias("hours"),
                (pl.col("total_ascent").sum() * 3.28084).round(0).alias("ascent_ft"),
            )
            .sort("month")
        )
