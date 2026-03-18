import os

import polars as pl

from .schemas import load_sessions
from .storage import storage


class skiing:
    """Read-only query layer for alpine skiing sessions.

    Does not inherit from FitFileProcessor — FIT ingestion is handled
    separately at startup via FitFileProcessor.run() in app.py.
    """

    def __init__(self, mergedfiles_path=None, user_id=None) -> None:
        self.mergedfiles_path = mergedfiles_path or storage.merged_path(user_id)
        self.skiing = self.load_skiing_data()

    # ── Data loading ──────────────────────────────────────────────────────────

    def load_skiing_data(self) -> pl.DataFrame:
        parquet_path = storage.path_join(self.mergedfiles_path, "session_mesgs.parquet")
        df = load_sessions("skiing", parquet_path)
        if df.is_empty():
            return df
        ts_col = "timestamp"
        if df[ts_col].dtype.time_zone is None:
            df = df.with_columns(pl.col(ts_col).dt.replace_time_zone("UTC"))
        return df.with_columns(
            pl.col(ts_col)
            .dt.convert_time_zone("America/Denver")
            .dt.date()
            .alias("DT_DENVER")
        )

    # ── Season helpers ─────────────────────────────────────────────────────────

    def _with_season(self) -> pl.DataFrame:
        """Return the skiing DataFrame with a ``season`` string column added."""
        return self.skiing.with_columns(
            pl.when(pl.col("DT_DENVER").dt.month() >= 10)
            .then(pl.col("DT_DENVER").dt.year())
            .otherwise(pl.col("DT_DENVER").dt.year() - 1)
            .alias("_season_start")
        ).with_columns(
            (
                pl.col("_season_start").cast(pl.Utf8)
                + "-"
                + (pl.col("_season_start") + 1).cast(pl.Utf8).str.slice(2)
            ).alias("season")
        )

    # ── Summary / listing methods ─────────────────────────────────────────────

    def summary_stats(self) -> dict:
        """Overall totals across all seasons."""
        df = self._with_season()
        if df.is_empty():
            return {}
        seasons = df["season"].n_unique()
        total_days = df["DT_DENVER"].n_unique()
        total_laps = int(df["num_laps"].drop_nulls().sum())
        total_descent_ft = round(df["total_descent"].drop_nulls().sum() * 3.28084)
        max_speed_mph = round(df["enhanced_max_speed"].drop_nulls().max() * 2.23694, 1)
        return {
            "total_days": total_days,
            "total_seasons": seasons,
            "total_descent_ft": total_descent_ft,
            "total_laps": total_laps,
            "max_speed_mph": max_speed_mph,
        }

    def list_sessions(self) -> list[dict]:
        """All sessions sorted most-recent-first, with season tag for filtering.

        Trimmed to the last year to match the record_mesgs window so every
        session shown has route/GPS detail data available.
        """
        from datetime import date, timedelta

        cutoff = date.today() - timedelta(days=365)
        df = self._with_season().filter(pl.col("DT_DENVER") >= cutoff)
        df = df.sort("DT_DENVER", descending=True)
        result = []
        for r in df.to_dicts():
            laps = int(r["num_laps"]) if r.get("num_laps") else 0
            descent = (
                f"{int(r['total_descent'] * 3.28084):,} ft"
                if r.get("total_descent")
                else "—"
            )
            profile = r.get("sport_profile_name") or "Ski"
            label = f"{r['DT_DENVER']} — {profile} — {laps} laps — {descent}"
            result.append(
                {
                    "label": label,
                    "value": r["source_file"],
                    "season": r["season"],
                }
            )
        return result

    def get_ski_route(self, source_file: str) -> dict:
        """Return GPS route data for a single ski session.

        Returns ``{lat, lon, elevation_ft, speed_mph, heart_rate}``.
        lat/lon are converted from FIT semicircles to decimal degrees.
        elevation_ft = enhanced_altitude × 3.28084
        speed_mph    = enhanced_speed   × 2.23694
        """
        records_path = storage.path_join(self.mergedfiles_path, "record_mesgs.parquet")
        empty: dict = {
            "lat": [],
            "lon": [],
            "elevation_ft": [],
            "speed_mph": [],
            "heart_rate": [],
        }
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
                    "enhanced_speed",
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
        speed_mph = (
            (gps["enhanced_speed"] * 2.23694).to_list()
            if "enhanced_speed" in gps.columns
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
            "speed_mph": speed_mph,
            "heart_rate": heart_rate,
        }

    # ── Legacy aggregation methods (kept for reference) ───────────────────────

    @staticmethod
    def _fmt_ride_time(seconds):
        if seconds is None:
            return None
        s = int(seconds)
        h, rem = divmod(s, 3600)
        m, sec = divmod(rem, 60)
        if h:
            return f"{h}h {m}m {sec}s"
        return f"{m}m {sec}s"

    def run_summary(self):
        df = self.skiing.group_by("DT_DENVER").agg(
            pl.col("avg_heart_rate").mean().round(0).alias("avg_heart_rate"),
            pl.col("max_heart_rate").max().alias("max_heart_rate"),
            pl.col("total_moving_time").sum().alias("total_moving_time"),
            pl.col("total_elapsed_time").sum().alias("total_elapsed_time"),
            pl.col("total_distance").sum().round(0).alias("total_distance"),
            pl.col("avg_speed").mean().round(2).alias("avg_speed"),
            pl.col("max_speed").max().round(2).alias("max_speed"),
            pl.col("total_ascent").sum().round(0).alias("total_ascent"),
            pl.col("total_descent").sum().round(0).alias("total_descent"),
            pl.col("num_laps").sum().alias("num_laps"),
            pl.col("enhanced_max_speed").max().round(2).alias("enhanced_max_speed"),
            pl.col("enhanced_avg_speed").mean().round(2).alias("enhanced_avg_speed"),
        )
        return df.sort("DT_DENVER", descending=True)

    def annual_summary(self):
        df = self._with_season()
        result = df.group_by("season").agg(
            pl.col("DT_DENVER").min().alias("first_day"),
            pl.col("DT_DENVER").max().alias("last_day"),
            pl.col("DT_DENVER").n_unique().alias("total_days"),
            pl.col("DT_DENVER")
            .filter(pl.col("sport_profile_name") == "Ski")
            .n_unique()
            .alias("ski_days"),
            pl.col("DT_DENVER")
            .filter(pl.col("sport_profile_name") == "Backcountry Ski")
            .n_unique()
            .alias("bc_days"),
            pl.col("avg_heart_rate").mean().round(0).alias("avg_heart_rate"),
            pl.col("max_heart_rate").max().alias("max_heart_rate"),
            pl.col("total_moving_time").sum().alias("total_moving_time"),
            pl.col("total_elapsed_time").sum().alias("total_elapsed_time"),
            pl.col("total_distance").sum().round(0).alias("total_distance"),
            pl.col("enhanced_max_speed").max().round(2).alias("max_speed"),
            pl.col("total_ascent").sum().round(0).alias("total_ascent"),
            pl.col("total_descent").sum().round(0).alias("total_descent"),
            pl.col("num_laps").sum().alias("num_laps"),
        )
        return result.sort("season", descending=True)
