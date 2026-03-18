import os

import numpy as np
import polars as pl

from .mixins import (
    CpModelMixin,
    PowerAnalysisMixin,
    RouteAnalysisMixin,
    TrainingLoadMixin,
)
from .schemas import load_records, load_sessions
from .storage import storage


class CyclingProcessor(
    PowerAnalysisMixin,
    RouteAnalysisMixin,
    CpModelMixin,
    TrainingLoadMixin,
):
    """Query and analyse cycling activities from the merged parquet files.

    This class is a *read-only* query layer.  It does not inherit from
    FitFileProcessor — FIT file ingestion is handled separately at startup
    via ``FitFileProcessor.run()`` in app.py.
    """

    def __init__(self, mergedfiles_path=None, user_id=None):
        self.mergedfiles_path = mergedfiles_path or storage.merged_path(user_id)
        self.cycling = self._load_cycling_sessions()
        self._update_power_curve_cache()
        self._update_bootstrap_cache()

    # ── Data loading ──────────────────────────────────────────────────────

    def _load_cycling_sessions(self) -> pl.DataFrame:
        """Load session_mesgs parquet and filter to cycling activities."""
        parquet_path = storage.path_join(self.mergedfiles_path, "session_mesgs.parquet")
        return load_sessions("cycling", parquet_path)

    def _update_power_curve_cache(self):
        """Compute and cache per-ride best power for each duration in CURVE_DURATIONS.

        Stores power_curves.parquet with columns: source_file, d_120, d_121, ..., d_1200.
        Only computes for rides not already in the cache.
        """
        cache_path = storage.path_join(self.mergedfiles_path, "power_curves.parquet")
        records_path = storage.path_join(self.mergedfiles_path, "record_mesgs.parquet")

        # If power_curves.parquet is already warm in the parquet cache, the
        # previous CyclingProcessor instantiation already verified that all
        # rides are present — skip the S3 stat calls entirely.
        if storage.is_cached(cache_path):
            return

        if not storage.path_exists(records_path):
            return

        # All cycling source files
        all_cycling_files = (
            set(self.cycling["source_file"].unique().to_list())
            if not self.cycling.is_empty()
            else set()
        )
        if not all_cycling_files:
            return

        # Load existing cache
        cached_files = set()
        existing_cache = None
        if storage.path_exists(cache_path):
            existing_cache = storage.read_parquet(cache_path)
            cached_files = set(existing_cache["source_file"].unique().to_list())

        new_files = all_cycling_files - cached_files
        if not new_files:
            return

        # Read records for new files only
        records = load_records(
            "cycling",
            records_path,
            source_files=list(new_files),
            columns=["source_file", "power", "timestamp"],
        ).sort("source_file", "timestamp")

        if records.is_empty():
            return

        rows = []
        for _, group in records.group_by("source_file"):
            sf = group["source_file"][0]
            power_series = group["power"]
            if power_series.drop_nulls().len() == 0:
                continue
            power = (
                power_series.fill_null(
                    (power_series.shift(1) + power_series.shift(2)) / 2
                )
                .fill_null(0)
                .cast(pl.Int64)
                .to_list()
            )
            ride_best = self._best_power_for_durations(power, self.CURVE_DURATIONS)
            row = {"source_file": sf}
            for d in self.CURVE_DURATIONS:
                row[f"d_{d}"] = ride_best.get(d)
            rows.append(row)

        if not rows:
            return

        new_cache = pl.DataFrame(rows)
        if existing_cache is not None:
            new_cache = pl.concat([existing_cache, new_cache], how="diagonal_relaxed")

        storage.write_parquet(new_cache, cache_path)
        print(f"  Power curve cache: added {len(rows)} rides (total: {len(new_cache)})")

    def _update_bootstrap_cache(self):
        """Refresh the CP covariate bootstrap cache if data has changed."""
        boot_path = storage.path_join(
            self.mergedfiles_path, "cp_covariate_bootstrap.json"
        )
        curves_path = storage.path_join(self.mergedfiles_path, "power_curves.parquet")

        # If power_curves.parquet is already warm in the parquet cache, the
        # bootstrap staleness check was already done this session — skip the
        # S3 path_exists / path_mtime API calls.
        if storage.is_cached(curves_path):
            return

        # Re-run bootstrap if cache is missing or older than power curves
        if storage.path_exists(curves_path) and (
            not storage.path_exists(boot_path)
            or storage.path_mtime(boot_path) < storage.path_mtime(curves_path)
        ):
            print("  Refreshing CP covariate bootstrap cache...")
            self.refresh_cp_covariate_bootstrap()

    # ── Ride listing & summary ────────────────────────────────────────────

    def list_rides(self) -> list[dict]:
        """Return a list of rides with label and timestamp for dropdown selection.

        Only rides within the last year are included — this matches the
        record_mesgs.parquet window so every ride shown has detail data
        (elevation, power, route) available.
        """
        from datetime import date, timedelta

        df = self.cycling.clone()
        if df.is_empty():
            return []

        ts_col = "timestamp"
        # Trim to the same 1-year window used for record_mesgs so the rides
        # dropdown never shows a ride whose detail data has been filtered out.
        cutoff = date.today() - timedelta(days=182)
        df = df.filter(pl.col(ts_col).dt.date() >= cutoff)
        if df.is_empty():
            return []

        if df[ts_col].dtype.time_zone is None:
            df = df.with_columns(pl.col(ts_col).dt.replace_time_zone("UTC"))
        df = df.with_columns(pl.col(ts_col).dt.convert_time_zone("America/Denver"))

        df = df.with_columns(
            (pl.col("total_distance") / 1609.344).round(1).alias("miles"),
            (pl.col("total_timer_time") / 3600).round(1).alias("hours"),
        ).sort(ts_col, descending=True)

        rides = []
        for r in df.to_dicts():
            dt = r[ts_col]
            label = f"{dt.strftime('%Y-%m-%d')} — {r['miles']} mi, {r['hours']} hr"
            rides.append({"label": label, "value": dt.isoformat()})
        return rides

    def get_ride_summary(self, ride_timestamp: str) -> dict | None:
        """Return summary stats for a single ride identified by its timestamp."""
        from datetime import datetime
        from zoneinfo import ZoneInfo

        dt = datetime.fromisoformat(ride_timestamp).astimezone(
            ZoneInfo("America/Denver")
        )

        df = self.cycling.clone()
        ts_col = "timestamp"
        if df[ts_col].dtype.time_zone is None:
            df = df.with_columns(pl.col(ts_col).dt.replace_time_zone("UTC"))
        df = df.with_columns(pl.col(ts_col).dt.convert_time_zone("America/Denver"))

        ride = df.filter(
            pl.col(ts_col) == pl.lit(dt).cast(pl.Datetime("us", "America/Denver"))
        )
        if ride.is_empty():
            return None

        r = ride.to_dicts()[0]

        avg_power = r.get("avg_power")
        normalized_power = r.get("normalized_power")
        avg_cadence = r.get("avg_cadence")
        threshold_power = r.get("threshold_power")
        left_right_balance = r.get("left_right_balance")

        return {
            "date": r[ts_col].strftime("%Y-%m-%d %I:%M %p"),
            "distance_mi": round(r["total_distance"] / 1609.344, 1),
            "duration_hr": round(r["total_timer_time"] / 3600, 2),
            "total_timer_time_s": r["total_timer_time"],
            "elapsed_hr": round(r["total_elapsed_time"] / 3600, 2),
            "avg_power": int(avg_power) if avg_power is not None else None,
            "normalized_power": int(normalized_power)
            if normalized_power is not None
            else None,
            "avg_speed_mph": round(r["enhanced_avg_speed"] * 2.23694, 1)
            if r.get("enhanced_avg_speed")
            else None,
            "avg_cadence": int(avg_cadence) if avg_cadence is not None else None,
            "avg_hr": r.get("avg_heart_rate"),
            "max_hr": r.get("max_heart_rate"),
            "total_ascent_ft": f"{round(r['total_ascent'] * 3.28084):,.0f}"
            if r.get("total_ascent")
            else None,
            "total_descent_ft": round(r["total_descent"] * 3.28084)
            if r.get("total_descent")
            else None,
            "calories": f"{r['total_calories']:,.0f}"
            if r.get("total_calories")
            else None,
            "tss": round(r["training_stress_score"])
            if r.get("training_stress_score")
            else None,
            "intensity_factor": r.get("intensity_factor"),
            "ftp": int(threshold_power) if threshold_power is not None else None,
            "work_kj": f"{round(r['total_work'] / 1000, 0):,.0f}"
            if r.get("total_work")
            else None,
            "left_balance": round(100 - (int(left_right_balance) & 0x3FFF) / 100, 1)
            if left_right_balance is not None
            else None,
            "right_balance": round((int(left_right_balance) & 0x3FFF) / 100, 1)
            if left_right_balance is not None
            else None,
            "source_file": r.get("source_file"),
        }

    # ── Shared utilities ──────────────────────────────────────────────────

    def _load_ride_power(self, source_file: str) -> list[int] | None:
        """Load and clean power data for a single ride from record_mesgs."""
        records_path = storage.path_join(self.mergedfiles_path, "record_mesgs.parquet")

        records = load_records(
            "cycling",
            records_path,
            source_files=[source_file],
            columns=["source_file", "power", "timestamp"],
        ).sort("timestamp")

        if records.is_empty() or "power" not in records.columns:
            return None
        if records["power"].drop_nulls().len() == 0:
            return None

        return (
            records.with_columns(
                pl.col("power")
                .fill_null((pl.col("power").shift(1) + pl.col("power").shift(2)) / 2)
                .fill_null(0)
            )["power"]
            .cast(pl.Int64)
            .to_list()
        )

    @staticmethod
    def _best_avg_power(power: list[int], window: int) -> int | None:
        """Compute best average power for a given window size using sliding window."""
        if len(power) < window:
            return None
        running_sum = sum(power[:window])
        best = running_sum
        for i in range(window, len(power)):
            running_sum += power[i] - power[i - window]
            if running_sum > best:
                best = running_sum
        return round(best / window)

    @staticmethod
    def _best_power_for_durations(power, durations):
        """Compute max average power for each duration in one pass."""
        power = np.array(power, dtype=float)
        if len(power) == 0:
            return {}

        csum = np.cumsum(np.insert(power, 0, 0))
        best = {}

        for d in durations:
            if d > len(power):
                continue
            avg = (csum[d:] - csum[:-d]) / d
            best[d] = float(avg.max())  # keep float for regression
        return best

    @staticmethod
    def _rolling_avg(values: list[float], window: int) -> list[float]:
        """Compute a rolling average over *values* with the given window size."""
        window = min(window, len(values))
        result = []
        running = sum(values[:window])
        for i in range(len(values)):
            if i < window:
                result.append(sum(values[: i + 1]) / (i + 1))
            else:
                running += values[i] - values[i - window]
                result.append(running / window)
        return result
