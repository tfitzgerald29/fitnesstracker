"""
Sleep data ingestion and read-only query layer for Garmin sleep exports.

Ingestion pipeline (local mode only, mirrors FitFileProcessor):
    ~/Downloads/*_sleepData.json  →  copy to sleepdata/  →  load DataFrame

In S3 mode, ingestion is not run at startup — files are expected to already
exist at s3://<bucket>/<user_id>/sleepdata/.

Does not inherit from FitFileProcessor — sleep data is pre-exported JSON,
not FIT files.
"""

import glob
import json
import os
import shutil

import polars as pl

from .storage import storage


class SleepProcessor:
    DEFAULT_SOURCE_FOLDER = os.environ.get(
        "FIT_SOURCE_FOLDER", os.path.expanduser("~/Downloads")
    )

    def __init__(
        self, source_folder: str | None = None, user_id: str | None = None
    ) -> None:
        self.source_folder = source_folder or self.DEFAULT_SOURCE_FOLDER
        self.wellness_path = storage.wellness_path(user_id)
        self.mergedfiles_path = storage.merged_path(user_id)
        self.sleep = self._load_sleep_data()

    # ── Ingestion ─────────────────────────────────────────────────────────────

    def ingest_from_downloads(self) -> list[str]:
        """Scan source_folder for *_sleepData.json files and copy new ones to wellness_path.

        Skips any file whose name already exists in wellness_path.
        Returns a list of filenames that were newly copied.
        """
        if not os.path.isdir(self.source_folder):
            print(f"  Sleep ingest: source folder not found: {self.source_folder}")
            return []

        storage.makedirs(self.wellness_path)

        new_files = []
        candidates = sorted(
            e.path
            for e in os.scandir(self.source_folder)
            if e.is_file() and e.name.endswith("_sleepData.json")
        )

        for src_path in candidates:
            filename = os.path.basename(src_path)
            dest_path = storage.path_join(self.wellness_path, filename)

            if storage.path_exists(dest_path):
                print(f"  Sleep ingest: skipping {filename} (already present)")
                continue

            try:
                shutil.copy2(src_path, dest_path)
                new_files.append(filename)
                print(f"  Sleep ingest: copied {filename}")
            except Exception as e:
                print(f"  Sleep ingest: error copying {filename}: {e}")

        return new_files

    def run(self) -> dict:
        """Ingest new sleep JSON files from Downloads, merge to parquet, reload DataFrame.

        Mirrors the FitFileProcessor.run() interface so app.py can call both
        pipelines the same way on startup.
        """
        print("=" * 60)
        print("Starting Sleep Data Ingestion Pipeline")
        print("=" * 60)

        print(f"\n[Step 1] Scanning {self.source_folder} for *_sleepData.json ...")
        new_files = self.ingest_from_downloads()

        print(f"\n[Step 2] Merging sleep records to parquet ...")
        self._merge_to_parquet()

        print(f"\n[Step 3] Reloading sleep DataFrame ...")
        self.sleep = self._load_sleep_data()

        print("\n" + "=" * 60)
        print("Sleep Pipeline Complete!")
        print("=" * 60)
        print(f"New files copied : {len(new_files)}")
        print(f"Total records    : {len(self.sleep)}")
        if not self.sleep.is_empty():
            print(
                f"Date range       : {self.sleep['calendar_date'].min()} → {self.sleep['calendar_date'].max()}"
            )

        return {
            "new_files_copied": len(new_files),
            "new_files": new_files,
            "total_records": len(self.sleep),
        }

    def _merge_to_parquet(self) -> None:
        """Parse all sleep JSON files and write/update mergedfiles/sleep.parquet.

        Deduplicates on calendar_date so re-running is always safe.
        """
        parquet_path = storage.path_join(self.mergedfiles_path, "sleep.parquet")

        # Parse all JSON files fresh
        fresh_df = self._parse_all_json()
        if fresh_df.is_empty():
            print("  Sleep merge: no records parsed, skipping parquet write")
            return

        if storage.path_exists(parquet_path):
            try:
                existing_df = storage.read_parquet(parquet_path)
                combined = pl.concat([existing_df, fresh_df], how="diagonal_relaxed")
                combined = combined.unique(subset=["calendar_date"], keep="last").sort(
                    "calendar_date"
                )
            except Exception as e:
                print(
                    f"  Sleep merge: could not read existing parquet, overwriting — {e}"
                )
                combined = fresh_df
        else:
            storage.makedirs(self.mergedfiles_path)
            combined = fresh_df

        storage.write_parquet(combined, parquet_path)
        print(f"  ✓ sleep.parquet: {len(combined)} records → {parquet_path}")

    # ── Data loading ──────────────────────────────────────────────────────────

    def _list_sleep_files(self) -> list[str]:
        """Return sorted list of *_sleepData.json paths from wellness_path, local or S3."""
        if self.wellness_path.startswith("s3://"):
            try:
                prefix = self.wellness_path.rstrip("/") + "/"
                all_files = storage._s3fs.ls(prefix, detail=False)
                return sorted(f for f in all_files if f.endswith("_sleepData.json"))
            except Exception as e:
                print(f"  Error listing S3 wellness files: {e}")
                return []
        else:
            if not os.path.isdir(self.wellness_path):
                return []
            pattern = self.wellness_path.rstrip("/") + "/*_sleepData.json"
            return sorted(glob.glob(pattern))

    def _read_sleep_file(self, path: str) -> list:
        """Read a single JSON file, returning a list of records."""
        try:
            raw = storage.read_json(path)
            return raw if isinstance(raw, list) else []
        except Exception as e:
            print(f"  Error reading sleep file {os.path.basename(path)}: {e}")
            return []

    def _load_sleep_data(self) -> pl.DataFrame:
        """Load sleep data — from parquet if available, otherwise parse JSON directly."""
        parquet_path = storage.path_join(self.mergedfiles_path, "sleep.parquet")
        if storage.path_exists(parquet_path):
            try:
                return storage.read_parquet(parquet_path)
            except Exception as e:
                print(f"  Sleep load: parquet read failed, falling back to JSON — {e}")
        return self._parse_all_json()

    def _parse_all_json(self) -> pl.DataFrame:
        """Parse all *_sleepData.json files from wellness_path into a DataFrame."""
        files = self._list_sleep_files()
        if not files:
            return pl.DataFrame()

        records = []
        for file_path in files:
            records.extend(self._read_sleep_file(file_path))

        if not records:
            return pl.DataFrame()

        rows = []
        for r in records:
            try:
                rows.append(self._parse_record(r))
            except Exception as e:
                print(f"  Error parsing sleep record {r.get('calendarDate', '?')}: {e}")

        if not rows:
            return pl.DataFrame()

        # Explicit schema overrides for every column that can be None in some
        # records. Without this, Polars infers the type from the first N rows
        # and raises a ComputeError when it later encounters a None or a
        # different primitive type (e.g. int vs float, str vs NoneType).
        schema_overrides = {
            # nullable strings
            "calendar_date": pl.Utf8,
            "sleep_start_gmt": pl.Utf8,
            "sleep_end_gmt": pl.Utf8,
            "feedback": pl.Utf8,
            # nullable floats
            "sleep_efficiency_pct": pl.Float64,
            "avg_spo2": pl.Float64,
            "avg_hr": pl.Float64,
            "avg_respiration": pl.Float64,
            "lowest_respiration": pl.Float64,
            "highest_respiration": pl.Float64,
            "avg_sleep_stress": pl.Float64,
            # nullable ints (Int32 matches Garmin's score range 0-100)
            "lowest_spo2": pl.Int32,
            "score_overall": pl.Int32,
            "score_quality": pl.Int32,
            "score_duration": pl.Int32,
            "score_recovery": pl.Int32,
            "score_deep": pl.Int32,
            "score_rem": pl.Int32,
        }
        return pl.DataFrame(rows, schema_overrides=schema_overrides).sort(
            "calendar_date"
        )

    def _parse_record(self, r: dict) -> dict:
        deep = r.get("deepSleepSeconds") or 0
        light = r.get("lightSleepSeconds") or 0
        rem = r.get("remSleepSeconds") or 0
        awake = r.get("awakeSleepSeconds") or 0

        total_sleep_sec = deep + light + rem
        total_in_bed_sec = total_sleep_sec + awake
        efficiency = (
            round(total_sleep_sec / total_in_bed_sec * 100, 1)
            if total_in_bed_sec > 0
            else None
        )

        spo2 = r.get("spo2SleepSummary") or {}
        scores = r.get("sleepScores") or {}

        return {
            "calendar_date": r.get("calendarDate"),
            "sleep_start_gmt": r.get("sleepStartTimestampGMT"),
            "sleep_end_gmt": r.get("sleepEndTimestampGMT"),
            "deep_sec": deep,
            "light_sec": light,
            "rem_sec": rem,
            "awake_sec": awake,
            "total_sleep_sec": total_sleep_sec,
            "total_in_bed_sec": total_in_bed_sec,
            "sleep_efficiency_pct": efficiency,
            "deep_hrs": round(deep / 3600, 2),
            "light_hrs": round(light / 3600, 2),
            "rem_hrs": round(rem / 3600, 2),
            "awake_hrs": round(awake / 3600, 2),
            "total_sleep_hrs": round(total_sleep_sec / 3600, 2),
            "avg_spo2": spo2.get("averageSPO2"),
            "lowest_spo2": spo2.get("lowestSPO2"),
            "avg_hr": spo2.get("averageHR"),
            "avg_respiration": r.get("averageRespiration"),
            "lowest_respiration": r.get("lowestRespiration"),
            "highest_respiration": r.get("highestRespiration"),
            "awake_count": r.get("awakeCount") or 0,
            "restless_moments": r.get("restlessMomentCount") or 0,
            "avg_sleep_stress": r.get("avgSleepStress"),
            "score_overall": scores.get("overallScore"),
            "score_quality": scores.get("qualityScore"),
            "score_duration": scores.get("durationScore"),
            "score_recovery": scores.get("recoveryScore"),
            "score_deep": scores.get("deepScore"),
            "score_rem": scores.get("remScore"),
            "feedback": scores.get("feedback"),
        }

    # ── Summary stats ─────────────────────────────────────────────────────────

    def summary_stats(self) -> dict:
        """High-level averages and totals across all sleep records."""
        df = self.sleep
        if df.is_empty():
            return {}

        avg_sleep = df["total_sleep_hrs"].mean()
        avg_score = df["score_overall"].drop_nulls().mean()
        avg_deep = df["deep_hrs"].mean()
        avg_rem = df["rem_hrs"].mean()
        avg_efficiency = df["sleep_efficiency_pct"].drop_nulls().mean()
        avg_spo2 = df["avg_spo2"].drop_nulls().mean()

        return {
            "total_nights": len(df),
            "avg_sleep_hrs": round(avg_sleep, 1) if avg_sleep is not None else None,
            "avg_score": round(avg_score, 1) if avg_score is not None else None,
            "avg_deep_hrs": round(avg_deep, 2) if avg_deep is not None else None,
            "avg_rem_hrs": round(avg_rem, 2) if avg_rem is not None else None,
            "avg_efficiency_pct": round(avg_efficiency, 1)
            if avg_efficiency is not None
            else None,
            "avg_spo2": round(avg_spo2, 1) if avg_spo2 is not None else None,
            "date_range_start": df["calendar_date"].min(),
            "date_range_end": df["calendar_date"].max(),
        }

    def recent_stats(self, days: int = 30) -> dict:
        """Same stats restricted to the most recent N days of data."""
        df = self.sleep
        if df.is_empty():
            return {}
        latest = df["calendar_date"].max()
        cutoff = (
            pl.Series([latest]).str.to_date().dt.offset_by(f"-{days}d").cast(pl.Utf8)[0]
        )
        recent = df.filter(pl.col("calendar_date") >= cutoff)
        if recent.is_empty():
            return {}

        avg_sleep = recent["total_sleep_hrs"].mean()
        avg_score = recent["score_overall"].drop_nulls().mean()

        return {
            "avg_sleep_hrs": round(avg_sleep, 1) if avg_sleep is not None else None,
            "avg_score": round(avg_score, 1) if avg_score is not None else None,
            "nights": len(recent),
        }

    def chart_data(self, metric: str = "total_sleep_hrs") -> pl.DataFrame:
        """Return calendar_date + chosen metric for the trend chart."""
        df = self.sleep
        if df.is_empty() or metric not in df.columns:
            return pl.DataFrame()
        return df.select(["calendar_date", metric]).drop_nulls()

    def stage_breakdown_data(self) -> pl.DataFrame:
        """Return nightly stage percentages of total time in bed for the stacked bar chart."""
        df = self.sleep
        if df.is_empty():
            return pl.DataFrame()
        return df.filter(pl.col("total_in_bed_sec") > 0).select(
            [
                pl.col("calendar_date"),
                (pl.col("deep_sec") / pl.col("total_in_bed_sec") * 100)
                .round(1)
                .alias("deep_pct"),
                (pl.col("rem_sec") / pl.col("total_in_bed_sec") * 100)
                .round(1)
                .alias("rem_pct"),
                (pl.col("light_sec") / pl.col("total_in_bed_sec") * 100)
                .round(1)
                .alias("light_pct"),
                (pl.col("awake_sec") / pl.col("total_in_bed_sec") * 100)
                .round(1)
                .alias("awake_pct"),
            ]
        )
