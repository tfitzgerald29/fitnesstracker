"""
Sleep data ingestion and read-only query layer for Garmin sleep exports.

Ingestion pipeline (local mode only, mirrors FitFileProcessor):
    ~/Downloads/*_sleepData.json  →  copy to sleepdata/
    ~/Downloads/*sleep*.csv       →  parse key/value exports (one row per file)
    then merge both sources into mergedfiles/sleep.parquet

In S3 mode, ingestion is not run at startup — files are expected to already
exist at s3://<bucket>/<user_id>/sleepdata/.

Does not inherit from FitFileProcessor — sleep data is pre-exported JSON,
not FIT files.
"""

import glob
import csv
import os
import re
import shutil
from datetime import datetime

import polars as pl

from .schemas.sleep import SLEEP
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

    CSV_KEY_MAP: dict[str, list[str]] = {
        "calendar_date": ["Calendar Date", "Date"],
        "sleep_start_gmt": ["Sleep Start GMT", "Sleep Start Time", "Sleep Start"],
        "sleep_end_gmt": ["Sleep End GMT", "Sleep End Time", "Sleep End"],
        "deep_sec": ["Deep Sleep Duration", "Deep Sleep"],
        "light_sec": ["Light Sleep Duration", "Light Sleep"],
        "rem_sec": ["REM Duration", "REM Sleep Duration", "REM Sleep"],
        "awake_sec": ["Awake Time", "Awake Duration", "Awake"],
        "total_sleep_sec": ["Sleep Duration", "Total Sleep Duration", "Total Sleep"],
        "total_in_bed_sec": ["Time in Bed", "Total in Bed", "In Bed Duration"],
        "sleep_efficiency_pct": ["Sleep Efficiency", "Sleep Efficiency %"],
        "avg_spo2": ["Average SpO2", "Avg SpO2"],
        "lowest_spo2": ["Lowest SpO2", "Min SpO2"],
        "avg_hr": ["Average Heart Rate", "Average HR", "Avg HR"],
        "avg_respiration": ["Average Respiration", "Avg Respiration"],
        "lowest_respiration": ["Lowest Respiration"],
        "highest_respiration": ["Highest Respiration"],
        "awake_count": ["Awakenings", "Awake Count"],
        "restless_moments": ["Restless Moments", "Restless"],
        "avg_sleep_stress": ["Average Sleep Stress", "Avg Sleep Stress"],
        "score_overall": ["Overall Sleep Score", "Sleep Score", "Score Overall"],
        "score_quality": ["Sleep Quality Score", "Score Quality"],
        "score_duration": ["Sleep Duration Score", "Score Duration"],
        "score_recovery": ["Recovery Score", "Score Recovery"],
        "score_deep": ["Deep Sleep Score", "Score Deep"],
        "score_rem": ["REM Sleep Score", "Score REM"],
        "feedback": ["Feedback", "Sleep Feedback"],
    }

    CSV_SECONDS_FIELDS = {
        "deep_sec",
        "light_sec",
        "rem_sec",
        "awake_sec",
        "total_sleep_sec",
        "total_in_bed_sec",
    }

    CSV_INT_FIELDS = {
        "deep_sec",
        "light_sec",
        "rem_sec",
        "awake_sec",
        "total_sleep_sec",
        "total_in_bed_sec",
        "lowest_spo2",
        "awake_count",
        "restless_moments",
        "score_overall",
        "score_quality",
        "score_duration",
        "score_recovery",
        "score_deep",
        "score_rem",
    }

    CSV_FLOAT_FIELDS = {
        "sleep_efficiency_pct",
        "deep_hrs",
        "light_hrs",
        "rem_hrs",
        "awake_hrs",
        "total_sleep_hrs",
        "avg_spo2",
        "avg_hr",
        "avg_respiration",
        "lowest_respiration",
        "highest_respiration",
        "avg_sleep_stress",
    }

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

    def _find_sleep_csvs(self) -> list[str]:
        """Return sorted list of sleep-like CSV paths from source_folder."""
        if not os.path.isdir(self.source_folder):
            return []

        return sorted(
            e.path
            for e in os.scandir(self.source_folder)
            if e.is_file()
            and e.name.lower().endswith(".csv")
            and "sleep" in e.name.lower()
        )

    @staticmethod
    def _norm_key(key: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", key.strip().lower())

    @staticmethod
    def _to_int(value: object) -> int | None:
        text = str(value).strip().replace(",", "")
        if not text:
            return None
        try:
            return int(float(text))
        except ValueError:
            return None

    @staticmethod
    def _to_float(value: object) -> float | None:
        text = str(value).strip().replace(",", "")
        if not text:
            return None
        if text.endswith("%"):
            text = text[:-1].strip()
        try:
            return float(text)
        except ValueError:
            return None

    @staticmethod
    def _to_iso_date(value: object) -> str | None:
        text = str(value).strip()
        if not text:
            return None

        for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%Y/%m/%d"]:
            try:
                return datetime.strptime(text, fmt).date().isoformat()
            except ValueError:
                pass

        try:
            return datetime.fromisoformat(text).date().isoformat()
        except ValueError:
            return text

    @staticmethod
    def _duration_to_seconds(duration: object) -> int | None:
        text = str(duration).strip().lower()
        if not text:
            return None

        if ":" in text:
            parts = text.split(":")
            try:
                if len(parts) == 3:
                    h, m, s = (int(p) for p in parts)
                    return h * 3600 + m * 60 + s
                if len(parts) == 2:
                    m, s = (int(p) for p in parts)
                    return m * 60 + s
            except ValueError:
                pass

        matches = re.findall(r"(\d+)\s*([hms])", text)
        if matches:
            total = 0
            for value_str, unit in matches:
                value = int(value_str)
                if unit == "h":
                    total += value * 3600
                elif unit == "m":
                    total += value * 60
                elif unit == "s":
                    total += value
            return total

        if text.isdigit():
            return int(text)

        return None

    def _coerce_sleep_schema(self, df: pl.DataFrame) -> pl.DataFrame:
        """Ensure all sleep schema columns exist and are cast to canonical dtypes."""
        for col, dtype in SLEEP.items():
            if col not in df.columns:
                null_series = pl.Series(col, [None] * len(df), dtype=dtype)
                df = df.with_columns(null_series)
            elif df[col].dtype != dtype:
                try:
                    df = df.with_columns(pl.col(col).cast(dtype, strict=False))
                except Exception as e:
                    print(
                        f"  Sleep schema: could not cast '{col}' from {df[col].dtype} to {dtype}: {e}"
                    )

        return df.select(list(SLEEP.keys()))

    def _parse_sleep_csv(self, csv_path: str) -> dict | None:
        """Parse one sleep-like CSV key/value file into a canonical sleep row dict."""
        raw_data = {}

        try:
            with open(csv_path, mode="r", newline="", encoding="utf-8") as f:
                reader = csv.reader(f)
                try:
                    next(reader)
                except StopIteration:
                    return None

                for row in reader:
                    if len(row) >= 2 and row[0].strip() and row[1].strip():
                        raw_data[self._norm_key(row[0])] = row[1].strip()
        except Exception as e:
            print(f"  Sleep CSV parse: error reading {os.path.basename(csv_path)}: {e}")
            return None

        if not raw_data:
            return None

        row = {k: None for k in SLEEP.keys()}

        for target_key, aliases in self.CSV_KEY_MAP.items():
            value = None
            for alias in aliases:
                candidate = raw_data.get(self._norm_key(alias))
                if candidate is not None:
                    value = candidate
                    break

            if value is None:
                continue

            if target_key == "calendar_date":
                row[target_key] = self._to_iso_date(value)
            elif target_key in self.CSV_SECONDS_FIELDS:
                row[target_key] = self._duration_to_seconds(value)
            elif target_key in self.CSV_INT_FIELDS:
                row[target_key] = self._to_int(value)
            elif target_key in self.CSV_FLOAT_FIELDS:
                row[target_key] = self._to_float(value)
            else:
                row[target_key] = value

        if row["calendar_date"] is None:
            print(
                f"  Sleep CSV parse: missing calendar date in {os.path.basename(csv_path)}, skipping"
            )
            return None

        for sec_key, hrs_key in [
            ("deep_sec", "deep_hrs"),
            ("light_sec", "light_hrs"),
            ("rem_sec", "rem_hrs"),
            ("awake_sec", "awake_hrs"),
            ("total_sleep_sec", "total_sleep_hrs"),
        ]:
            if row[sec_key] is not None and row[hrs_key] is None:
                row[hrs_key] = row[sec_key] / 3600

        if row["total_in_bed_sec"] is None:
            total_sleep_sec = row["total_sleep_sec"]
            awake_sec = row["awake_sec"]
            if total_sleep_sec is not None and awake_sec is not None:
                row["total_in_bed_sec"] = total_sleep_sec + awake_sec

        if (
            row["sleep_efficiency_pct"] is None
            and row["total_in_bed_sec"]
            and row["total_sleep_sec"] is not None
        ):
            row["sleep_efficiency_pct"] = round(
                row["total_sleep_sec"] / row["total_in_bed_sec"] * 100, 1
            )

        return row

    def run(self) -> dict:
        """Ingest new sleep JSON files from Downloads, merge to parquet, reload DataFrame.

        Mirrors the FitFileProcessor.run() interface so app.py can call both
        pipelines the same way on startup.
        """
        print("=" * 60)
        print("Starting Sleep Data Ingestion Pipeline")
        print("=" * 60)

        print(
            f"\n[Step 1] Scanning {self.source_folder} for *_sleepData.json and *sleep*.csv ..."
        )
        new_files = self.ingest_from_downloads()

        print(f"\n[Step 2] Merging sleep records to parquet ...")
        merge_stats = self._merge_to_parquet()

        print(f"\n[Step 3] Reloading sleep DataFrame ...")
        self.sleep = self._load_sleep_data()

        print("\n" + "=" * 60)
        print("Sleep Pipeline Complete!")
        print("=" * 60)
        print(f"New files copied : {len(new_files)}")
        print(f"CSV rows added   : {merge_stats['csv_rows_added']}")
        print(f"CSV rows skipped : {merge_stats['csv_rows_skipped_stale']}")
        print(f"Total records    : {len(self.sleep)}")
        if not self.sleep.is_empty():
            print(
                f"Date range       : {self.sleep['calendar_date'].min()} → {self.sleep['calendar_date'].max()}"
            )

        return {
            "new_files_copied": len(new_files),
            "new_files": new_files,
            "csv_rows_added": merge_stats["csv_rows_added"],
            "csv_rows_skipped_stale": merge_stats["csv_rows_skipped_stale"],
            "total_records": len(self.sleep),
        }

    def _merge_to_parquet(self) -> dict[str, int]:
        """Parse sleep JSON + optional sleep-like CSV files and update sleep.parquet.

        Deduplicates on calendar_date so re-running is always safe.
        JSON is authoritative when JSON and CSV contain the same date.
        """
        parquet_path = storage.path_join(self.mergedfiles_path, "sleep.parquet")

        csv_rows_added = 0
        csv_rows_skipped_stale = 0

        json_df = self._parse_all_json()

        csv_df = pl.DataFrame()
        csv_paths = self._find_sleep_csvs()
        if not csv_paths:
            print(f"  Sleep merge: no *sleep*.csv files found in {self.source_folder}")
        else:
            csv_rows = []
            for csv_path in csv_paths:
                row = self._parse_sleep_csv(csv_path)
                if row is None:
                    print(
                        f"  Sleep merge: could not parse {os.path.basename(csv_path)}, skipping"
                    )
                    continue
                csv_rows.append(row)

            if not csv_rows:
                print("  Sleep merge: no parseable sleep CSV rows found")
            else:
                csv_df = self._coerce_sleep_schema(pl.DataFrame(csv_rows, strict=False))
                before = len(csv_df)
                csv_df = csv_df.unique(subset=["calendar_date"], keep="last")
                dropped_dupe_dates = before - len(csv_df)
                if dropped_dupe_dates > 0:
                    print(
                        f"  Sleep merge: skipped {dropped_dupe_dates} duplicate CSV row(s) by calendar_date"
                    )

        if not csv_df.is_empty() and not json_df.is_empty():
            json_dates = json_df.select("calendar_date").drop_nulls().unique()
            before = len(csv_df)
            csv_df = csv_df.join(json_dates, on="calendar_date", how="anti")
            csv_rows_skipped_stale = before - len(csv_df)
            if csv_rows_skipped_stale > 0:
                print(
                    f"  Sleep merge: skipped {csv_rows_skipped_stale} CSV row(s) already present in JSON"
                )

        csv_rows_added = len(csv_df)

        fresh_frames = []
        if not csv_df.is_empty():
            fresh_frames.append(csv_df)
        if not json_df.is_empty():
            fresh_frames.append(json_df)

        if not fresh_frames:
            print("  Sleep merge: no records parsed, skipping parquet write")
            return {
                "csv_rows_added": csv_rows_added,
                "csv_rows_skipped_stale": csv_rows_skipped_stale,
            }

        fresh_df = self._coerce_sleep_schema(
            pl.concat(fresh_frames, how="diagonal_relaxed")
        )

        if storage.path_exists(parquet_path):
            try:
                existing_df = self._coerce_sleep_schema(
                    storage.read_parquet(parquet_path)
                )
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

        storage.write_parquet(self._coerce_sleep_schema(combined), parquet_path)
        print(f"  ✓ sleep.parquet: {len(combined)} records → {parquet_path}")
        return {
            "csv_rows_added": csv_rows_added,
            "csv_rows_skipped_stale": csv_rows_skipped_stale,
        }

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
                return self._coerce_sleep_schema(
                    storage.read_parquet(parquet_path)
                ).sort("calendar_date")
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

        # Build with explicit schema overrides to avoid mixed-type inference
        # failures across rows (e.g. int in early rows, float in later rows).
        df = pl.DataFrame(rows, schema_overrides=SLEEP, strict=False)
        return self._coerce_sleep_schema(df).sort("calendar_date")

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
