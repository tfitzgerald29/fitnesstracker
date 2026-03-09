import os

import polars as pl


class SportSummarizer:

    def __init__(self, mergedfiles_path):
        self.mergedfiles_path = mergedfiles_path

    def load_session_data(self, message_type="session_mesgs") -> pl.DataFrame | None:
        file_path = os.path.join(self.mergedfiles_path, f"{message_type}.parquet")

        if not os.path.exists(file_path):
            print(f"Warning: {file_path} not found")
            return None

        try:
            df = pl.read_parquet(file_path)
            print(f"Loaded {df.shape[0]} records from {message_type}.parquet")
            return df
        except Exception as e:
            print(f"Error loading {message_type}: {e}")
            return None

    def summarize_hours_by_sport(self, group_by=None, timestamp_col="timestamp"):
        df = self.load_session_data("session_mesgs")

        if df is None:
            return None

        group_cols = ["sport"]

        if group_by in ("year", "month", "week"):
            if timestamp_col not in df.columns:
                print(f"Error: timestamp column '{timestamp_col}' not found")
                return None

            if df[timestamp_col].dtype != pl.Datetime:
                df = df.with_columns(pl.col(timestamp_col).cast(pl.Datetime))

            if df[timestamp_col].dtype.time_zone is None:
                df = df.with_columns(pl.col(timestamp_col).dt.replace_time_zone("UTC"))
            df = df.with_columns(
                pl.col(timestamp_col).dt.convert_time_zone("America/Denver")
            )

            if group_by == "year":
                df = df.with_columns(
                    pl.col(timestamp_col).dt.year().alias("year"),
                )
                group_cols.append("year")

            elif group_by == "month":
                # Extract year and month
                df = df.with_columns(
                    [
                        pl.col(timestamp_col).dt.year().alias("year"),
                        pl.col(timestamp_col).dt.month().alias("month"),
                    ]
                )
                group_cols.extend(["year", "month"])

            elif group_by == "week":
                # Truncate to ISO week start (Monday)
                df = df.with_columns(
                    [pl.col(timestamp_col).dt.truncate("1w").alias("week_start")]
                )
                df = df.with_columns(
                    [
                        pl.col("week_start").dt.year().alias("year"),
                        pl.col("week_start")
                        .dt.strftime("%Y-%m-%d")
                        .alias("week_starting"),
                        # pl.col('week_start').dt.month().alias('month')
                        (pl.col("week_start") + pl.duration(days=6))
                        .dt.strftime("%Y-%m-%d")
                        .alias("week_ending"),
                    ]
                )
                group_cols.extend(["year", "week_starting", "week_ending"])

        # Group by specified columns and sum total_timer_time
        summary = df.group_by(group_cols).agg(
            pl.col("total_timer_time").sum().alias("total_seconds")
        )

        # Convert seconds to HH:MM:SS format
        summary = summary.with_columns(
            [
                (pl.col("total_seconds") // 3600).cast(pl.Int64).alias("hours"),
                ((pl.col("total_seconds") % 3600) // 60)
                .cast(pl.Int64)
                .alias("minutes"),
                (pl.col("total_seconds") % 60).cast(pl.Int64).alias("seconds"),
            ]
        )

        # Format as HH:MM:SS string
        summary = summary.with_columns(
            (
                pl.col("hours").cast(pl.Utf8).str.pad_start(2, "0")
                + ":"
                + pl.col("minutes").cast(pl.Utf8).str.pad_start(2, "0")
                + ":"
                + pl.col("seconds").cast(pl.Utf8).str.pad_start(2, "0")
            ).alias("total_time")
        )

        # Select final columns and sort
        if group_by == "year":
            index_cols = ["year"]
        elif group_by == "month":
            index_cols = ["year", "month"]
        elif group_by == "week":
            index_cols = ["year", "week_starting", "week_ending"]
        else:
            return summary.select(["sport", "total_time"]).sort("sport")

        # Pivot: sports become columns, index is the time grouping
        pivoted = summary.select(index_cols + ["sport", "total_seconds"]).pivot(
            index=index_cols, on="sport", values="total_seconds"
        )

        # Rename sport columns for display
        rename_map = {"training": "weight_lifting"}
        pivoted = pivoted.rename({k: v for k, v in rename_map.items() if k in pivoted.columns})

        # Order sport columns: preferred sports first, then alphabetical remainder
        sport_cols = [c for c in pivoted.columns if c not in index_cols]
        preferred_order = ["cycling", "weight_lifting", "rock_climbing"]
        ordered = [s for s in preferred_order if s in sport_cols]
        ordered += sorted(s for s in sport_cols if s not in ordered)
        sport_cols = ordered
        pivoted = pivoted.select(index_cols + sport_cols)
        null_masks = {c: pivoted[c].is_null() for c in sport_cols}
        pivoted = pivoted.with_columns([pl.col(c).fill_null(0) for c in sport_cols])
        pivoted = pivoted.with_columns(pl.sum_horizontal(sport_cols).alias("total"))

        # Compute percentage of total for each sport column
        pivoted = pivoted.with_columns(
            [
                pl.when(pl.col("total") > 0)
                .then((pl.col(c) / pl.col("total") * 100).round(0).cast(pl.Int64))
                .otherwise(pl.lit(0))
                .alias(f"{c}_pct")
                for c in sport_cols
            ]
        )

        # Convert all sport columns and total to HH:MM:SS
        time_cols = sport_cols + ["total"]
        pivoted = pivoted.with_columns(
            [
                (
                    (pl.col(c) // 3600)
                    .cast(pl.Int64)
                    .cast(pl.Utf8)
                    .str.pad_start(2, "0")
                    + ":"
                    + ((pl.col(c) % 3600) // 60)
                    .cast(pl.Int64)
                    .cast(pl.Utf8)
                    .str.pad_start(2, "0")
                    + ":"
                    + (pl.col(c) % 60)
                    .cast(pl.Int64)
                    .cast(pl.Utf8)
                    .str.pad_start(2, "0")
                ).alias(c)
                for c in time_cols
            ]
        )

        # Append percentage to sport columns: "HH:MM:SS (XX%)"
        pivoted = pivoted.with_columns(
            [
                (pl.col(c) + " (" + pl.col(f"{c}_pct").cast(pl.Utf8) + "%)").alias(c)
                for c in sport_cols
            ]
        )

        # Drop the temporary pct columns
        pivoted = pivoted.drop([f"{c}_pct" for c in sport_cols])

        # Replace originally null sport cells with empty string
        pivoted = pivoted.with_columns(
            [
                pl.when(pl.lit(null_masks[c]))
                .then(pl.lit(""))
                .otherwise(pl.col(c))
                .alias(c)
                for c in sport_cols
            ]
        )

        return pivoted.sort(index_cols)

    RENAME_MAP = {"training": "weight_lifting"}

    def get_chart_data(self, group_by="year", timestamp_col="timestamp") -> list[dict]:
        """Return per-sport hours in a format suitable for charting.

        Returns list of dicts: {sport, label, hours} where label is the period string.
        """
        df = self.load_session_data("session_mesgs")
        if df is None:
            return []

        if timestamp_col not in df.columns:
            return []

        if df[timestamp_col].dtype != pl.Datetime:
            df = df.with_columns(pl.col(timestamp_col).cast(pl.Datetime))
        if df[timestamp_col].dtype.time_zone is None:
            df = df.with_columns(pl.col(timestamp_col).dt.replace_time_zone("UTC"))
        df = df.with_columns(
            pl.col(timestamp_col).dt.convert_time_zone("America/Denver")
        )

        # Apply sport renames
        for old, new in self.RENAME_MAP.items():
            df = df.with_columns(
                pl.when(pl.col("sport") == old)
                .then(pl.lit(new))
                .otherwise(pl.col("sport"))
                .alias("sport")
            )

        if group_by == "year":
            df = df.with_columns(
                pl.col(timestamp_col).dt.year().cast(pl.Utf8).alias("label")
            )
        elif group_by == "month":
            df = df.with_columns(
                (
                    pl.col(timestamp_col).dt.year().cast(pl.Utf8)
                    + "-"
                    + pl.col(timestamp_col).dt.month().cast(pl.Utf8).str.pad_start(2, "0")
                ).alias("label")
            )
        else:
            df = df.with_columns(
                pl.col(timestamp_col).dt.truncate("1w").dt.strftime("%Y-%m-%d").alias("label")
            )

        summary = df.group_by(["sport", "label"]).agg(
            pl.col("total_timer_time").sum().alias("total_seconds"),
            pl.len().alias("activities"),
        ).with_columns(
            (pl.col("total_seconds") / 3600).round(1).alias("hours"),
        ).sort("label")

        return summary.to_dicts()

    def get_summary_stats(self, timestamp_col="timestamp") -> dict:
        """Return high-level summary stats across all sports.

        Returns {total_hours_ytd, total_activities_ytd, sports: [{sport, hours, activities, hours_per_week}]}
        """
        from datetime import date

        df = self.load_session_data("session_mesgs")
        if df is None:
            return {"total_hours_ytd": 0, "total_activities_ytd": 0, "sports": []}

        if timestamp_col not in df.columns:
            return {"total_hours_ytd": 0, "total_activities_ytd": 0, "sports": []}

        if df[timestamp_col].dtype != pl.Datetime:
            df = df.with_columns(pl.col(timestamp_col).cast(pl.Datetime))
        if df[timestamp_col].dtype.time_zone is None:
            df = df.with_columns(pl.col(timestamp_col).dt.replace_time_zone("UTC"))
        df = df.with_columns(
            pl.col(timestamp_col).dt.convert_time_zone("America/Denver")
        )

        # Apply sport renames
        for old, new in self.RENAME_MAP.items():
            df = df.with_columns(
                pl.when(pl.col("sport") == old)
                .then(pl.lit(new))
                .otherwise(pl.col("sport"))
                .alias("sport")
            )

        today = date.today()
        ytd = df.filter(pl.col(timestamp_col).dt.year() == today.year)

        total_hours = round(ytd["total_timer_time"].sum() / 3600, 1) if not ytd.is_empty() else 0
        total_activities = len(ytd)

        # Weeks elapsed this year (at least 1)
        day_of_year = today.timetuple().tm_yday
        weeks_elapsed = max(day_of_year / 7, 1)

        # Per-sport stats YTD
        sport_stats = []
        if not ytd.is_empty():
            by_sport = ytd.group_by("sport").agg(
                pl.col("total_timer_time").sum().alias("total_seconds"),
                pl.len().alias("activities"),
            ).sort("total_seconds", descending=True)

            for r in by_sport.to_dicts():
                hours = round(r["total_seconds"] / 3600, 1)
                sport_stats.append({
                    "sport": r["sport"],
                    "hours": hours,
                    "activities": r["activities"],
                    "hours_per_week": round(hours / weeks_elapsed, 1),
                })

        return {
            "total_hours_ytd": total_hours,
            "total_activities_ytd": total_activities,
            "sports": sport_stats,
        }
