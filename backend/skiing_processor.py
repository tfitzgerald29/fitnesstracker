import polars as pl
import os
from .FitFileProcessor import FitFileProcessor


class skiing(FitFileProcessor):
    def __init__(
        self, source_folder=None, processedpath=None, mergedfiles_path=None
    ) -> None:
        super().__init__(source_folder, processedpath, mergedfiles_path)
        self.skiing = self.load_skiing_data()

    def load_skiing_data(self):
        # parquet_path = os.path.join(self.mergedfiles_path, "split_mesgs.parquet")
        parquet_path = os.path.join(self.mergedfiles_path, "session_mesgs.parquet")
        if os.path.exists(parquet_path):
            session_mesgs = (
                pl.read_parquet(parquet_path)
                .filter(pl.col("sport") == "alpine_skiing")
                .select(
                    "timestamp",
                    "total_elapsed_time",
                    "total_distance",
                    "sport_profile_name",
                    "avg_speed",
                    "max_speed",
                    "total_ascent",
                    "total_descent",
                    "num_laps",
                    "event",
                    "event_type",
                    "sport",
                    "sub_sport",
                    "trigger",
                    "avg_temperature",
                    "max_temperature",
                    "min_temperature",
                    "enhanced_max_speed",
                    "enhanced_avg_speed",
                    "source_file",
                    "avg_heart_rate",
                    "max_heart_rate",
                    "total_moving_time",
                )
                .with_columns(
                    pl.col("timestamp")
                    .dt.convert_time_zone("America/Denver")
                    .dt.date()
                    .alias("DT_DENVER")
                )
            )
            return session_mesgs
        return pl.DataFrame()

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
        # Ski season: Oct–Apr → e.g. Oct 2024–Apr 2025 = "2024-25"
        df = self.skiing.with_columns(
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
