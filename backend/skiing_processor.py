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
        parquet_path = os.path.join(self.mergedfiles_path, "split_mesgs.parquet")
        if os.path.exists(parquet_path):
            session_mesgs = pl.read_parquet(parquet_path).with_columns(
                pl.col("start_time")
                .dt.convert_time_zone("America/Denver")
                .dt.date()
                .alias("DT_DENVER")
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
        df = (
            self.skiing.filter(pl.col("split_type") == "ski_run_split")
            .group_by("DT_DENVER")
            .agg(
                pl.col("split_type").count().alias("NUMBER_OF_RUNS"),
                (pl.col("total_descent") * 3.28084)
                .sum()
                .round(1)
                .alias("TOTAL_DESCENT"),
                pl.col("total_elapsed_time")
                .sum()
                .map_elements(self._fmt_ride_time)
                .alias("ELAPSED_TIME"),
                pl.col("total_moving_time")
                .sum()
                .map_elements(self._fmt_ride_time)
                .alias("total_moving_time"),
            )
        )
        return df
