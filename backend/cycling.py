import os
from datetime import timedelta

import plotly.graph_objects as go
import polars as pl

from .FitFileProcessor import FitFileProcessor


class CyclingProcessor(FitFileProcessor):
    """Process cycling activities from session_mesgs for summary and KPI calculations."""

    DEFAULT_SOURCE_FOLDER = "/Users/Tylerfitzgerald/Downloads/"
    DEFAULT_PROCESSED_PATH = "/Users/tylerfitzgerald/Documents/cyclingdashboard_v2/processedfiles"
    DEFAULT_MERGED_PATH = "/Users/tylerfitzgerald/Documents/cyclingdashboard_v2/mergedfiles"

    def __init__(self, source_folder=None, processedpath=None, mergedfiles_path=None):
        super().__init__(
            source_folder or self.DEFAULT_SOURCE_FOLDER,
            processedpath or self.DEFAULT_PROCESSED_PATH,
            mergedfiles_path or self.DEFAULT_MERGED_PATH,
        )
        self.run()
        self.cycling = self._load_cycling_sessions()

    def _load_cycling_sessions(self) -> pl.DataFrame:
        """Load session_mesgs parquet and filter to cycling activities."""
        parquet_path = os.path.join(self.mergedfiles_path, "session_mesgs.parquet")
        if os.path.exists(parquet_path):
            session_mesgs = pl.read_parquet(parquet_path)
            return session_mesgs.filter(pl.col("sport") == "cycling")
        return pl.DataFrame()

    def list_rides(self) -> list[dict]:
        """Return a list of rides with label and timestamp for dropdown selection."""
        df = self.cycling.clone()
        if df.is_empty():
            return []

        ts_col = "timestamp"
        if df[ts_col].dtype.time_zone is None:
            df = df.with_columns(pl.col(ts_col).dt.replace_time_zone("UTC"))
        df = df.with_columns(
            pl.col(ts_col).dt.convert_time_zone("America/Denver")
        )

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

        dt = datetime.fromisoformat(ride_timestamp).astimezone(ZoneInfo("America/Denver"))

        df = self.cycling.clone()
        ts_col = "timestamp"
        if df[ts_col].dtype.time_zone is None:
            df = df.with_columns(pl.col(ts_col).dt.replace_time_zone("UTC"))
        df = df.with_columns(
            pl.col(ts_col).dt.convert_time_zone("America/Denver")
        )

        ride = df.filter(pl.col(ts_col) == pl.lit(dt).cast(pl.Datetime("us", "America/Denver")))
        if ride.is_empty():
            return None

        r = ride.to_dicts()[0]
        return {
            "date": r[ts_col].strftime("%Y-%m-%d %I:%M %p"),
            "distance_mi": round(r["total_distance"] / 1609.344, 1),
            "duration_hr": round(r["total_timer_time"] / 3600, 2),
            "total_timer_time_s": r["total_timer_time"],
            "elapsed_hr": round(r["total_elapsed_time"] / 3600, 2),
            "avg_power": r.get("avg_power"),
            "normalized_power": r.get("normalized_power"),
            "avg_speed_mph": round(r["enhanced_avg_speed"] * 2.23694, 1) if r.get("enhanced_avg_speed") else None,
            "avg_cadence": r.get("avg_cadence"),
            "avg_hr": r.get("avg_heart_rate"),
            "max_hr": r.get("max_heart_rate"),
            "total_ascent_ft": round(r["total_ascent"] * 3.28084) if r.get("total_ascent") else None,
            "total_descent_ft": round(r["total_descent"] * 3.28084) if r.get("total_descent") else None,
            "calories": r.get("total_calories"),
            "tss": round(r["training_stress_score"]) if r.get("training_stress_score") else None,
            "intensity_factor": r.get("intensity_factor"),
            "ftp": r.get("threshold_power"),
            "work_kj": round(r["total_work"] / 1000) if r.get("total_work") else None,
            "left_balance": round(100 - (r["left_right_balance"] & 0x3FFF) / 100, 1) if r.get("left_right_balance") else None,
            "right_balance": round((r["left_right_balance"] & 0x3FFF) / 100, 1) if r.get("left_right_balance") else None,
            "source_file": r.get("source_file"),
        }

    def _load_ride_power(self, source_file: str) -> list[int] | None:
        """Load and clean power data for a single ride from record_mesgs."""
        records_path = os.path.join(self.mergedfiles_path, "record_mesgs.parquet")
        if not os.path.exists(records_path):
            return None

        records = pl.read_parquet(records_path).filter(
            pl.col("source_file") == source_file
        ).sort("timestamp")

        if records.is_empty() or "power" not in records.columns:
            return None
        if records["power"].drop_nulls().len() == 0:
            return None

        return records.with_columns(
            pl.col("power")
            .fill_null((pl.col("power").shift(1) + pl.col("power").shift(2)) / 2)
            .fill_null(0)
        )["power"].cast(pl.Int64).to_list()

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

    # Standard durations for peak power cards
    PEAK_DURATIONS = [
        (5, "5s"), (30, "30s"), (60, "1min"), (300, "5min"),
        (600, "10min"), (1200, "20min"), (1800, "30min"), (3600, "60min"),
        (5400, "90min"), (7200, "120min"),
    ]

    # Finer durations for power curve chart
    CURVE_DURATIONS = [
        1, 2, 3, 5, 10, 15, 20, 30, 45, 60, 90, 120, 180, 240, 300,
        360, 420, 480, 540, 600, 720, 900, 1200, 1500, 1800, 2400, 3600, 5400, 7200,
    ]

    def get_peak_powers(self, source_file: str) -> list[dict]:
        """Compute best average power for standard durations from record_mesgs."""
        power = self._load_ride_power(source_file)
        if not power:
            return []

        results = []
        for window, label in self.PEAK_DURATIONS:
            if window > len(power):
                results.append({"duration": label, "watts": "N/A"})
            else:
                watts = self._best_avg_power(power, window)
                results.append({"duration": label, "watts": watts if watts is not None else "N/A"})
        return results

    def get_power_curve(self, source_file: str) -> dict:
        """Compute full power curve for a single ride. Returns {durations: [], watts: []}."""
        power = self._load_ride_power(source_file)
        if not power:
            return {"durations": [], "watts": []}

        durations = []
        watts = []
        for d in self.CURVE_DURATIONS:
            w = self._best_avg_power(power, d)
            if w is not None:
                durations.append(d)
                watts.append(w)
        return {"durations": durations, "watts": watts}

    def get_best_power_curve(self, period_months: int | None = None) -> dict:
        """Compute best-of power curve across all rides in a date range.

        Args:
            period_months: Number of months to look back (None = all time)
        Returns:
            {durations: [], watts: []}
        """
        from datetime import date

        df = self.cycling.clone()
        if df.is_empty():
            return {"durations": [], "watts": []}

        ts_col = "timestamp"
        if df[ts_col].dtype.time_zone is None:
            df = df.with_columns(pl.col(ts_col).dt.replace_time_zone("UTC"))

        if period_months is not None:
            cutoff = date.today() - timedelta(days=period_months * 30)
            df = df.filter(pl.col(ts_col).dt.date() >= cutoff)

        source_files = df["source_file"].unique().to_list()
        if not source_files:
            return {"durations": [], "watts": []}

        # Compute best across all rides for each duration
        best_by_duration = {}
        for sf in source_files:
            power = self._load_ride_power(sf)
            if not power:
                continue
            for d in self.CURVE_DURATIONS:
                w = self._best_avg_power(power, d)
                if w is not None:
                    if d not in best_by_duration or w > best_by_duration[d]:
                        best_by_duration[d] = w

        durations = sorted(best_by_duration.keys())
        watts = [best_by_duration[d] for d in durations]
        return {"durations": durations, "watts": watts}

    def summarize_cycling(self, group_by="year") -> pl.DataFrame:
        """Summarize cycling rides by year, month, or week with rides, miles, time, and TSS."""
        df = self.cycling.clone()

        timestamp_col = "timestamp"
        if df[timestamp_col].dtype.time_zone is None:
            df = df.with_columns(pl.col(timestamp_col).dt.replace_time_zone("UTC"))
        df = df.with_columns(
            pl.col(timestamp_col).dt.convert_time_zone("America/Denver")
        )

        if group_by == "year":
            df = df.with_columns(pl.col(timestamp_col).dt.year().alias("year"))
            group_cols = ["year"]
        elif group_by == "month":
            df = df.with_columns(
                pl.col(timestamp_col).dt.year().alias("year"),
                pl.col(timestamp_col).dt.month().alias("month"),
            )
            group_cols = ["year", "month"]
        else:
            df = df.with_columns(
                pl.col(timestamp_col).dt.truncate("1w").alias("week_start")
            )
            df = df.with_columns(
                pl.col("week_start").dt.year().alias("year"),
                pl.col("week_start").dt.strftime("%Y-%m-%d").alias("week_starting"),
                (pl.col("week_start") + pl.duration(days=6))
                .dt.strftime("%Y-%m-%d")
                .alias("week_ending"),
            )
            group_cols = ["year", "week_starting", "week_ending"]

        summary = df.group_by(group_cols).agg(
            pl.col("total_timer_time").sum().alias("total_seconds"),
            pl.col("total_distance").sum().alias("total_meters"),
            pl.col("training_stress_score").sum().alias("total_tss"),
            pl.len().alias("rides"),
        )

        summary = summary.with_columns(
            (pl.col("total_seconds") / 3600).round(1).alias("hours"),
            (pl.col("total_meters") / 1609.344).round(1).alias("miles"),
            pl.col("total_tss").round(0).cast(pl.Int64).alias("tss"),
        )

        select_cols = group_cols + ["rides", "miles", "hours", "tss"]
        return summary.select(select_cols).sort(group_cols)

    def compute_daily_tss(self) -> pl.DataFrame:
        
        df = self.cycling.with_columns(
            pl.col("timestamp")
            .dt.convert_time_zone("America/Denver")
            .dt.date()
            .alias("date"),
            pl.when(pl.col("training_stress_score").is_not_null())
            .then(pl.col("training_stress_score"))
            .otherwise(
                (
                    pl.col("total_timer_time")
                    * pl.col("normalized_power")
                    * pl.col("intensity_factor")
                )
                / (pl.col("threshold_power") * 3600)
                * 100
            )
            .alias("tss"),
        )

        daily_tss = df.group_by("date").agg(pl.col("tss").sum()).sort("date")

        # Expand to full date range + 60-day projection, filling gaps with 0
        from datetime import date

        min_date = daily_tss["date"].min()
        today = date.today()
        projection_end = today + timedelta(days=60)

        all_dates = pl.DataFrame(
            {"date": pl.date_range(min_date, projection_end, eager=True)}
        )

        daily_tss = all_dates.join(daily_tss, on="date", how="left").with_columns(
            pl.col("tss").fill_null(0.0),
            (pl.col("date") > today).alias("is_projection"),
        )

        return daily_tss

    def compute_ctl_atl(self, ctl_days: int = 42, atl_days: int = 7) -> pl.DataFrame:
        
        daily_tss = self.compute_daily_tss()

        ctl_decay = 2.0 / (ctl_days + 1)
        atl_decay = 2.0 / (atl_days + 1)

        tss_values = daily_tss["tss"].to_list()

        ctl_values = []
        atl_values = []
        ctl = 0.0
        atl = 0.0

        for tss in tss_values:
            ctl = ctl * (1 - ctl_decay) + tss * ctl_decay
            atl = atl * (1 - atl_decay) + tss * atl_decay
            ctl_values.append(ctl)
            atl_values.append(atl)

        return daily_tss.with_columns(
            pl.Series("ctl", ctl_values),
            pl.Series("atl", atl_values),
            (pl.Series("ctl", ctl_values) - pl.Series("atl", atl_values)).alias("tsb"),
        )

    def compute_ctl_atl_forecast(
        self, ctl_days: int = 42, atl_days: int = 7, lookback_days: int = 42
    ) -> pl.DataFrame:
        
        daily_tss = self.compute_daily_tss()

        # Calculate average daily TSS from the last N actual (non-projection) days
        actual = daily_tss.filter(~pl.col("is_projection"))
        recent = actual.tail(lookback_days)
        avg_tss = recent["tss"].mean()

        # Replace projection-day TSS with the historical average
        forecast_tss = daily_tss.with_columns(
            pl.when(pl.col("is_projection"))
            .then(pl.lit(avg_tss))
            .otherwise(pl.col("tss"))
            .alias("tss_forecast")
        )

        ctl_decay = 2.0 / (ctl_days + 1)
        atl_decay = 2.0 / (atl_days + 1)

        tss_values = forecast_tss["tss_forecast"].to_list()

        ctl_values = []
        atl_values = []
        ctl = 0.0
        atl = 0.0

        for tss in tss_values:
            ctl = ctl * (1 - ctl_decay) + tss * ctl_decay
            atl = atl * (1 - atl_decay) + tss * atl_decay
            ctl_values.append(ctl)
            atl_values.append(atl)

        return forecast_tss.select(
            "date", "tss_forecast", "is_projection"
        ).with_columns(
            pl.Series("ctl_forecast", ctl_values),
            pl.Series("atl_forecast", atl_values),
            (
                pl.Series("ctl_forecast", ctl_values)
                - pl.Series("atl_forecast", atl_values)
            ).alias("tsb_forecast"),
        )

    def plot_training_load(
        self, start_date=None, include_forecast: bool = True
    ) -> go.Figure:

        df = self.compute_ctl_atl()

        if include_forecast:
            df_forecast = self.compute_ctl_atl_forecast()

        if start_date is not None:
            if isinstance(start_date, str):
                from datetime import date

                start_date = date.fromisoformat(start_date)
            df = df.filter(pl.col("date") >= start_date)
            if include_forecast:
                df_forecast = df_forecast.filter(pl.col("date") >= start_date)

        dates = df["date"].to_list()
        projection_start = df.filter(pl.col("is_projection"))["date"].min()

        # Forecast lines only in the projection zone
        proj_dates = []
        if include_forecast:
            proj = df_forecast.filter(pl.col("is_projection"))
            proj_dates = proj["date"].to_list()

        fig = go.Figure()

        # Daily TSS as bars
        fig.add_trace(
            go.Bar(
                x=dates,
                y=df["tss"].to_list(),
                name="TSS",
                marker_color="rgba(150,150,150,0.4)",
                yaxis="y2",
            )
        )

        # CTL (Fitness)
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=df["ctl"].to_list(),
                name="CTL (Fitness)",
                mode="lines",
                line=dict(color="#2196F3", width=2),
            )
        )

        # ATL (Fatigue)
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=df["atl"].to_list(),
                name="ATL (Fatigue)",
                mode="lines",
                line=dict(color="#F44336", width=2),
            )
        )

        # TSB (Form)
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=df["tsb"].to_list(),
                name="TSB (Form)",
                mode="lines",
                line=dict(color="#4CAF50", width=2, dash="dash"),
            )
        )

        # History-based forecast lines (projection zone only)
        if include_forecast and len(proj_dates) > 0:
            fig.add_trace(
                go.Scatter(
                    x=proj_dates,
                    y=proj["ctl_forecast"].to_list(),
                    name="CTL Forecast",
                    mode="lines",
                    line=dict(color="#2196F3", width=2, dash="dot"),
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=proj_dates,
                    y=proj["atl_forecast"].to_list(),
                    name="ATL Forecast",
                    mode="lines",
                    line=dict(color="#F44336", width=2, dash="dot"),
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=proj_dates,
                    y=proj["tsb_forecast"].to_list(),
                    name="TSB Forecast",
                    mode="lines",
                    line=dict(color="#4CAF50", width=2, dash="dot"),
                )
            )

        # Projection shading
        if projection_start is not None:
            fig.add_vrect(
                x0=projection_start,
                x1=dates[-1],
                fillcolor="rgba(200,200,200,0.2)",
                line_width=0,
                annotation_text="Projection",
                annotation_position="top left",
            )

        fig.update_layout(
            title="Training Load: CTL / ATL / TSB",
            xaxis_title="Date",
            yaxis=dict(title="CTL / ATL / TSB"),
            yaxis2=dict(title="TSS", overlaying="y", side="right", showgrid=False),
            hovermode="x unified",
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
            ),
            template="plotly_white",
            width=1400,
            height=700,
        )

        return fig
