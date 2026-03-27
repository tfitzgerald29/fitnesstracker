from datetime import date, timedelta

import plotly.graph_objects as go
import polars as pl


class TrainingLoadMixin:
    """Cycling summary, daily TSS, CTL/ATL/TSB, forecast, and training load plot."""

    @staticmethod
    def _empty_daily_tss() -> pl.DataFrame:
        return pl.DataFrame(
            {
                "date": pl.Series([], dtype=pl.Date),
                "tss": pl.Series([], dtype=pl.Float64),
                "is_projection": pl.Series([], dtype=pl.Boolean),
            }
        )

    @staticmethod
    def _empty_ctl_atl() -> pl.DataFrame:
        return pl.DataFrame(
            {
                "date": pl.Series([], dtype=pl.Date),
                "tss": pl.Series([], dtype=pl.Float64),
                "is_projection": pl.Series([], dtype=pl.Boolean),
                "ctl": pl.Series([], dtype=pl.Float64),
                "atl": pl.Series([], dtype=pl.Float64),
                "tsb": pl.Series([], dtype=pl.Float64),
            }
        )

    @staticmethod
    def _empty_forecast() -> pl.DataFrame:
        return pl.DataFrame(
            {
                "date": pl.Series([], dtype=pl.Date),
                "tss_forecast": pl.Series([], dtype=pl.Float64),
                "is_projection": pl.Series([], dtype=pl.Boolean),
                "ctl_forecast": pl.Series([], dtype=pl.Float64),
                "atl_forecast": pl.Series([], dtype=pl.Float64),
                "tsb_forecast": pl.Series([], dtype=pl.Float64),
            }
        )

    @staticmethod
    def _empty_training_load_figure() -> go.Figure:
        fig = go.Figure()
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
            autosize=True,
            height=700,
            annotations=[
                {
                    "text": "No cycling data",
                    "showarrow": False,
                    "xref": "paper",
                    "yref": "paper",
                    "x": 0.5,
                    "y": 0.5,
                    "font": {"size": 14, "color": "#888"},
                }
            ],
        )
        return fig

    def summarize_cycling(self, group_by="year") -> pl.DataFrame:
        """Summarize cycling rides by year, month, or week with rides, miles, time, and TSS."""
        df = self.cycling.clone()

        if df.is_empty():
            return df

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
        if self.cycling.is_empty() or "timestamp" not in self.cycling.columns:
            return self._empty_daily_tss()

        df = self.cycling
        ts_dtype = df["timestamp"].dtype
        if getattr(ts_dtype, "time_zone", None) is None:
            df = df.with_columns(pl.col("timestamp").dt.replace_time_zone("UTC"))

        fallback_cols = {
            "total_timer_time",
            "normalized_power",
            "intensity_factor",
            "threshold_power",
        }
        has_training_stress_score = "training_stress_score" in df.columns
        has_fallback = fallback_cols.issubset(df.columns)

        fallback_tss = (
            (
                pl.col("total_timer_time")
                * pl.col("normalized_power")
                * pl.col("intensity_factor")
            )
            / (pl.col("threshold_power") * 3600)
            * 100
        )

        if has_training_stress_score and has_fallback:
            tss_expr = (
                pl.when(pl.col("training_stress_score").is_not_null())
                .then(pl.col("training_stress_score"))
                .otherwise(fallback_tss)
                .alias("tss")
            )
        elif has_training_stress_score:
            tss_expr = pl.col("training_stress_score").alias("tss")
        elif has_fallback:
            tss_expr = fallback_tss.alias("tss")
        else:
            return self._empty_daily_tss()

        df = df.with_columns(
            pl.col("timestamp")
            .dt.convert_time_zone("America/Denver")
            .dt.date()
            .alias("date"),
            tss_expr,
        )

        daily_tss = df.group_by("date").agg(pl.col("tss").sum()).sort("date")
        if daily_tss.is_empty() or "date" not in daily_tss.columns:
            return self._empty_daily_tss()

        # Expand to full date range + 60-day projection, filling gaps with 0
        min_date = daily_tss["date"].min()
        if min_date is None:
            return self._empty_daily_tss()
        today = date.today()
        projection_end = today + timedelta(days=60)

        all_dates = pl.DataFrame(
            {"date": pl.date_range(min_date, projection_end, eager=True)}
        )

        daily_tss = all_dates.join(daily_tss, on="date", how="left").with_columns(
            pl.col("tss").fill_null(0.0),
            (pl.col("date") > today).alias("is_projection"),
        )

        return daily_tss.select("date", "tss", "is_projection")

    def compute_ctl_atl(self, ctl_days: int = 42, atl_days: int = 7) -> pl.DataFrame:
        daily_tss = self.compute_daily_tss()
        if daily_tss.is_empty() or "tss" not in daily_tss.columns:
            return self._empty_ctl_atl()

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
        ).select("date", "tss", "is_projection", "ctl", "atl", "tsb")

    def compute_ctl_atl_forecast(
        self,
        ctl_days: int = 42,
        atl_days: int = 7,
        lookback_days: int = 42,
        tss_overrides: dict[str, float] | None = None,
    ) -> pl.DataFrame:
        daily_tss = self.compute_daily_tss()
        if daily_tss.is_empty() or "tss" not in daily_tss.columns:
            return self._empty_forecast()

        # Calculate average daily TSS from the last N actual (non-projection) days
        actual = daily_tss.filter(~pl.col("is_projection"))
        recent = actual.tail(lookback_days)
        avg_tss = recent["tss"].mean()
        if avg_tss is None:
            avg_tss = 0.0

        override_by_date: dict[date, float] = {}
        if tss_overrides:
            for day_str, tss_value in tss_overrides.items():
                try:
                    override_day = date.fromisoformat(day_str)
                    override_by_date[override_day] = float(tss_value)
                except (TypeError, ValueError):
                    continue

        # Replace projection-day TSS with the historical average
        if override_by_date:
            override_dates = list(override_by_date.keys())
            override_values = [override_by_date[d] for d in override_dates]
            override_df = pl.DataFrame(
                {
                    "date": pl.Series(override_dates, dtype=pl.Date),
                    "tss_override": pl.Series(override_values, dtype=pl.Float64),
                }
            )
            forecast_tss = daily_tss.join(
                override_df, on="date", how="left"
            ).with_columns(
                pl.when(pl.col("is_projection") & pl.col("tss_override").is_not_null())
                .then(pl.col("tss_override"))
                .when(pl.col("is_projection"))
                .then(pl.lit(avg_tss))
                .otherwise(pl.col("tss"))
                .alias("tss_forecast")
            )
        else:
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
        self,
        start_date=None,
        include_forecast: bool = True,
        tss_overrides: dict[str, float] | None = None,
        ctl_atl_df: pl.DataFrame | None = None,
        forecast_df: pl.DataFrame | None = None,
    ) -> go.Figure:
        df = ctl_atl_df if ctl_atl_df is not None else self.compute_ctl_atl()
        if df.is_empty() or "date" not in df.columns:
            return self._empty_training_load_figure()

        if include_forecast:
            df_forecast = (
                forecast_df
                if forecast_df is not None
                else self.compute_ctl_atl_forecast(tss_overrides=tss_overrides)
            )

        if start_date is not None:
            if isinstance(start_date, str):
                start_date = date.fromisoformat(start_date)
            df = df.filter(pl.col("date") >= start_date)
            if include_forecast:
                df_forecast = df_forecast.filter(pl.col("date") >= start_date)

        if df.is_empty():
            return self._empty_training_load_figure()

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
        if projection_start is not None and len(dates) > 0:
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
            autosize=True,
            height=700,
        )

        return fig
