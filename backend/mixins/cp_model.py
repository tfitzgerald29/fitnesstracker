import os
from datetime import date, timedelta

import numpy as np
import polars as pl
import statsmodels.formula.api as smf


class CpModelMixin:
    """Critical power estimation, best power curve, and CP over time."""

    # Dense durations for CP model fitting: every second from 2min–20min
    CURVE_DURATIONS = list(range(120, 1201))

    def get_best_power_curve(self, period_months=None, as_of=None, chart=False):
        """Get best power curve from the precomputed cache.

        Args:
            period_months: Rolling lookback window.
            as_of: Reference date for the window.
            chart: If True, also compute sparse CHART_DURATIONS outside the
                   cached 2-20min range (for display on the power curve chart).
        """
        cache_path = os.path.join(self.mergedfiles_path, "power_curves.parquet")
        if not os.path.exists(cache_path):
            return {"durations": [], "watts": []}

        df = self.cycling.clone()
        if df.is_empty():
            return {"durations": [], "watts": []}

        from datetime import date

        ref_date = as_of or date.today()
        if period_months is not None:
            cutoff = ref_date - timedelta(days=period_months * 30)
            df = df.filter(
                (pl.col("timestamp").dt.date() >= cutoff)
                & (pl.col("timestamp").dt.date() <= ref_date)
            )

        source_files = set(df["source_file"].unique().to_list())
        if not source_files:
            return {"durations": [], "watts": []}

        cache = pl.read_parquet(cache_path).filter(
            pl.col("source_file").is_in(source_files)
        )

        if cache.is_empty():
            return {"durations": [], "watts": []}

        # Take max across all rides for each duration column
        dur_cols = [f"d_{d}" for d in self.CURVE_DURATIONS]
        available_cols = [c for c in dur_cols if c in cache.columns]

        best = cache.select(available_cols).max()

        best_by_duration = {}
        for col in available_cols:
            val = best[col][0]
            if val is not None:
                best_by_duration[int(col[2:])] = float(val)

        # For chart display, compute extra durations outside the cached range
        if chart:
            extra_durations = [
                d for d in self.CHART_DURATIONS if d not in best_by_duration
            ]
            if extra_durations:
                records_path = os.path.join(
                    self.mergedfiles_path, "record_mesgs.parquet"
                )
                if os.path.exists(records_path):
                    records = (
                        pl.read_parquet(
                            records_path, columns=["source_file", "power", "timestamp"]
                        )
                        .filter(pl.col("source_file").is_in(source_files))
                        .sort("source_file", "timestamp")
                    )
                    for _, group in records.group_by("source_file"):
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
                        ride_best = self._best_power_for_durations(
                            power, extra_durations
                        )
                        for d, w in ride_best.items():
                            if d not in best_by_duration or w > best_by_duration[d]:
                                best_by_duration[d] = w

        # For chart mode, only return the sparse CHART_DURATIONS for a smooth line
        if chart:
            chart_durations = [d for d in self.CHART_DURATIONS if d in best_by_duration]
            chart_watts = [best_by_duration[d] for d in chart_durations]
            return {"durations": chart_durations, "watts": chart_watts}

        durations = sorted(best_by_duration.keys())
        watts = [best_by_duration[d] for d in durations]
        return {"durations": durations, "watts": watts}

    def estimate_critical_power(self, period_months=None, as_of=None):
        curve = self.get_best_power_curve(period_months, as_of)
        durations = curve["durations"]
        watts = curve["watts"]

        if len(durations) < 3:
            return {
                "cp": None,
                "wprime_kj": None,
                "r2": None,
                "durations": [],
                "watts": [],
                "fit_durations": [],
                "fitted_watts": [],
            }

        # Only 2–20 min durations
        fit_pairs = [(t, w) for t, w in zip(durations, watts) if 120 <= t <= 1200]
        if len(fit_pairs) < 3:
            return {
                "cp": None,
                "wprime_kj": None,
                "r2": None,
                "durations": [],
                "watts": [],
                "fit_durations": [],
                "fitted_watts": [],
            }

        fit_t = [p[0] for p in fit_pairs]
        fit_w = [p[1] for p in fit_pairs]

        x = np.array([1.0 / t for t in fit_t])
        y = np.array(fit_w)

        # Linear regression
        A = np.vstack([x, np.ones(len(x))]).T
        wprime_j, cp = np.linalg.lstsq(A, y, rcond=None)[0]

        if wprime_j < 0:
            return {
                "cp": None,
                "wprime_kj": None,
                "r2": None,
                "durations": [],
                "watts": [],
                "fit_durations": [],
                "fitted_watts": [],
            }

        fitted_watts = cp + wprime_j / np.array(fit_t)

        # R²
        ss_res = np.sum((y - fitted_watts) ** 2)
        ss_tot = np.sum((y - np.mean(y)) ** 2)
        r2 = 1 - ss_res / ss_tot if ss_tot != 0 else None

        return {
            "cp": round(cp),
            "wprime_kj": round(wprime_j / 1000, 1),
            "r2": round(r2, 3) if r2 is not None else None,
            "durations": durations,
            "watts": watts,
            "fit_durations": fit_t,
            "fitted_watts": [round(w, 1) for w in fitted_watts],
        }

    def cp_over_time(self, period_months: int) -> dict:
        """Compute CP and W' at monthly intervals using a rolling window.

        Args:
            period_months: Rolling lookback window for each data point.

        Returns:
            {dates: [], cp: [], wprime_kj: [], r2: []}
        """

        df = self.cycling.clone()
        if df.is_empty():
            return {"dates": [], "cp": [], "wprime_kj": [], "r2": []}

        ts_col = "timestamp"
        if df[ts_col].dtype.time_zone is None:
            df = df.with_columns(pl.col(ts_col).dt.replace_time_zone("UTC"))

        # Get the date range of rides
        min_date = df[ts_col].dt.date().min()
        max_date = df[ts_col].dt.date().max()

        # Start from period_months after first ride so we have enough data
        start = min_date + timedelta(days=period_months * 30)
        if start > max_date:
            return {"dates": [], "cp": [], "wprime_kj": [], "r2": []}

        # Generate monthly sample points
        dates = []
        cp_vals = []
        wprime_vals = []
        r2_vals = []

        current = date(start.year, start.month, 1)
        end = date(max_date.year, max_date.month, 1)

        while current <= end:
            result = self.estimate_critical_power(
                period_months=period_months, as_of=current
            )
            if result["cp"] is not None:
                dates.append(current.isoformat())
                cp_vals.append(result["cp"])
                wprime_vals.append(result["wprime_kj"])
                r2_vals.append(result["r2"])

            # Next month
            if current.month == 12:
                current = date(current.year + 1, 1, 1)
            else:
                current = date(current.year, current.month + 1, 1)

        return {"dates": dates, "cp": cp_vals, "wprime_kj": wprime_vals, "r2": r2_vals}

    # Durations to regress against covariates (seconds -> label)
    PEAK_REGRESSION_DURATIONS = {120: "2min", 300: "5min", 1200: "20min"}

    def cp_covariate_analysis(self) -> dict:
        """Regress monthly best peak powers at key durations against covariates.

        Uses raw peak powers (2min, 5min, 20min) as dependent variables
        instead of derived CP/W', avoiding the artifact where the 2-param
        model forces CP and W' to trade off against each other.

        Covariates: 3-month lagged CTL + season dummies (winter=reference).
        """
        cache_path = os.path.join(self.mergedfiles_path, "power_curves.parquet")
        if not os.path.exists(cache_path):
            return {"models": None, "data": None}

        ctl_atl = self.compute_ctl_atl()
        df = self.cycling.clone()
        if df.is_empty():
            return {"models": None, "data": None}

        ts_col = "timestamp"
        if df[ts_col].dtype.time_zone is None:
            df = df.with_columns(pl.col(ts_col).dt.replace_time_zone("UTC"))
        df = df.with_columns(
            pl.col(ts_col).dt.convert_time_zone("America/Denver"),
        )

        # Load power curves and join with session timestamps
        curves = pl.read_parquet(cache_path)
        dur_cols = {
            f"d_{d}": label for d, label in self.PEAK_REGRESSION_DURATIONS.items()
        }
        available_dur_cols = {
            c: la for c, la in dur_cols.items() if c in curves.columns
        }
        if not available_dur_cols:
            return {"models": None, "data": None}

        rides = (
            df.select("source_file", ts_col)
            .join(
                curves.select(["source_file"] + list(available_dur_cols.keys())),
                on="source_file",
                how="inner",
            )
            .with_columns(
                pl.col(ts_col).dt.truncate("1mo").dt.date().alias("month"),
            )
        )

        # Monthly best peak power at each duration
        monthly_peaks = rides.group_by("month").agg(
            [pl.col(c).max().alias(c) for c in available_dur_cols]
        )

        # Monthly CTL snapshots (end-of-month value)
        monthly_ctl = (
            ctl_atl.filter(~pl.col("is_projection"))
            .with_columns(pl.col("date").dt.truncate("1mo").alias("month"))
            .group_by("month")
            .agg(pl.col("ctl").last().alias("ctl"))
        )

        # Join and sort
        combined = monthly_peaks.join(monthly_ctl, on="month", how="left").sort("month")

        # 3-month lagged CTL
        combined = combined.with_columns(
            pl.col("ctl").shift(3).alias("ctl_lag3"),
        )

        # Season dummies (winter=Dec-Feb is reference)
        combined = combined.with_columns(
            pl.col("month").dt.month().alias("mo_num"),
        )
        combined = combined.with_columns(
            pl.when(pl.col("mo_num").is_in([3, 4, 5]))
            .then(1)
            .otherwise(0)
            .alias("spring"),
            pl.when(pl.col("mo_num").is_in([6, 7, 8]))
            .then(1)
            .otherwise(0)
            .alias("summer"),
            pl.when(pl.col("mo_num").is_in([9, 10, 11]))
            .then(1)
            .otherwise(0)
            .alias("fall"),
        ).drop("mo_num")

        combined = combined.drop_nulls()

        if len(combined) < 5:
            return {"models": None, "data": None}

        covariate_names = ["ctl_lag3", "spring", "summer", "fall"]
        usable = []
        for c in covariate_names:
            col = combined[c].drop_nulls()
            if len(col) == len(combined) and col.std() > 0:
                usable.append(c)

        if len(usable) < 1:
            return {"models": None, "data": None}

        pdf = combined.select(
            ["month"] + list(available_dur_cols.keys()) + usable
        ).to_pandas()

        formula_rhs = " + ".join(usable)

        def _fit_ols(dep_col, pdf):
            formula = f"{dep_col} ~ {formula_rhs}"
            model = smf.ols(formula, data=pdf).fit(
                cov_type="HAC", cov_kwds={"maxlags": 3}
            )
            coefs = []
            for name in model.params.index:
                coefs.append(
                    {
                        "name": "const" if name == "Intercept" else name,
                        "coef": round(float(model.params[name]), 4),
                        "pvalue": round(float(model.pvalues[name]), 4),
                        "ci_low": round(float(model.conf_int().loc[name][0]), 4),
                        "ci_high": round(float(model.conf_int().loc[name][1]), 4),
                    }
                )
            return {
                "r2": round(float(model.rsquared), 3),
                "r2_adj": round(float(model.rsquared_adj), 3),
                "f_pvalue": round(float(model.f_pvalue), 4),
                "n": int(model.nobs),
                "coefficients": coefs,
            }

        # Fit a model for each duration
        models = {}
        dep_cols = []
        for col, label in available_dur_cols.items():
            models[label] = _fit_ols(col, pdf)
            dep_cols.append(col)

        # Correlation matrix
        corr_cols_map = {c: l for c, l in available_dur_cols.items()}
        corr_labels = list(corr_cols_map.values()) + usable
        corr_raw_cols = list(corr_cols_map.keys()) + usable
        corr_df = combined.select(corr_raw_cols).to_pandas()
        corr_df.columns = corr_labels
        corr_matrix = corr_df.corr().round(3)

        return {
            "models": models,
            "covariates": usable,
            "data": {
                "months": combined["month"].cast(pl.String).to_list(),
                **{
                    label: combined[col].to_list()
                    for col, label in available_dur_cols.items()
                },
                **{c: combined[c].to_list() for c in usable},
            },
            "correlation": {
                "columns": corr_labels,
                "values": corr_matrix.values.tolist(),
            },
        }
