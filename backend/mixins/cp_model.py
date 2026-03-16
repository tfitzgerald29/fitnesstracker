import json
import os
from datetime import date, timedelta

import numpy as np
import polars as pl
import statsmodels.formula.api as smf

from ..schemas import load_records


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
                    records = load_records(
                        "cycling",
                        records_path,
                        source_files=list(source_files),
                        columns=["source_file", "power", "timestamp"],
                    ).sort("source_file", "timestamp")
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

    def cp_covariate_analysis(self, lag_months: int = 3) -> dict:
        """Regress monthly best peak powers at key durations against covariates.

        Uses raw peak powers (2min, 5min, 20min) as dependent variables
        instead of derived CP/W', avoiding the artifact where the 2-param
        model forces CP and W' to trade off against each other.

        Args:
            lag_months: Number of months to lag CTL (default 3).

        Covariates: lagged CTL + season dummies (winter=reference).
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
        ctl_col = f"ctl_lag{lag_months}"
        combined = combined.with_columns(
            pl.col("ctl").shift(lag_months).alias(ctl_col),
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

        covariate_names = [ctl_col, "spring", "summer", "fall"]
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

        # Load cached bootstrap results if available
        boot_cache_path = os.path.join(
            self.mergedfiles_path, "cp_covariate_bootstrap.json"
        )
        boot_cache = {}
        if os.path.exists(boot_cache_path):
            with open(boot_cache_path) as f:
                full_cache = json.load(f)
            # Support both new keyed format and legacy flat format
            lag_key = f"lag{lag_months}"
            if lag_key in full_cache:
                boot_cache = full_cache[lag_key]
            elif not any(k.startswith("lag") for k in full_cache):
                # Legacy flat format (pre-lag support), only valid for lag=3
                if lag_months == 3:
                    boot_cache = full_cache

        def _fit_ols(dep_col, pdf):
            formula = f"{dep_col} ~ {formula_rhs}"
            model = smf.ols(formula, data=pdf).fit(
                cov_type="HAC", cov_kwds={"maxlags": lag_months}
            )
            param_names = list(model.params.index)

            # Use bootstrap CIs from cache if available
            boot_result = boot_cache.get(dep_col)

            coefs = []
            for name in param_names:
                point = float(model.params[name])
                entry = {
                    "name": "const" if name == "Intercept" else name,
                    "coef": round(point, 4),
                    "pvalue": round(float(model.pvalues[name]), 4),
                    "ci_low": round(float(model.conf_int().loc[name][0]), 4),
                    "ci_high": round(float(model.conf_int().loc[name][1]), 4),
                }
                # Overlay bootstrap results if cached
                if boot_result:
                    key = "const" if name == "Intercept" else name
                    for bc in boot_result["coefficients"]:
                        if bc["name"] == key:
                            entry["pvalue_boot"] = bc["pvalue_boot"]
                            entry["ci_low"] = bc["ci_low"]
                            entry["ci_high"] = bc["ci_high"]
                            entry["coef_median"] = bc["coef_median"]
                            entry["coef_mean"] = bc["coef_mean"]
                            entry["ci_method"] = "bootstrap"
                            break
                coefs.append(entry)

            result = {
                "r2": round(float(model.rsquared), 3),
                "r2_adj": round(float(model.rsquared_adj), 3),
                "f_pvalue": round(float(model.f_pvalue), 4),
                "n": int(model.nobs),
                "coefficients": coefs,
            }
            if boot_result:
                result["n_bootstrap"] = boot_result.get("n_bootstrap", 0)
            return result

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

    def refresh_cp_covariate_bootstrap(self, n_bootstrap: int = 5000) -> None:
        """Run bootstrap resampling for CP covariate models and cache results.

        Runs for each lag variant (2 and 3 months) and stores under
        separate keys in the JSON cache.
        """
        cache_path = os.path.join(self.mergedfiles_path, "power_curves.parquet")
        if not os.path.exists(cache_path):
            return

        ctl_atl = self.compute_ctl_atl()
        df = self.cycling.clone()
        if df.is_empty():
            return

        ts_col = "timestamp"
        if df[ts_col].dtype.time_zone is None:
            df = df.with_columns(pl.col(ts_col).dt.replace_time_zone("UTC"))
        df = df.with_columns(
            pl.col(ts_col).dt.convert_time_zone("America/Denver"),
        )

        curves = pl.read_parquet(cache_path)
        dur_cols = {
            f"d_{d}": label for d, label in self.PEAK_REGRESSION_DURATIONS.items()
        }
        available_dur_cols = {
            c: la for c, la in dur_cols.items() if c in curves.columns
        }
        if not available_dur_cols:
            return

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

        monthly_peaks = rides.group_by("month").agg(
            [pl.col(c).max().alias(c) for c in available_dur_cols]
        )

        monthly_ctl = (
            ctl_atl.filter(~pl.col("is_projection"))
            .with_columns(pl.col("date").dt.truncate("1mo").alias("month"))
            .group_by("month")
            .agg(pl.col("ctl").last().alias("ctl"))
        )

        base_combined = monthly_peaks.join(monthly_ctl, on="month", how="left").sort(
            "month"
        )

        # Add season dummies once
        base_combined = base_combined.with_columns(
            pl.col("month").dt.month().alias("mo_num"),
        )
        base_combined = base_combined.with_columns(
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

        full_boot_cache = {}

        for lag in (2, 3):
            ctl_col = f"ctl_lag{lag}"
            combined = base_combined.with_columns(
                pl.col("ctl").shift(lag).alias(ctl_col),
            ).drop_nulls()

            if len(combined) < 5:
                continue

            covariate_names = [ctl_col, "spring", "summer", "fall"]
            usable = []
            for c in covariate_names:
                col = combined[c].drop_nulls()
                if len(col) == len(combined) and col.std() > 0:
                    usable.append(c)
            if len(usable) < 1:
                continue

            pdf = combined.select(
                ["month"] + list(available_dur_cols.keys()) + usable
            ).to_pandas()

            formula_rhs = " + ".join(usable)
            cov_cols = usable
            n = len(pdf)
            rng = np.random.default_rng(42)

            lag_cache = {}
            for dep_col in available_dur_cols:
                formula = f"{dep_col} ~ {formula_rhs}"
                model = smf.ols(formula, data=pdf).fit()
                param_names = list(model.params.index)
                n_params = len(param_names)

                y_full = pdf[dep_col].values.astype(float)
                X_full = np.column_stack(
                    [np.ones(n)] + [pdf[c].values.astype(float) for c in cov_cols]
                )

                boot_coefs = np.empty((n_bootstrap, n_params))
                for b in range(n_bootstrap):
                    idx = rng.integers(0, n, size=n)
                    try:
                        boot_coefs[b] = np.linalg.lstsq(
                            X_full[idx], y_full[idx], rcond=None
                        )[0]
                    except np.linalg.LinAlgError:
                        boot_coefs[b] = np.nan

                valid = ~np.isnan(boot_coefs).any(axis=1)
                boot_coefs = boot_coefs[valid]

                coefs = []
                for i, name in enumerate(param_names):
                    boot_dist = boot_coefs[:, i]
                    ci_low, ci_high = (
                        float(np.percentile(boot_dist, 2.5)),
                        float(np.percentile(boot_dist, 97.5)),
                    )
                    point = float(model.params[name])
                    if point >= 0:
                        p_boot = 2 * float(np.mean(boot_dist <= 0))
                    else:
                        p_boot = 2 * float(np.mean(boot_dist >= 0))
                    p_boot = min(p_boot, 1.0)

                    coefs.append(
                        {
                            "name": "const" if name == "Intercept" else name,
                            "coef_median": round(float(np.median(boot_dist)), 4),
                            "coef_mean": round(float(np.mean(boot_dist)), 4),
                            "pvalue_boot": round(p_boot, 4),
                            "ci_low": round(ci_low, 4),
                            "ci_high": round(ci_high, 4),
                        }
                    )

                lag_cache[dep_col] = {
                    "n_bootstrap": int(np.sum(valid)),
                    "coefficients": coefs,
                }

            full_boot_cache[f"lag{lag}"] = lag_cache

        out_path = os.path.join(self.mergedfiles_path, "cp_covariate_bootstrap.json")
        with open(out_path, "w") as f:
            json.dump(full_boot_cache, f, indent=2)
