from datetime import date, timedelta
from threading import Lock

import numpy as np
import polars as pl
import statsmodels.formula.api as smf

from ..schemas import load_records
from ..storage import storage

_CP_BOOTSTRAP_LOCK = Lock()


class CpModelMixin:
    """Critical power estimation, best power curve, and CP over time."""

    # Dense durations for CP model fitting: every second from 2min–20min
    CURVE_DURATIONS = list(range(120, 1201))

    def get_best_power_curve(
        self, period_months=None, as_of=None, chart=False, _power_curves_df=None
    ):
        """Get best power curve from the precomputed cache.

        Args:
            period_months: Rolling lookback window.
            as_of: Reference date for the window.
            chart: If True, also compute sparse CHART_DURATIONS outside the
                   cached 2-20min range (for display on the power curve chart).
            _power_curves_df: Pre-loaded power curves DataFrame. If provided,
                              skips reading from disk/S3 (used by cp_over_time
                              to avoid re-reading the file for every month).
        """
        cache_path = storage.path_join(self.mergedfiles_path, "power_curves.parquet")

        if _power_curves_df is None:
            if not storage.path_exists(cache_path):
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

        full_cache = (
            _power_curves_df
            if _power_curves_df is not None
            else storage.read_parquet(cache_path)
        )
        cache = full_cache.filter(pl.col("source_file").is_in(source_files))

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
                records_path = storage.path_join(
                    self.mergedfiles_path, "record_mesgs.parquet"
                )
                if storage.path_exists(records_path):
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

    def estimate_critical_power(
        self, period_months=None, as_of=None, _power_curves_df=None
    ):
        curve = self.get_best_power_curve(
            period_months, as_of, _power_curves_df=_power_curves_df
        )
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
        cache_path = storage.path_join(self.mergedfiles_path, "power_curves.parquet")
        cache_version = (
            f"{storage.path_mtime(cache_path):.3f}"
            if storage.path_exists(cache_path)
            else "missing"
        )
        # Cache key: user's merged path + power-curve version + period
        cache_key = (
            f"{self.mergedfiles_path}:cp_over_time:{cache_version}:{period_months}"
        )
        cached = storage.get_compute_cache(cache_key)
        if cached is not None:
            return cached
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

        # Load power curves once for the entire loop
        power_curves_df = (
            storage.read_parquet(cache_path)
            if storage.path_exists(cache_path)
            else None
        )

        # Generate monthly sample points
        dates = []
        cp_vals = []
        wprime_vals = []
        r2_vals = []

        current = date(start.year, start.month, 1)
        end = date(max_date.year, max_date.month, 1)

        while current <= end:
            result = self.estimate_critical_power(
                period_months=period_months,
                as_of=current,
                _power_curves_df=power_curves_df,
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

        result = {
            "dates": dates,
            "cp": cp_vals,
            "wprime_kj": wprime_vals,
            "r2": r2_vals,
        }
        storage.set_compute_cache(cache_key, result)
        return result

    # Durations to regress against covariates (seconds -> label)
    PEAK_REGRESSION_DURATIONS = {120: "2min", 300: "5min", 1200: "20min"}

    # Sleep covariate name used in the model → column name in sleep.parquet
    SLEEP_COVARIATE_COL = "sleep_score"
    SLEEP_PARQUET_COL = "score_overall"

    # ── Sleep covariate helper ────────────────────────────────────────────────

    def _build_sleep_covariates(self, rides_df: pl.DataFrame) -> pl.DataFrame:
        """Compute monthly mean sleep score weighted by ride-days.

        For each ride, looks up the Garmin sleep score for the night immediately
        before the ride (calendar_date == ride_date - 1 day), then aggregates to
        monthly means across all rides in that month.

        Args:
            rides_df: Per-ride DataFrame with columns source_file, timestamp
                      (tz-aware America/Denver), month (Date).

        Returns:
            DataFrame with columns [month, sleep_score], or empty DataFrame
            if sleep.parquet is missing or has no overlap.
        """
        sleep_path = storage.path_join(self.mergedfiles_path, "sleep.parquet")
        if not storage.path_exists(sleep_path):
            return pl.DataFrame()

        sleep = storage.read_parquet(sleep_path)
        sleep = (
            sleep.filter(pl.col("calendar_date").is_not_null())
            .with_columns(pl.col("calendar_date").str.to_date().alias("calendar_date"))
            .select(["calendar_date", self.SLEEP_PARQUET_COL])
        )
        if sleep.is_empty():
            return pl.DataFrame()

        # For each ride, look up the night before (ride_date - 1)
        rides_with_date = rides_df.select(
            ["source_file", "month", "timestamp"]
        ).with_columns(
            (pl.col("timestamp").dt.date() - pl.duration(days=1)).alias("prior_night")
        )

        per_ride = rides_with_date.join(
            sleep.rename({"calendar_date": "prior_night"}),
            on="prior_night",
            how="left",
        )

        monthly = (
            per_ride.group_by("month")
            .agg(pl.col(self.SLEEP_PARQUET_COL).mean().alias(self.SLEEP_COVARIATE_COL))
            .sort("month")
        )

        return monthly

    def cp_covariate_analysis(self, include_sleep: bool = False) -> dict:
        """Regress monthly best peak powers at key durations against covariates.

        Uses raw peak powers (2min, 5min, 20min) as dependent variables
        instead of derived CP/W', avoiding the artifact where the 2-param
        model forces CP and W' to trade off against each other.

        All continuous covariates are mean-centered so the intercept reads as
        predicted peak power for an average month.

        Args:
            include_sleep: When True, joins the prior-night Garmin sleep score
                as an additional covariate. Only months with both power and
                sleep data are included (inner join).

        Covariates:
            tss_per_100   — monthly TSS sum / 100  (W per 100 TSS)
            ascent_per_1k — monthly ascent sum / 1000m  (W per 1000m climbed)
            sleep_score   — mean prior-night Garmin sleep score [optional]
        """
        cache_path = storage.path_join(self.mergedfiles_path, "power_curves.parquet")
        if not storage.path_exists(cache_path):
            return {"models": None, "data": None}

        df = self.cycling.clone()
        if df.is_empty():
            return {"models": None, "data": None}

        ts_col = "timestamp"
        if df[ts_col].dtype.time_zone is None:
            df = df.with_columns(pl.col(ts_col).dt.replace_time_zone("UTC"))
        df = df.with_columns(
            pl.col(ts_col).dt.convert_time_zone("America/Denver"),
        )

        # Load power curves and join with session timestamps + training data.
        # source_file is Categorical in the cache; cast to String so it joins
        # with self.cycling (session_mesgs) which has source_file as String.
        curves = storage.read_parquet(cache_path).with_columns(
            pl.col("source_file").cast(pl.String)
        )
        dur_cols = {
            f"d_{d}": label for d, label in self.PEAK_REGRESSION_DURATIONS.items()
        }
        available_dur_cols = {
            c: la for c, la in dur_cols.items() if c in curves.columns
        }
        if not available_dur_cols:
            return {"models": None, "data": None}

        rides = (
            df.select(["source_file", ts_col, "training_stress_score"])
            .join(
                curves.select(["source_file"] + list(available_dur_cols.keys())),
                on="source_file",
                how="inner",
            )
            .with_columns(
                pl.col(ts_col).dt.truncate("1mo").dt.date().alias("month"),
            )
        )

        # Monthly aggregates — best peak power + scaled training covariate
        monthly = (
            rides.group_by("month")
            .agg(
                [pl.col(c).max().alias(c) for c in available_dur_cols]
                + [
                    (pl.col("training_stress_score").sum() / 100.0).alias(
                        "tss_per_100"
                    ),
                ]
            )
            .sort("month")
        )

        combined = monthly.clone()

        # Optionally join sleep covariates (inner — only months with both power + sleep)
        if include_sleep:
            sleep_monthly = self._build_sleep_covariates(rides)
            if sleep_monthly.is_empty():
                include_sleep = False  # graceful fallback
            else:
                combined = combined.join(sleep_monthly, on="month", how="inner")

        combined = combined.drop_nulls()

        if len(combined) < 5:
            return {"models": None, "data": None}

        covariate_names = ["tss_per_100"]
        if include_sleep:
            covariate_names.append(self.SLEEP_COVARIATE_COL)

        usable = []
        for c in covariate_names:
            col = combined[c].drop_nulls()
            if len(col) == len(combined) and col.std() > 0:
                usable.append(c)

        if len(usable) < 1:
            return {"models": None, "data": None}

        # Mean-center all continuous covariates so the intercept reads as
        # "predicted peak power for a perfectly average month" rather than
        # the nonsensical prediction at TSS=0 / sleep_score=0.
        for c in usable:
            combined = combined.with_columns((pl.col(c) - pl.col(c).mean()).alias(c))

        pdf = combined.select(
            ["month"] + list(available_dur_cols.keys()) + usable
        ).to_pandas()

        # Load cached bootstrap results if available
        cache_key = "sleep" if include_sleep else "no_sleep"
        boot_cache_path = storage.path_join(
            self.mergedfiles_path, "cp_covariate_bootstrap.json"
        )
        boot_cache = {}
        if storage.path_exists(boot_cache_path):
            try:
                full_cache = storage.read_json(boot_cache_path)
                if cache_key in full_cache:
                    boot_cache = full_cache[cache_key]
            except Exception as e:
                print(f"  CP covariate cache read error: {e}; using OLS intervals")

        def _fit_ols(dep_col, pdf):
            formula = f"{dep_col} ~ {' + '.join(usable)}"
            model = smf.ols(formula, data=pdf).fit(
                cov_type="HAC", cov_kwds={"maxlags": 3}
            )
            param_names = list(model.params.index)

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

    def refresh_cp_covariate_bootstrap(
        self, n_bootstrap: int = 5000, only_if_stale: bool = False
    ) -> None:
        """Run bootstrap resampling for CP covariate models and cache results.

        Runs for no_sleep and sleep variants and stores under keys:
        no_sleep, sleep.
        """
        with _CP_BOOTSTRAP_LOCK:
            if only_if_stale and hasattr(self, "_bootstrap_cache_is_stale"):
                if not self._bootstrap_cache_is_stale():
                    return

            cache_path = storage.path_join(
                self.mergedfiles_path, "power_curves.parquet"
            )
            if not storage.path_exists(cache_path):
                return

            df = self.cycling.clone()
            if df.is_empty():
                return

            ts_col = "timestamp"
            if df[ts_col].dtype.time_zone is None:
                df = df.with_columns(pl.col(ts_col).dt.replace_time_zone("UTC"))
            df = df.with_columns(
                pl.col(ts_col).dt.convert_time_zone("America/Denver"),
            )

            # Cast source_file Categorical → String to match session_mesgs dtype.
            curves = storage.read_parquet(cache_path).with_columns(
                pl.col("source_file").cast(pl.String)
            )
            dur_cols = {
                f"d_{d}": label for d, label in self.PEAK_REGRESSION_DURATIONS.items()
            }
            available_dur_cols = {
                c: la for c, la in dur_cols.items() if c in curves.columns
            }
            if not available_dur_cols:
                return

            rides = (
                df.select(["source_file", ts_col, "training_stress_score"])
                .join(
                    curves.select(["source_file"] + list(available_dur_cols.keys())),
                    on="source_file",
                    how="inner",
                )
                .with_columns(
                    pl.col(ts_col).dt.truncate("1mo").dt.date().alias("month"),
                )
            )

            base_monthly = (
                rides.group_by("month")
                .agg(
                    [pl.col(c).max().alias(c) for c in available_dur_cols]
                    + [
                        (pl.col("training_stress_score").sum() / 100.0).alias(
                            "tss_per_100"
                        ),
                    ]
                )
                .sort("month")
            )

            sleep_monthly = self._build_sleep_covariates(rides)

            out_path = storage.path_join(
                self.mergedfiles_path, "cp_covariate_bootstrap.json"
            )
            if storage.path_exists(out_path):
                try:
                    full_boot_cache = storage.read_json(out_path)
                except Exception:
                    full_boot_cache = {}
            else:
                full_boot_cache = {}

            updated_variants = []
            for with_sleep in (False, True):
                cache_key = "sleep" if with_sleep else "no_sleep"

                if with_sleep and sleep_monthly.is_empty():
                    continue

                if with_sleep:
                    combined = base_monthly.join(sleep_monthly, on="month", how="inner")
                else:
                    combined = base_monthly.clone()

                combined = combined.drop_nulls()

                if len(combined) < 5:
                    continue

                covariate_names = ["tss_per_100"]
                if with_sleep:
                    covariate_names.append(self.SLEEP_COVARIATE_COL)

                usable = []
                for c in covariate_names:
                    col = combined[c].drop_nulls()
                    if len(col) == len(combined) and col.std() > 0:
                        usable.append(c)
                if len(usable) < 1:
                    continue

                # Mean-center covariates (mirrors cp_covariate_analysis)
                for c in usable:
                    combined = combined.with_columns(
                        (pl.col(c) - pl.col(c).mean()).alias(c)
                    )

                pdf = combined.select(
                    ["month"] + list(available_dur_cols.keys()) + usable
                ).to_pandas()

                n = len(pdf)
                rng = np.random.default_rng(42)
                variant_cache = {}

                for dep_col in available_dur_cols:
                    y_full = pdf[dep_col].values.astype(float)

                    formula = f"{dep_col} ~ {' + '.join(usable)}"
                    model = smf.ols(formula, data=pdf).fit()
                    param_names = list(model.params.index)
                    n_params = len(param_names)

                    X_full = np.column_stack(
                        [np.ones(n)] + [pdf[c].values.astype(float) for c in usable]
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
                        p_boot = 2 * float(
                            np.mean(boot_dist <= 0)
                            if point >= 0
                            else np.mean(boot_dist >= 0)
                        )
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

                    variant_cache[dep_col] = {
                        "n_bootstrap": int(np.sum(valid)),
                        "coefficients": coefs,
                    }

                full_boot_cache[cache_key] = variant_cache
                updated_variants.append(f"{cache_key}={len(variant_cache)}")

            if not updated_variants:
                print("  CP covariates update skipped: not enough monthly data")
                return

            storage.write_json(out_path, full_boot_cache)
            variants_text = ", ".join(updated_variants)
            print(
                f"  CP covariates updated ({variants_text}, n_bootstrap={n_bootstrap})"
            )
