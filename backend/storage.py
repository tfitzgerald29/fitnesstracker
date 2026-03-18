import os
from datetime import date, timedelta
from functools import cached_property
from threading import Lock

# ── Constants ──────────────────────────────────────────────────────────
_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Per-file date filter config: maps parquet filename suffix →
# (lookback_years, date_column, is_string).
# is_string=True means the column is stored as "YYYY-MM-DD" text (sleep.parquet).
# is_string=False means the column is a Datetime/Date dtype.
# record_mesgs is limited to 1 year — it's raw per-second telemetry and the
# dashboard never looks back further than 12 months for ride-detail views.
# Everything else uses 4 years to cover all aggregate charts.
_DATE_FILTERS: dict[str, tuple[int, str, bool]] = {
    "session_mesgs.parquet": (4, "timestamp", False),
    "record_mesgs.parquet": (1, "timestamp", False),
    "set_mesgs.parquet": (4, "timestamp", False),
    "activity_mesgs.parquet": (4, "timestamp", False),
    "split_mesgs.parquet": (4, "start_time", False),
    "sleep.parquet": (4, "calendar_date", True),
}

# Columns to keep when caching record_mesgs.parquet in memory.
# The full file has 35 columns (many >95% null); we only use these 10.
# Keeping just these reduces the in-memory footprint from ~1.3 GB to ~200 MB.
_RECORD_MESGS_KEEP_COLS = [
    "source_file",
    "timestamp",
    "power",
    "distance",
    "enhanced_altitude",
    "position_lat",  # kept as Float64 — GPS needs precision
    "position_long",  # kept as Float64 — GPS needs precision
    "enhanced_speed",
    "cadence",
    "heart_rate",
]

# Numeric columns in record_mesgs that can be downcast Float64 → Float32
# without meaningful loss (power, speed, altitude, cadence, HR are all
# well within Float32's ~7 significant decimal digits).
_RECORD_MESGS_FLOAT32_COLS = [
    "power",
    "distance",
    "enhanced_altitude",
    "enhanced_speed",
    "cadence",
    "heart_rate",
]

_MODE = os.environ.get("STORAGE_MODE", "local")  # "local" | "s3"
_S3_BUCKET = os.environ.get("S3_BUCKET", "")


# ── StorageConfig ──────────────────────────────────────────────────────
class StorageConfig:
    """Resolves storage paths for either local or S3 mode.

    Local mode (default):
        All paths resolve to the repo's local directories.
        Behaviour is identical to the pre-cloud codebase.

    S3 mode:
        Paths resolve to s3://<bucket>/<user_id>/... prefixes.
        Set STORAGE_MODE=s3 and S3_BUCKET=<your-bucket> in the environment.
        A user_id must be supplied to any method that returns a per-user path.

    Usage:
        storage = StorageConfig()

        # Path-only (no user isolation needed, e.g. local default)
        storage.merged_path()

        # Per-user path (required in S3 mode)
        storage.merged_path(user_id="abc123")
    """

    def __init__(self, mode: str | None = None, bucket: str | None = None):
        self.mode = mode or _MODE
        self.bucket = bucket or _S3_BUCKET
        self._parquet_cache: dict[str, object] = {}
        self._compute_cache: dict[str, object] = {}
        self._cache_lock = Lock()

    def is_s3(self) -> bool:
        return self.mode == "s3"

    def merged_path(self, user_id: str | None = None) -> str:
        if self.is_s3():
            _require_user_id(user_id)
            return f"s3://{self.bucket}/{user_id}/mergedfiles"
        return os.path.join(_BASE_DIR, "mergedfiles")

    def processed_path(self, user_id: str | None = None) -> str:
        if self.is_s3():
            _require_user_id(user_id)
            return f"s3://{self.bucket}/{user_id}/processedfiles"
        return os.path.join(_BASE_DIR, "processedfiles")

    def wellness_path(self, user_id: str | None = None) -> str:
        if self.is_s3():
            _require_user_id(user_id)
            return f"s3://{self.bucket}/{user_id}/sleepdata"
        return os.path.join(_BASE_DIR, "sleepdata")

    def wt_data_file(self, user_id: str | None = None) -> str:
        if self.is_s3():
            _require_user_id(user_id)
            return f"s3://{self.bucket}/{user_id}/weighttraining_data/weighttraining_data.json"
        return os.path.join(
            _BASE_DIR, "weighttraining_data", "weighttraining_data.json"
        )

    def wt_draft_file(self, user_id: str | None = None) -> str:
        if self.is_s3():
            _require_user_id(user_id)
            return (
                f"s3://{self.bucket}/{user_id}/weighttraining_data/draft_workout.json"
            )
        return os.path.join(_BASE_DIR, "weighttraining_data", "draft_workout.json")

    def path_join(self, base: str, *parts: str) -> str:
        """Join path segments for either a local path or an S3 URI.

        os.path.join corrupts s3:// URIs on some platforms, so S3 paths
        are joined with a simple '/' concatenation instead.
        """
        if base.startswith("s3://"):
            return "/".join([base.rstrip("/"), *parts])
        return os.path.join(base, *parts)

    def path_exists(self, path: str) -> bool:
        """Check whether a path exists locally or as an S3 object/prefix."""
        if path.startswith("s3://"):
            return self._s3fs.exists(path)
        return os.path.exists(path)

    def path_mtime(self, path: str) -> float:
        """Return the last-modified time as a Unix timestamp."""
        if path.startswith("s3://"):
            info = self._s3fs.info(path)
            return info["LastModified"].timestamp()
        return os.path.getmtime(path)

    def makedirs(self, path: str) -> None:
        """Create local directories. No-op for S3 (S3 has no real directories)."""
        if not path.startswith("s3://"):
            os.makedirs(path, exist_ok=True)

    def delete_file(self, path: str) -> None:
        """Delete a file from local disk or S3. Silent if not found."""
        try:
            if path.startswith("s3://"):
                self._s3fs.rm(path)
            else:
                os.remove(path)
        except FileNotFoundError:
            pass

    def read_parquet(self, path: str, **kwargs) -> object:
        """Read a parquet file, using an in-memory cache in S3 mode.

        In local mode, falls through to pl.read_parquet directly (fast disk).
        In S3 mode, caches the DataFrame so repeated callback calls don't
        re-fetch from S3 on every request.

        Column-filtered reads (``columns=`` kwarg): if the cached DataFrame
        already contains those columns, the filter is applied in-memory
        instead of going back to S3.  On a cache miss the full DataFrame is
        fetched, transformed (see below), cached, and then the column slice
        is returned — so every subsequent caller benefits.

        Transformations applied on first load (both local and S3 mode):

        Date filter (_LOOKBACK_YEARS, default 3):
            Rows older than ``today - LOOKBACK_YEARS * 365 days`` are dropped
            from session_mesgs, record_mesgs, set_mesgs, activity_mesgs,
            split_mesgs, and sleep.  This bounds memory and keeps charts
            focused on recent data.  Override by setting DATA_LOOKBACK_YEARS
            in the environment.  power_curves.parquet is not directly filtered
            here — stale ride rows are excluded because the corresponding
            source_files no longer appear in the trimmed session_mesgs cache.

        record_mesgs column slimming:
            Only the 10 columns used by the codebase are kept; numeric
            columns are downcast Float64 → Float32; source_file is cast to
            Categorical.  Reduces ~1.3 GB → ~185 MB in-memory.

        source_file Categorical:
            Both record_mesgs and power_curves cast source_file to Categorical
            so in-memory joins between them work without dtype errors.
        """
        import polars as pl

        columns = kwargs.pop("columns", None)

        # ── Cache lookup (S3 mode only) ───────────────────────────────────
        if self.is_s3():
            with self._cache_lock:
                cached = self._parquet_cache.get(path)

            if cached is not None:
                if columns is not None:
                    available = [c for c in columns if c in cached.columns]
                    return cached.select(available)
                return cached

        # ── Fetch from disk / S3 ─────────────────────────────────────────
        df = pl.read_parquet(path, **kwargs)

        # ── Date filter ───────────────────────────────────────────────────
        suffix = path.split("/")[-1]
        date_cfg = _DATE_FILTERS.get(suffix)
        if date_cfg is not None:
            years, date_col, is_string = date_cfg
            if date_col in df.columns:
                cutoff = date.today() - timedelta(days=years * 365)
                if is_string:
                    df = df.filter(pl.col(date_col) >= str(cutoff))
                else:
                    df = df.filter(pl.col(date_col).dt.date() >= cutoff)

        # ── record_mesgs column slimming + dtype optimisation ─────────────
        if suffix == "record_mesgs.parquet":
            keep = [c for c in _RECORD_MESGS_KEEP_COLS if c in df.columns]
            df = df.select(keep)
            f32 = [c for c in _RECORD_MESGS_FLOAT32_COLS if c in df.columns]
            if f32:
                df = df.with_columns([pl.col(c).cast(pl.Float32) for c in f32])
            df = df.with_columns(pl.col("source_file").cast(pl.Categorical))

        # ── Categorical source_file for power_curves ──────────────────────
        if suffix == "power_curves.parquet" and "source_file" in df.columns:
            df = df.with_columns(pl.col("source_file").cast(pl.Categorical))

        # ── Store in cache (S3 mode only) ─────────────────────────────────
        if self.is_s3():
            with self._cache_lock:
                self._parquet_cache[path] = df

        if columns is not None:
            available = [c for c in columns if c in df.columns]
            return df.select(available)
        return df

    def write_parquet(self, df: object, path: str) -> None:
        """Write a parquet file and invalidate the cache for that path."""
        df.write_parquet(path)
        with self._cache_lock:
            self._parquet_cache.pop(path, None)

    def get_compute_cache(self, key: str) -> object | None:
        """Retrieve a cached computation result by key, or None if not cached."""
        with self._cache_lock:
            return self._compute_cache.get(key)

    def set_compute_cache(self, key: str, value: object) -> None:
        """Store a computation result in the cache."""
        with self._cache_lock:
            self._compute_cache[key] = value

    def is_cached(self, path: str) -> bool:
        """Return True if the full DataFrame for *path* is already in the parquet cache."""
        with self._cache_lock:
            return path in self._parquet_cache

    def invalidate_cache(self, user_id: str) -> None:
        """Invalidate all cached parquet files and computations for a user.

        Called after a successful upload so the next callback reads fresh data.
        """
        prefix = f"s3://{self.bucket}/{user_id}/"
        with self._cache_lock:
            for cache in (self._parquet_cache, self._compute_cache):
                keys = [k for k in cache if k.startswith(prefix)]
                for k in keys:
                    del cache[k]

    def read_json(self, path: str) -> dict | list:
        """Read a JSON file from either a local path or S3."""
        import json

        if path.startswith("s3://"):
            with self._s3fs.open(path, "r") as f:
                return json.load(f)
        with open(path, "r") as f:
            return json.load(f)

    def write_json(self, path: str, data: dict | list) -> None:
        """Write a JSON file to either a local path or S3."""
        import json

        if path.startswith("s3://"):
            with self._s3fs.open(path, "w") as f:
                json.dump(data, f, indent=2)
        else:
            with open(path, "w") as f:
                json.dump(data, f, indent=2)

    @cached_property
    def _s3fs(self):
        """Lazily initialised s3fs filesystem — only created in S3 mode."""
        import s3fs  # late import: not needed in local mode

        return s3fs.S3FileSystem(anon=False)


# ── Helpers ────────────────────────────────────────────────────────────
def _require_user_id(user_id: str | None) -> None:
    if not user_id:
        raise ValueError("user_id is required when STORAGE_MODE=s3")


# ── Module-level default instance ─────────────────────────────────────
# Import this wherever a storage config is needed:
#   from backend.storage import storage
storage = StorageConfig()
