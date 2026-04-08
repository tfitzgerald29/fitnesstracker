# Tyler's Activities

A Dash-based personal activity dashboard that ingests Garmin FIT files and Garmin sleep exports to track cycling, climbing, running, hiking, pickleball, weight lifting, and recovery. It includes cycling power analysis, critical power modeling with bootstrap inference, training load tracking, sleep trends/stage breakdowns, and a custom weight-lifting form for logging exercises, sets, reps, and weight over time.

Dashboard link (not great on phone): https://fitnesstracker-xu57.onrender.com

## Quick Start

```bash
# Install dependencies
uv sync

# Run the dashboard
python app.py
# http://localhost:8051
```

Full local startup (ingestion + warm caches + app):
```bash
python run_local.py
```

Production (gunicorn):
```bash
python warmup_caches.py
gunicorn app:server
```

## Architecture

```
backend/
  FitFileProcessor.py       # Main orchestrator: FIT ingestion, dedup, Parquet pipeline
  cycling_processor.py      # Cycling-specific analysis (inherits cycling mixins)
  sleep_processor.py        # Sleep JSON/CSV ingestion + sleep query layer
  running_processor.py      # Running summaries and session-level views
  hiking_processor.py       # Hiking summaries and route metrics
  storage.py                # Local/S3 path + parquet/json storage abstraction
  SportSummarizer.py        # Multi-sport summaries (cycling, climbing, running, etc.)
  weighttraining_entry.py   # Weight-lifting JSON logger and form backend
  skiing_processor.py       # Skiing file processing
  mixins/
    cp_model.py             # Critical power estimation and covariate bootstrap
    power_analysis.py       # Peak powers, zones, histograms
    route_analysis.py       # Elevation profiles, climb detection, VAM
    training_load.py        # CTL/ATL/TSB with 60-day forecast

dashboard/
  layout.py                 # Tab router and header
  auth_layout.py            # Sign-in UI layout (auth flow in progress)
  config.py                 # Colors, paths, styling
  callbacks.py              # Tab switching
  tabs/
    calendar.py             # Activity heatmap (all sports)
    sleep.py                # Sleep overview, trends, stage breakdowns
    sports.py               # Multi-sport summary tables
    cycling.py              # Cycling sub-tab router
    cycling_overview.py     # Year/month/week stats and TSS trends
    cycling_training_load.py # CTL/ATL/TSB + forecast planner inputs
    cycling_rides.py        # Single-ride detail (power, elevation, climbs)
    cycling_cp.py           # CP model, CP over time, covariate analysis
    cycling_covariate.py    # Peak power regressions (with optional sleep covariate)
    climbing.py             # Indoor climbing split analysis
    running.py              # Running summaries and session table
    hiking.py               # Hiking trends and route map
    pickleball.py           # Pickleball session summaries
    weights.py              # Weight-lifting form, exercise tracker, progress charts
    skiing.py               # Skiing views and summaries
```

## Data Pipeline

```
~/Downloads/*.zip
  -> FitFileProcessor.unzip_fit_files()
processedfiles/*.fit
  -> FitFileProcessor.process_new_fit_files()
     - Decode with garmin-fit-sdk
     - Filter message types (file_id, activity, session, record, split, set)
     - Align schemas, deduplicate, cast types
mergedfiles/
  -> session_mesgs.parquet    # Ride/activity summaries (~500 activities)
  -> record_mesgs.parquet     # Second-by-second power/GPS/HR (~100MB)
  -> power_curves.parquet     # Per-ride best power at 120-1200s durations
  -> cp_covariate_bootstrap.json  # Cached bootstrap CIs
  -> sleep.parquet            # Canonical nightly sleep dataset
  -> (+ file_id, activity, split, split_summary, set parquets)

~/Downloads/*_sleepData.json
  -> SleepProcessor.ingest_from_downloads()
sleepdata/
  -> SleepProcessor._parse_all_json()
~/Downloads/*sleep*.csv
  -> SleepProcessor._parse_sleep_csv()
mergedfiles/
  -> SleepProcessor._merge_to_parquet()  # dedupe by calendar_date
```

All processing is **incremental**: only new activities are decoded and appended. Run `python warmup_caches.py` before serving to precompute power-curve and CP covariate bootstrap caches.

## Features

### Multi-Sport Activity Tracking
Automatically categorizes activities from Garmin FIT files: road cycling, mountain biking, gravel, indoor cycling, indoor climbing, strength training, running, pickleball, and more. Calendar heatmaps and sport summary tables show volume across all activity types.

### Weight Lifting Tracker
Custom form-based workout logger (stored as JSON, independent of Garmin). Log exercises, sets, reps, and weight directly in the dashboard, then track progress with per-exercise volume and strength trend charts.

### Critical Power Model
Fits the 2-parameter hyperbolic model `P = CP + W'/t` using linear regression on 2-20 minute best powers. Tracks CP and W' monthly with configurable rolling windows (3/6/9/12 months).

### Covariate Analysis with Bootstrap
Regresses monthly best peak powers (2 min, 5 min, 20 min) against:
- **CTL (3-month lag):** Does sustained training load predict power?
- **Sleep score (optional):** Prior-night Garmin sleep score, aggregated monthly
- **Season dummies** (spring/summer/fall vs winter baseline)

Uses **5,000 bootstrap resamples** (case resampling with NumPy `lstsq`) for confidence intervals. Shows OLS, bootstrap median, and bootstrap mean coefficients side by side. CIs that do not cross zero are significant at the 95% level.

### Training Load (CTL/ATL/TSB)
- **CTL:** 42-day exponential moving average of daily TSS (chronic training load / fitness)
- **ATL:** 7-day EMA (acute training load / fatigue)
- **TSB:** `CTL - ATL` (training stress balance / form)
- 60-day forecast using 42-day historical average TSS
- 21-day planner inputs to override projected daily TSS and preview CTL/ATL/TSB impact

### Sleep Tracking
- Garmin sleep ingestion from JSON exports and key/value CSV files
- Nightly trend charts for sleep duration, score, efficiency, SpO2, HR, and stress
- Stage breakdown (deep/REM/light/awake) and latest-night score cards
- Sleep dataset (`sleep.parquet`) reused in cycling covariate analysis

### Power Analysis
- Peak powers at 10 durations (5s through 2hr)
- Power zone distribution (Z1-Z7, % of FTP)
- Per-ride power histogram with zone coloring

### Route Analysis
- Elevation profile with both 20s and 5s smoothed grade for comparison. I got burned by aggressive smoothing in Strava during a gravel race and ended up way under-geared, so this view is built to make steep changes more obvious.
- Climb detection: sustained >3% grade, >400m distance, >30m elevation gain
- VAM (vertical ascent meters/hour) per climb

### Climbing
Split-level analysis from Garmin's indoor climbing activity type, tracking individual climb segments.

## Planned Improvements

- Multi-user authentication and account flow (currently evaluating options).

## Usage (Programmatic)

```python
from backend.cycling_processor import CyclingProcessor
from backend.sleep_processor import SleepProcessor

cp = CyclingProcessor()
sp = SleepProcessor()

# Power curve & CP
cp.get_best_power_curve(period_months=6)
cp.estimate_critical_power(period_months=6)

# Training load
cp.compute_ctl_atl()

# Covariate analysis (reads cached bootstrap)
cp.cp_covariate_analysis()
cp.cp_covariate_analysis(include_sleep=True)

# Sleep
sp.summary_stats()
sp.chart_data(metric="total_sleep_hrs")

# Refresh bootstrap cache after new data
cp.refresh_cp_covariate_bootstrap(n_bootstrap=5000)

# Rebuild all parquets from scratch
cp.rebuild()
```

## Dependencies

- **dash** - Web framework + Plotly charts
- **dash-mantine-components** - Mantine component library for Dash UI
- **polars** - DataFrame engine (Parquet I/O, aggregations)
- **garmin-fit-sdk** - FIT file decoding
- **statsmodels** - OLS regression
- **numpy** - Bootstrap resampling, power curve computation
- **pandas** - Required by statsmodels
- **pyarrow** - Parquet read/write backend

## Deployment Modes

- **Local mode (default):** reads/writes local `mergedfiles/`, `processedfiles/`, and `sleepdata/`.
- **S3 storage mode:** set `STORAGE_MODE=s3`, `S3_BUCKET=<bucket>`, and user-scoped paths resolve through `backend/storage.py`.
- **Auth and multi-user access:** in progress; currently running single-user local workflow while exploring options.

Python 3.13+ managed with [uv](https://docs.astral.sh/uv/).
