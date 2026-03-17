# Tyler's Activities

A Dash-based personal activity dashboard that ingests Garmin FIT files to track cycling, climbing, running, weight lifting, and other sports. Includes cycling power analysis, critical power modeling with bootstrap inference, training load tracking, and a custom weight lifting form for logging exercises, sets, reps, and weight to track gym progress over time.

link to plotly hosted dashboard (not current): https://526068ff-66c1-4f99-8a31-aa9c4b1da937.plotly.app

## Quick Start

```bash
# Install dependencies
uv sync

# Run the dashboard
python app.py
# http://localhost:8050
```

Production (gunicorn):
```bash
gunicorn app:server
```

## Architecture

```
backend/
  FitFileProcessor.py       # Main orchestrator: FIT file ingestion, dedup & parquet pipeline
  cycling_processor.py      # Cycling-specific analysis (inherits all mixins these are for cycling)
  SportSummarizer.py        # Multi-sport summaries (cycling, climbing, running, etc.)
  weighttraining_entry.py   # Weight lifting JSON logger & form backend
  skiing_processor.py       #process skiing files
  mixins/
    cp_model.py             # Critical power estimation & covariate bootstrap
    power_analysis.py       # Peak powers, zones, histograms
    route_analysis.py       # Elevation profiles, climb detection, VAM
    training_load.py        # CTL/ATL/TSB with 60-day forecast

dashboard/
  layout.py                 # Tab router & header
  config.py                 # Colors, paths, styling
  callbacks.py              # Tab switching
  tabs/
    calendar.py             # Activity heatmap (all sports)
    sports.py               # Multi-sport summary tables
    cycling.py              # Cycling sub-tab router
    cycling_overview.py     # Year/month/week stats & TSS trends
    cycling_rides.py        # Single-ride detail (power, elevation, climbs)
    cycling_cp.py           # CP model, CP over time, covariate analysis
    climbing.py             # Rock climbing split analysis
    weights.py              # Weight lifting form, exercise tracker, progress charts
    skiing.py               # skiing, dont have much but its there
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
  -> (+ file_id, activity, split, split_summary, set parquets)
```

All processing is **incremental** -- only new activities are decoded and appended. Power curve cache and bootstrap cache auto-refresh when upstream data changes.

## Features

### Multi-Sport Activity Tracking
Automatically categorizes activities from Garmin FIT files: road cycling, mountain biking, gravel, indoor cycling, indoor climbing, strength training, running, pickleball, and more. Calendar heatmap and sport summary tables show volume across all activity types.

### Weight Lifting Tracker
Custom form-based workout logger (stored as JSON, independent of Garmin). Log exercises, sets, reps, and weight directly in the dashboard. Track progress over time with per-exercise charts showing volume and strength trends.

### Critical Power Model
Fits the 2-parameter hyperbolic model `P = CP + W'/t` using linear regression on 2-20min best powers. Tracks CP and W' monthly with configurable rolling windows (3/6/9/12 months).

### Covariate Analysis with Bootstrap
Regresses monthly best peak powers (2min, 5min, 20min) against:
- **CTL (3-month lag)**: Does sustained training load predict power?
- **Season dummies** (spring/summer/fall vs winter baseline)

Uses **5,000 bootstrap resamples** (case resampling with numpy lstsq) for confidence intervals. Shows OLS, bootstrap median, and bootstrap mean coefficients side by side. CIs that don't cross zero are significant at the 95% level.

### Training Load (CTL/ATL/TSB)
- **CTL**: 42-day exponential moving average of daily TSS (chronic training load / fitness)
- **ATL**: 7-day EMA (acute training load / fatigue)
- **TSB**: CTL - ATL (training stress balance / form)
- 60-day forecast using 42-day historical average TSS
- need to add planner next for projected TSS by day, etc

### Power Analysis
- Peak powers at 10 durations (5s through 2hr)
- Power zone distribution (Z1-Z7, % of FTP)
- Per-ride power histogram with zone coloring

### Route Analysis
- Elevation profile with 20s smoothed grade and 5s smoothed grade for comparison. Used Strava for a gravel race and got rekt because the smoothing screwed me up so i made this. was way under geared.
- Climb detection: sustained >3% grade, >400m distance, >30m elevation gain
- VAM (vertical ascent meters/hour) per climb

### Climbing
Split-level analysis from Garmin's indoor climbing activity type, tracking individual climb segments.

## Usage (Programmatic)

```python
from backend.cycling_processor import CyclingProcessor

cp = CyclingProcessor()

# Power curve & CP
cp.get_best_power_curve(period_months=6)
cp.estimate_critical_power(period_months=6)

# Training load
cp.compute_ctl_atl()

# Covariate analysis (reads cached bootstrap)
cp.cp_covariate_analysis()

# Refresh bootstrap cache after new data
cp.refresh_cp_covariate_bootstrap(n_bootstrap=5000)

# Rebuild all parquets from scratch
cp.rebuild()
```

## Dependencies

- **dash** - Web framework + Plotly charts
- **polars** - Dataframe engine (parquet I/O, aggregations)
- **garmin-fit-sdk** - FIT file decoding
- **statsmodels** - OLS regression
- **numpy** - Bootstrap resampling, power curve computation
- **pandas** - Required by statsmodels
- **pyarrow** - Parquet read/write backend

Python 3.13+ managed with [uv](https://docs.astral.sh/uv/).
