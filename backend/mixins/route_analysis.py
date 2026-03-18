import os

import gc
import polars as pl

from ..schemas import load_records
from ..storage import storage


class RouteAnalysisMixin:
    """Elevation profile, climb detection, and GPS route data."""

    # FIT semicircles to degrees conversion factor
    SEMICIRCLES_TO_DEGREES = 180.0 / (2**31)

    def get_elevation_profile(self, source_file: str) -> dict:
        """Return elevation profile data with gradient coloring.

        Returns {distance_mi: [], altitude_ft: [], grade_pct: [], grade_instant: []}.
        Grade is smoothed over a rolling window to reduce GPS noise.
        """
        records_path = storage.path_join(self.mergedfiles_path, "record_mesgs.parquet")
        if not storage.path_exists(records_path):
            return {"distance_mi": [], "altitude_ft": [], "grade_pct": []}

        records = load_records(
            "cycling",
            records_path,
            source_files=[source_file],
            columns=["source_file", "timestamp", "distance", "enhanced_altitude"],
        ).sort("timestamp")

        if records.is_empty():
            del records
            gc.collect()
            return {"distance_mi": [], "altitude_ft": [], "grade_pct": []}

        has_alt = "enhanced_altitude" in records.columns
        has_dist = "distance" in records.columns
        if not has_alt or not has_dist:
            del records
            gc.collect()
            return {"distance_mi": [], "altitude_ft": [], "grade_pct": []}

        df = (
            records.select("distance", "enhanced_altitude")
            .with_columns(
                pl.col("distance").fill_null(strategy="forward"),
                pl.col("enhanced_altitude").fill_null(strategy="forward"),
            )
            .drop_nulls()
        )  # drop any leading nulls that couldn't be forward-filled

        if df.height < 2:
            del records, df
            gc.collect()
            return {"distance_mi": [], "altitude_ft": [], "grade_pct": []}

        dist_m = df["distance"].to_list()
        alt_m = df["enhanced_altitude"].to_list()

        # Compute raw grade between consecutive points
        grade = [0.0]
        for i in range(1, len(dist_m)):
            dd = dist_m[i] - dist_m[i - 1]
            da = alt_m[i] - alt_m[i - 1]
            grade.append((da / dd * 100) if dd > 0 else 0.0)

        # Smooth grade with two windows:
        # - 20-point (~20s) for the profile chart
        # - 5-point (~5s) for a "near-instant" grade closer to what the Garmin shows
        smoothed = self._rolling_avg(grade, 20)
        instant = self._rolling_avg(grade, 5)

        distance_mi = [d / 1609.344 for d in dist_m]
        altitude_ft = [a * 3.28084 for a in alt_m]

        result = {
            "distance_mi": distance_mi,
            "altitude_ft": altitude_ft,
            "grade_pct": smoothed,
            "grade_instant": instant,
        }
        del dist_m, alt_m, grade, smoothed, instant, records, df
        gc.collect()
        return result

    def detect_climbs(
        self,
        source_file: str,
        min_grade: float = 3.0,
        min_distance_m: float = 400,
        min_gain_m: float = 30,
    ) -> list[dict]:
        """Detect sustained climbs in a ride.

        A climb is a segment where the smoothed grade stays above min_grade
        for at least min_distance_m meters.

        Returns list of dicts with: start_mi, end_mi, distance_mi, elevation_gain_ft,
        avg_grade, max_grade, duration_s, vam (vertical meters/hour).
        """
        records_path = storage.path_join(self.mergedfiles_path, "record_mesgs.parquet")
        if not storage.path_exists(records_path):
            return []

        records = load_records(
            "cycling",
            records_path,
            source_files=[source_file],
            columns=[
                "source_file",
                "timestamp",
                "distance",
                "enhanced_altitude",
                "power",
                "cadence",
            ],
        ).sort("timestamp")

        if records.is_empty():
            del records
            gc.collect()
            return []

        has_alt = "enhanced_altitude" in records.columns
        has_dist = "distance" in records.columns
        has_ts = "timestamp" in records.columns
        if not has_alt or not has_dist:
            del records
            gc.collect()
            return []

        has_power = "power" in records.columns
        has_cadence = "cadence" in records.columns
        cols = (
            ["distance", "enhanced_altitude"]
            + (["timestamp"] if has_ts else [])
            + (["power"] if has_power else [])
            + (["cadence"] if has_cadence else [])
        )
        df = (
            records.select(cols)
            .with_columns(
                pl.col("distance").fill_null(strategy="forward"),
                pl.col("enhanced_altitude").fill_null(strategy="forward"),
            )
            .drop_nulls(subset=["distance", "enhanced_altitude"])
        )

        if df.height < 2:
            del records, df
            gc.collect()
            return []

        dist_m = df["distance"].to_list()
        alt_m = df["enhanced_altitude"].to_list()
        timestamps = df["timestamp"].to_list() if has_ts else None
        power_list = (
            df["power"].fill_null(0).cast(pl.Int64).to_list() if has_power else None
        )
        cadence_list = (
            df["cadence"].fill_null(0).cast(pl.Int64).to_list() if has_cadence else None
        )

        # Compute smoothed grade (same as elevation profile)
        grade = [0.0]
        for i in range(1, len(dist_m)):
            dd = dist_m[i] - dist_m[i - 1]
            da = alt_m[i] - alt_m[i - 1]
            grade.append((da / dd * 100) if dd > 0 else 0.0)

        smoothed = self._rolling_avg(grade, 20)
        instant = self._rolling_avg(grade, 5)

        # Find contiguous segments above min_grade
        segments = []
        in_climb = False
        start_idx = 0
        for i, g in enumerate(smoothed):
            if g >= min_grade and not in_climb:
                in_climb = True
                start_idx = i
            elif g < min_grade and in_climb:
                in_climb = False
                segments.append((start_idx, i - 1))
        if in_climb:
            segments.append((start_idx, len(smoothed) - 1))

        # Merge segments that are part of the same overall climb.
        # Two segments merge if the gap between them is < 1.6km (~1mi)
        # AND the elevation doesn't drop more than 30% of what was gained
        # in the preceding segment (i.e. the dip is minor).
        merged = []
        for seg in segments:
            if merged:
                prev_start, prev_end = merged[-1]
                gap_dist = dist_m[seg[0]] - dist_m[prev_end]
                prev_gain = alt_m[prev_end] - alt_m[prev_start]
                gap_drop = alt_m[prev_end] - alt_m[seg[0]]  # positive = lost elevation
                # Merge if gap is short and descent in the gap is minor
                if gap_dist < 1609 and (prev_gain <= 0 or gap_drop < prev_gain * 0.3):
                    merged[-1] = (prev_start, seg[1])
                    continue
            merged.append(seg)

        # Filter by minimum distance and build results
        climbs = []
        for start, end in merged:
            seg_dist = dist_m[end] - dist_m[start]
            if seg_dist < min_distance_m:
                continue

            elev_gain = alt_m[end] - alt_m[start]
            if elev_gain < min_gain_m:
                continue

            # Compute avg grade as net gain / distance (not average of point grades)
            # This better represents the overall climb difficulty
            avg_grade = (elev_gain / seg_dist * 100) if seg_dist > 0 else 0
            # Use the 5s instant grade for max — closer to what the Garmin displays
            max_grade_val = max(instant[start : end + 1])

            duration_s = None
            vam = None
            if timestamps:
                dt = (timestamps[end] - timestamps[start]).total_seconds()
                if dt > 0:
                    duration_s = int(dt)
                    vam = round(elev_gain / (dt / 3600))

            # Power stats for the climb segment
            avg_power = None
            normalized_power = None
            if power_list:
                seg_power = power_list[start : end + 1]
                non_zero = [p for p in seg_power if p > 0]
                if non_zero:
                    avg_power = round(sum(non_zero) / len(non_zero))
                    # NP = 4th root of mean of rolling 30s avg raised to 4th power
                    if len(seg_power) >= 30:
                        rolling = []
                        window_sum = sum(seg_power[:30])
                        rolling.append(window_sum / 30)
                        for j in range(30, len(seg_power)):
                            window_sum += seg_power[j] - seg_power[j - 30]
                            rolling.append(window_sum / 30)
                        normalized_power = round(
                            (sum(r**4 for r in rolling) / len(rolling)) ** 0.25
                        )

            # Cadence stats for the climb segment
            avg_cadence = None
            if cadence_list:
                seg_cadence = cadence_list[start : end + 1]
                non_zero_cad = [c for c in seg_cadence if c > 0]
                if non_zero_cad:
                    avg_cadence = round(sum(non_zero_cad) / len(non_zero_cad))

            climbs.append(
                {
                    "start_mi": round(dist_m[start] / 1609.344, 2),
                    "end_mi": round(dist_m[end] / 1609.344, 2),
                    "distance_mi": round(seg_dist / 1609.344, 2),
                    "elevation_gain_ft": round(elev_gain * 3.28084),
                    "avg_grade": round(avg_grade, 1),
                    "max_grade": round(max_grade_val, 1),
                    "duration_s": duration_s,
                    "vam": vam,
                    "avg_power": avg_power,
                    "normalized_power": normalized_power,
                    "avg_cadence": avg_cadence,
                }
            )

        # Sort by position along the route
        climbs.sort(key=lambda c: c["start_mi"])
        del (
            records,
            df,
            grade,
            smoothed,
            instant,
            dist_m,
            alt_m,
            timestamps,
            power_list,
            cadence_list,
        )
        gc.collect()
        return climbs

    def get_ride_route(self, source_file: str) -> dict:
        """Return GPS route data for a ride: {lat: [], lon: [], power: [], elevation: []}."""
        records_path = storage.path_join(self.mergedfiles_path, "record_mesgs.parquet")
        if not storage.path_exists(records_path):
            return {"lat": [], "lon": [], "power": [], "elevation": []}

        records = load_records(
            "cycling",
            records_path,
            source_files=[source_file],
            columns=[
                "source_file",
                "timestamp",
                "position_lat",
                "position_long",
                "enhanced_altitude",
                "power",
            ],
        ).sort("timestamp")

        if records.is_empty() or "position_lat" not in records.columns:
            del records
            gc.collect()
            return {"lat": [], "lon": [], "power": [], "elevation": []}

        gps = records.filter(pl.col("position_lat").is_not_null())
        if gps.is_empty():
            del records, gps
            gc.collect()
            return {"lat": [], "lon": [], "power": [], "elevation": []}

        lat = (gps["position_lat"] * self.SEMICIRCLES_TO_DEGREES).to_list()
        lon = (gps["position_long"] * self.SEMICIRCLES_TO_DEGREES).to_list()
        power = gps["power"].fill_null(0).to_list() if "power" in gps.columns else []
        elevation = (
            gps["enhanced_altitude"].to_list()
            if "enhanced_altitude" in gps.columns
            else []
        )

        result = {"lat": lat, "lon": lon, "power": power, "elevation": elevation}
        del records, gps
        gc.collect()
        return result
