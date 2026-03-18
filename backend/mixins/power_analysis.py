import gc
import polars as pl


class PowerAnalysisMixin:
    """Per-ride power analysis: peaks, histogram, zones, W' balance, power curve."""

    # Standard durations for peak power cards
    PEAK_DURATIONS = [
        (5, "5s"),
        (30, "30s"),
        (60, "1min"),
        (300, "5min"),
        (600, "10min"),
        (1200, "20min"),
        (1800, "30min"),
        (3600, "60min"),
        (5400, "90min"),
        (7200, "120min"),
    ]

    # Standard power zones as % of FTP
    POWER_ZONES = [
        ("Z1 Recovery", 0, 0.55),
        ("Z2 Endurance", 0.55, 0.75),
        ("Z3 Tempo", 0.75, 0.90),
        ("Z4 Threshold", 0.90, 1.05),
        ("Z5 VO2max", 1.05, 1.20),
        ("Z6 Anaerobic", 1.20, 1.50),
        ("Z7 Neuromuscular", 1.50, None),
    ]

    ZONE_COLORS = [
        "#888888",  # Z1 grey
        "#2196F3",  # Z2 blue
        "#4CAF50",  # Z3 green
        "#FFEB3B",  # Z4 yellow
        "#FF9800",  # Z5 orange
        "#F44336",  # Z6 red
        "#9C27B0",  # Z7 purple
    ]

    # Sparse durations for power curve chart display: 1s–2hr
    CHART_DURATIONS = [
        1,
        2,
        3,
        5,
        10,
        15,
        20,
        30,
        45,
        60,
        90,
        120,
        180,
        240,
        300,
        360,
        420,
        480,
        540,
        600,
        720,
        900,
        1200,
        1500,
        1800,
        2400,
        3600,
        5400,
        7200,
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
                results.append(
                    {"duration": label, "watts": watts if watts is not None else "N/A"}
                )
        return results

    def get_power_histogram(self, source_file: str, bin_size: int = 25) -> dict:
        """Return power histogram data: {bins: [], counts: [], zone_colors: []}.

        Each bin is colored by its power zone based on FTP.
        """
        power = self._load_ride_power(source_file)
        if not power:
            return {"bins": [], "counts": [], "zone_colors": []}

        # Get FTP from the ride's session data
        ride = self.cycling.filter(pl.col("source_file") == source_file)
        ftp = (
            ride["threshold_power"][0]
            if not ride.is_empty() and ride["threshold_power"][0]
            else 250
        )

        max_power = max(power)
        bins = list(range(0, max_power + bin_size, bin_size))
        counts = [0] * len(bins)
        for p in power:
            idx = min(p // bin_size, len(bins) - 1)
            counts[idx] += 1

        # Assign zone color to each bin based on bin midpoint
        zone_colors = []
        for b in bins:
            midpoint = b + bin_size / 2
            pct = midpoint / ftp
            color = self.ZONE_COLORS[0]
            for i, (_, lo, hi) in enumerate(self.POWER_ZONES):
                if hi is None:
                    if pct >= lo:
                        color = self.ZONE_COLORS[i]
                elif lo <= pct < hi:
                    color = self.ZONE_COLORS[i]
                    break
            zone_colors.append(color)

        del ride
        gc.collect()
        return {"bins": bins, "counts": counts, "zone_colors": zone_colors}

    def get_power_zone_distribution(self, source_file: str) -> dict:
        """Return time-in-zone distribution: {zones: [], seconds: [], percentages: [], colors: [], ftp: int}."""
        power = self._load_ride_power(source_file)
        if not power:
            return {
                "zones": [],
                "seconds": [],
                "percentages": [],
                "colors": [],
                "ftp": None,
            }

        ride = self.cycling.filter(pl.col("source_file") == source_file)
        ftp = (
            ride["threshold_power"][0]
            if not ride.is_empty() and ride["threshold_power"][0]
            else 250
        )

        zone_seconds = [0] * len(self.POWER_ZONES)
        total = len(power)

        for p in power:
            pct = p / ftp
            for i, (_, lo, hi) in enumerate(self.POWER_ZONES):
                if hi is None:
                    if pct >= lo:
                        zone_seconds[i] += 1
                        break
                elif lo <= pct < hi:
                    zone_seconds[i] += 1
                    break

        percentages = [round(s / total * 100, 1) if total else 0 for s in zone_seconds]
        zones = [name for name, _, _ in self.POWER_ZONES]

        del ride
        gc.collect()
        del session
        gc.collect()
        return {
            "zones": zones,
            "seconds": zone_seconds,
            "percentages": percentages,
            "colors": self.ZONE_COLORS,
            "ftp": ftp,
        }

    def get_power_curve(self, source_file: str) -> dict:
        """Compute full power curve for a single ride. Returns {durations: [], watts: []}."""
        power = self._load_ride_power(source_file)
        if not power:
            return {"durations": [], "watts": []}

        durations = []
        watts = []
        for d in self.CHART_DURATIONS:
            w = self._best_avg_power(power, d)
            if w is not None:
                durations.append(d)
                watts.append(w)
        return {"durations": durations, "watts": watts}

    def get_wprime_balance(self, source_file: str) -> dict:
        """Compute W' balance over a ride using the Skiba differential model.

        Uses CP model (last 6 months) for CP and W' values, falling back to
        session FTP and 20 kJ default if the model can't be fitted.

        Returns:
            {time_min: [], wprime_bal_kj: [], wprime_pct: [], power: [], ftp: int, wprime_kj: float}
        """
        power = self._load_ride_power(source_file)
        if not power:
            return {
                "time_min": [],
                "wprime_bal_kj": [],
                "wprime_pct": [],
                "power": [],
                "ftp": 0,
                "wprime_kj": 0,
            }

        # Get the ride date to use as reference for the CP model
        import polars as pl

        session = self.cycling.filter(pl.col("source_file") == source_file)
        ride_date = None
        if not session.is_empty():
            ts = session["timestamp"][0]
            if ts is not None:
                ride_date = ts.date() if hasattr(ts, "date") else None

        # Try CP model using 6 months of data leading up to ride date
        cp_result = self.estimate_critical_power(period_months=6, as_of=ride_date)
        ftp = cp_result["cp"]
        wp_kj = cp_result["wprime_kj"]

        # Fallback to session FTP / defaults if model fails
        if ftp is None or wp_kj is None:
            ftp = 250
            if not session.is_empty() and "threshold_power" in session.columns:
                tp = session["threshold_power"][0]
                if tp is not None:
                    ftp = int(tp)
            wp_kj = 20.0

        wp = wp_kj * 1000  # convert to joules
        bal = [0.0] * len(power)
        bal[0] = wp

        for i in range(1, len(power)):
            p = power[i]
            if p >= ftp:
                # Depleting: W' decreases by (P - FTP) * dt (1 second)
                bal[i] = max(0, bal[i - 1] - (p - ftp))
            else:
                # Recovering: exponential reconstitution
                # tau = 546 * e^(-0.01 * (FTP - P)) + 316
                diff = ftp - p
                tau = 546.0 * (2.718281828 ** (-0.01 * diff)) + 316.0
                bal[i] = wp - (wp - bal[i - 1]) * (2.718281828 ** (-1.0 / tau))

        time_min = [i / 60.0 for i in range(len(power))]
        bal_kj = [b / 1000.0 for b in bal]
        bal_pct = [b / wp * 100.0 for b in bal]

        del session
        gc.collect()
        return {
            "time_min": time_min,
            "wprime_bal_kj": bal_kj,
            "wprime_pct": bal_pct,
            "power": power,
            "ftp": ftp,
            "wprime_kj": wp_kj,
        }
