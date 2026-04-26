"""
anomaly.py — Per-meter, per-hour anomaly detection.

Fixes the false-positive problem we saw in the dry run output where normal
morning ramp-up was flagged as anomalous because the rolling window was full
of overnight readings.

The fix: maintain separate baselines per hour of the day (0-23) using
an Exponential Moving Average (EMA). The 6am baseline only compares against
previous 6am readings — not against 3am trough readings.

One AnomalyDetector instance is created per meter in the consumer.
It persists in memory for the life of the consumer process.
"""

from collections import defaultdict


class AnomalyDetector:
    """
    Per-meter anomaly detector using per-hour EMA baselines.

    State held per instance:
        _hourly_baseline: dict[int, float]
            One EMA value per hour of the day (0-23).
            Initialised on first reading for that hour.
            Updated after every non-anomalous reading.
            Anomalous readings do NOT update the baseline — we don't
            want one spike to corrupt the expected value for that hour.
    """

    def __init__(self, spike_threshold: float, ema_alpha: float):
        """
        Parameters
        ----------
        spike_threshold : float
            A reading is anomalous if delta > baseline × spike_threshold.
            4.0 is recommended — gives headroom for natural variation.

        ema_alpha : float
            EMA smoothing factor. 0.2 means new readings contribute
            20% to the baseline update, history contributes 80%.
        """
        self._threshold = spike_threshold
        self._alpha     = ema_alpha

        # Key: hour of day (0-23). Value: current EMA baseline for that hour.
        # Using defaultdict so the first reading for any hour just initialises.
        self._hourly_baseline: dict[int, float] = {}

    def check(self, kwh_delta: float, hour: int) -> bool:
        """
        Check whether kwh_delta is anomalous for this hour.

        Returns True if anomalous, False if normal.
        Updates the per-hour baseline only on non-anomalous readings.

        Parameters
        ----------
        kwh_delta : float
            The consumption delta from this reading.
        hour : int
            The hour of the simulated day this reading belongs to (0-23).
        """
        if hour not in self._hourly_baseline:
            # First reading for this hour — initialise baseline, no verdict yet.
            # We need at least one data point before we can judge anomalies.
            self._hourly_baseline[hour] = kwh_delta
            return False

        baseline   = self._hourly_baseline[hour]
        is_anomaly = baseline > 0 and kwh_delta > baseline * self._threshold

        if not is_anomaly:
            # Normal reading — update the EMA baseline for this hour.
            # EMA formula: new_ema = alpha * new_value + (1 - alpha) * old_ema
            # This gives more weight to recent readings while never fully
            # forgetting the history — smoother than a simple rolling average.
            self._hourly_baseline[hour] = (
                self._alpha * kwh_delta +
                (1 - self._alpha) * baseline
            )
        # If anomalous, DON'T update the baseline — the spike shouldn't
        # shift what we consider "normal" for this hour.

        return is_anomaly

    def baseline_for_hour(self, hour: int) -> float:
        """Return the current baseline for a given hour. 0.0 if not yet seen."""
        return self._hourly_baseline.get(hour, 0.0)
