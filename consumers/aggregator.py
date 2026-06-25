"""
aggregator.py — Windowing and aggregation logic for the readings consumer.

Two things happen here simultaneously for every event consumed:

1. Per-meter anomaly detection (via AnomalyDetector in anomaly.py)
   Each meter has its own detector that maintains per-hour EMA baselines.
   This runs on every single event — it's stateful per meter.

2. Per-zone 5-minute tumbling window
   Events accumulate in the current window bucket. When the wall clock
   crosses a 5-minute boundary, the bucket is sealed, a ZoneAggregate
   is emitted, and a fresh bucket starts.

   A tumbling window is non-overlapping — each event belongs to exactly
   one window. This is the right choice for a dashboard that shows
   "what happened in the last 5 minutes" rather than a sliding view.

Why does the aggregator live separately from the consumer?
   The consumer handles Kafka mechanics (polling, offset commits, 
   partition assignment). The aggregator handles business logic. Keeping
   them separate means you can unit test aggregation logic without 
   spinning up a Kafka broker.
"""

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

from anomaly import AnomalyDetector
from config import AggregationConfig, PARTITION_ZONE

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Output dataclass — one per zone per window close
# ---------------------------------------------------------------------------

@dataclass
class ZoneAggregate:
    """
    The output of one closed 5-minute tumbling window for one zone.

    This is what gets pushed to Power BI and printed to terminal.
    Every field maps directly to a metric on the live dashboard.
    """

    # Identity
    zone_id:          str
    window_start_ts:  str       # ISO8601 UTC — when this window opened
    window_end_ts:    str       # ISO8601 UTC — when this window closed

    # Demand metrics — the core operational numbers
    total_kwh:        float     # total energy consumed in this window across all meters
    avg_power_kw:     float     # average instantaneous power draw
    peak_power_kw:    float     # highest single reading in this window
    avg_power_factor: float     # average PF across all readings — grid health indicator
    avg_voltage:      float     # average voltage — spot faults via deviation from nominal
    avg_frequency_hz: float     # average grid frequency — should be 60Hz ±0.5

    # Fleet health counts
    active_meter_count:   int   # meters that sent at least one reading this window
    silent_meter_count:   int   # meters expected but sent nothing this window
    degraded_count:       int   # readings tagged meter_state=degraded
    fault_count:          int   # readings tagged meter_state=fault
    anomaly_count:        int   # readings flagged by per-meter anomaly detector

    # Derived
    demand_vs_prev_window_pct: Optional[float] = None  # % change vs previous window
    # None on first window — no previous to compare against


# ---------------------------------------------------------------------------
# Per-zone window bucket — accumulates readings within one 5-minute window
# ---------------------------------------------------------------------------

@dataclass
class WindowBucket:
    """
    Mutable accumulator for one zone's current open window.

    Created fresh when a window opens. Sealed when the window closes
    and converted into a ZoneAggregate.
    """
    zone_id:      str
    window_start: str     # simulated timestamp (5-min boundary) when this window opened

    # Accumulators — updated on every incoming event
    total_kwh:        float = 0.0
    power_kw_sum:     float = 0.0
    power_factor_sum: float = 0.0
    voltage_sum:      float = 0.0
    frequency_sum:    float = 0.0
    peak_power_kw:    float = 0.0
    reading_count:    int   = 0

    # State counters
    active_meters:  set = field(default_factory=set)   # meter_ids seen this window
    degraded_count: int = 0
    fault_count:    int = 0
    anomaly_count:  int = 0

    def ingest(self, event: dict, is_anomaly: bool) -> None:
        """
        Add one parsed event to this bucket.
        Called for every event that belongs to this window.
        """
        self.total_kwh        += event.get("kwh_delta", 0.0)
        self.power_kw_sum     += event.get("power_kw", 0.0)
        self.power_factor_sum += event.get("power_factor", 0.0)
        self.voltage_sum      += event.get("voltage", 0.0)
        self.frequency_sum    += event.get("frequency_hz", 60.0)
        self.peak_power_kw     = max(self.peak_power_kw, event.get("power_kw", 0.0))
        self.reading_count    += 1
        self.active_meters.add(event["meter_id"])

        state = event.get("meter_state", "normal")
        if state == "degraded": self.degraded_count += 1
        if state == "fault":    self.fault_count    += 1
        if is_anomaly:          self.anomaly_count  += 1

    def seal(
        self,
        window_end_ts: str,
        expected_meter_count: int,
        prev_total_kwh: Optional[float],
    ) -> ZoneAggregate:
        """
        Close this window and return the aggregate.

        Parameters
        ----------
        window_end_ts        : simulated timestamp when window closed
        expected_meter_count : total meters assigned to this zone
                               used to compute silent_meter_count
        prev_total_kwh       : total_kwh from the previous window
                               used to compute demand_vs_prev_window_pct
        """
        n = max(self.reading_count, 1)   # guard against div-by-zero

        silent_count = expected_meter_count - len(self.active_meters)

        delta_pct = None
        if prev_total_kwh is not None and prev_total_kwh > 0:
            delta_pct = ((self.total_kwh - prev_total_kwh) / prev_total_kwh) * 100.0

        return ZoneAggregate(
            zone_id=self.zone_id,
            window_start_ts=self.window_start,
            window_end_ts=window_end_ts,
            total_kwh=round(self.total_kwh, 4),
            avg_power_kw=round(self.power_kw_sum / n, 3),
            peak_power_kw=round(self.peak_power_kw, 3),
            avg_power_factor=round(self.power_factor_sum / n, 3),
            avg_voltage=round(self.voltage_sum / n, 2),
            avg_frequency_hz=round(self.frequency_sum / n, 3),
            active_meter_count=len(self.active_meters),
            silent_meter_count=max(0, silent_count),
            degraded_count=self.degraded_count,
            fault_count=self.fault_count,
            anomaly_count=self.anomaly_count,
            demand_vs_prev_window_pct=round(delta_pct, 2) if delta_pct is not None else None,
        )


# ---------------------------------------------------------------------------
# ZoneAggregator — one instance per consumer (per zone)
# ---------------------------------------------------------------------------

class ZoneAggregator:
    """
    Manages the tumbling window and per-meter anomaly detectors for one zone.

    One ZoneAggregator is created per consumer instance. Since each consumer
    owns exactly one partition (one zone), this aggregator only ever sees
    events from its assigned zone — no cross-zone coordination needed.

    Lifecycle:
        aggregator = ZoneAggregator(zone_id, cfg, meter_count)

        # For each consumed event:
        aggregate = aggregator.ingest(event_dict)
        if aggregate is not None:
            # Window just closed — push this aggregate to Power BI / terminal
            push(aggregate)
    """

    def __init__(self, zone_id: str, cfg: AggregationConfig, expected_meter_count: int):
        self.zone_id              = zone_id
        self.cfg                  = cfg
        self.expected_meter_count = expected_meter_count
        self._last_event_timestamp: str = ""

        # Windows are event-time based: a window closes when the incoming
        # event's simulated timestamp crosses the window boundary, not when
        # wall-clock seconds elapse. This means all zones close the same
        # 5-minute slot simultaneously regardless of their message rate —
        # residential zones (slower) and industrial zones (faster) both emit
        # window_start=05:10/window_end=05:15 for the same simulated period.
        logger.info(
            "[Aggregator][%s] speed_multiplier=%.1f | "
            "window=%ds simulated (event-time)",
            zone_id, cfg.speed_multiplier, cfg.window_size_seconds,
        )

        # Per-meter anomaly detectors.
        # Key: meter_id. Created lazily on first event from that meter.
        self._detectors: dict[str, AnomalyDetector] = {}

        # Current open window bucket — None until first event arrives
        self._current_bucket: Optional[WindowBucket] = None

        # Previous window's total_kwh — used for delta% calculation
        self._prev_total_kwh: Optional[float] = None

        # Count of windows closed so far — useful for logging
        self._windows_closed: int = 0

    def ingest(self, event: dict) -> Optional[ZoneAggregate]:
        """
        Process one event. Returns a ZoneAggregate if this event caused
        a window to close, otherwise returns None.

        Parameters
        ----------
        event : dict
            Parsed JSON from the Kafka message value. Expected fields:
            meter_id, zone_id, timestamp, kwh_delta, power_kw,
            power_factor, voltage, frequency_hz, meter_state, is_anomaly
        """
        timestamp = event.get("timestamp", "")
        if timestamp:
            self._last_event_timestamp = timestamp

        hour = self._extract_hour(timestamp)

        # --- Per-meter anomaly detection ---
        meter_id = event["meter_id"]
        if meter_id not in self._detectors:
            self._detectors[meter_id] = AnomalyDetector(
                spike_threshold=self.cfg.anomaly_spike_threshold,
                ema_alpha=self.cfg.ema_alpha,
            )
        is_anomaly = self._detectors[meter_id].check(
            kwh_delta=event.get("kwh_delta", 0.0),
            hour=hour,
        )
        event["is_anomaly"] = is_anomaly

        # --- Event-time tumbling window management ---
        aggregate = None

        if self._current_bucket is None:
            self._open_new_window(timestamp)

        elif self._window_expired(timestamp):
            # This event's simulated timestamp crossed the window boundary —
            # seal the current window, then open a fresh one for this event.
            aggregate = self._close_window()
            self._open_new_window(timestamp)

        self._current_bucket.ingest(event, is_anomaly)

        return aggregate

    def flush(self) -> Optional[ZoneAggregate]:
        """
        Force-close the current window and return whatever has accumulated.
        Called on consumer shutdown so the last partial window isn't lost.
        """
        if self._current_bucket is None or self._current_bucket.reading_count == 0:
            return None
        return self._close_window()

    # -----------------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------------
    
    @staticmethod
    def _floor_to_window(timestamp: str, window_seconds: int) -> datetime:
        """Floor a simulated timestamp to the nearest window boundary."""
        dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
        total_seconds = dt.hour * 3600 + dt.minute * 60 + dt.second
        floored_seconds = (total_seconds // window_seconds) * window_seconds
        return dt.replace(
            hour=floored_seconds // 3600,
            minute=(floored_seconds % 3600) // 60,
            second=floored_seconds % 60,
        )

    def _window_expired(self, timestamp: str) -> bool:
        """
        True when the event's simulated timestamp has crossed the current
        window's end boundary. This is event-time windowing — window
        boundaries are determined by the data, not by wall-clock seconds.
        All zones close the same simulated slot simultaneously regardless
        of their message rate.
        """
        window_start_dt = datetime.strptime(self._current_bucket.window_start, "%Y-%m-%dT%H:%M:%SZ")
        window_end_dt   = window_start_dt + timedelta(seconds=self.cfg.window_size_seconds)
        event_dt        = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
        return event_dt >= window_end_dt

    def _open_new_window(self, timestamp: str) -> None:
        window_start_dt = self._floor_to_window(timestamp, self.cfg.window_size_seconds)
        self._current_bucket = WindowBucket(
            zone_id=self.zone_id,
            window_start=window_start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        )

    def _close_window(self) -> ZoneAggregate:
        window_start_dt = datetime.strptime(self._current_bucket.window_start, "%Y-%m-%dT%H:%M:%SZ")
        window_end_ts   = (window_start_dt + timedelta(seconds=self.cfg.window_size_seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")
        aggregate = self._current_bucket.seal(
            window_end_ts=window_end_ts,
            expected_meter_count=self.expected_meter_count,
            prev_total_kwh=self._prev_total_kwh,
        )
        self._prev_total_kwh = aggregate.total_kwh
        self._windows_closed += 1
        logger.info(
            "[Aggregator][%s] Window #%d closed | %s → %s | "
            "kwh=%.3f | active=%d | silent=%d | faults=%d | anomalies=%d",
            self.zone_id, self._windows_closed,
            self._current_bucket.window_start, window_end_ts,
            aggregate.total_kwh, aggregate.active_meter_count,
            aggregate.silent_meter_count, aggregate.fault_count,
            aggregate.anomaly_count,
        )
        return aggregate

    @staticmethod
    def _extract_hour(timestamp: str) -> int:
        """
        Parse hour from ISO8601 timestamp string.
        Returns 0 on parse failure — safe fallback.
        """
        try:
            # Timestamp format: "2025-04-22T09:14:30Z"
            return int(timestamp[11:13])
        except (IndexError, ValueError):
            return 0
