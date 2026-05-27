"""
config.py — Configuration for the Gridoscope consumer groups.

Kept separate from the producer config so the two can be deployed
independently — in production the consumers often run on different
machines or containers from the producer.
"""

from dataclasses import dataclass, field


# ---------------------------------------------------------------------------
# Zone → Partition mapping
# Must match exactly what the producer uses.
# Defined here (not in producer config) so the consumer package is
# self-contained — it doesn't need to import anything from producer/.
# ---------------------------------------------------------------------------

ZONE_PARTITION: dict[str, int] = {
    "ZONE-NORTH":   0,
    "ZONE-SOUTH":   1,
    "ZONE-EAST":    2,
    "ZONE-WEST":    3,
    "ZONE-CENTRAL": 4,
}

# Reverse map — useful for logging: given a partition number, what zone is it?
PARTITION_ZONE: dict[int, str] = {v: k for k, v in ZONE_PARTITION.items()}


@dataclass
class KafkaConsumerConfig:
    bootstrap_servers: str = "localhost:9092"
    readings_topic:    str = "meter.readings"
    alerts_topic:      str = "meter.alerts"

    # Consumer group IDs — each group tracks its own offsets independently.
    # Changing this value resets the group to consume from scratch.
    readings_group_id: str = "gridoscope-aggregation"
    alerts_group_id:   str = "gridoscope-alerts"

    # Where to start reading if this group has no committed offset yet.
    # "earliest" = from the beginning of the topic (good for dev/replay).
    # "latest"   = only new messages from now on (good for production).
    auto_offset_reset: str = "earliest"

    # How often (ms) to auto-commit the consumer's offset back to Kafka.
    # Committing means "I have successfully processed everything up to here."
    # If the consumer crashes before committing, it re-reads from the last
    # committed offset — which means some messages may be processed twice.
    # 5000ms (5s) is a reasonable balance between durability and overhead.
    auto_commit_interval_ms: int = 5000


@dataclass
class AggregationConfig:
    """
    Controls the two aggregation mechanisms in the readings consumer.
    """

    # --- Speed multiplier ---
    #
    # Must match the speed_multiplier set in the producer's SimulationConfig.
    # All time-based values below are expressed in SIMULATED seconds.
    # The aggregator divides them by speed_multiplier at runtime to get
    # the wall-clock duration it actually waits.
    #
    # Examples:
    #   speed_multiplier=1.0  → window_size_seconds=300 means wait 300 real seconds
    #   speed_multiplier=10.0 → window_size_seconds=300 means wait 30 real seconds
    #   speed_multiplier=60.0 → window_size_seconds=300 means wait 5 real seconds
    #
    # Rule of thumb: set this to whatever speed_multiplier you passed to the producer.
    speed_multiplier: float = 1.0

    # --- Per-meter rolling average (anomaly detection) ---

    # How many past readings to keep per meter per hour.
    # 12 readings × 5-min residential interval = 1 hour of history.
    # This is a COUNT not a duration — it does not need speed scaling.
    rolling_window_size: int = 12

    # A reading is anomalous if its delta exceeds this multiple of the
    # per-hour rolling average. 4× gives headroom to avoid false positives
    # during demand transitions (the bug we identified in dry_run output).
    anomaly_spike_threshold: float = 4.0

    # Exponential moving average smoothing factor for per-hour baselines.
    # 0.2 = new readings contribute 20% to the baseline, history 80%.
    # Lower = slower to adapt (good for stability).
    # Higher = faster to adapt (good for detecting g    radual drift).
    ema_alpha: float = 0.2

    # --- Per-zone tumbling window (dashboard aggregation) ---

    # How many SIMULATED seconds in each tumbling window.
    # 300s simulated = 5 minutes of meter data.
    # At speed_multiplier=1   → window closes after 300 real seconds
    # At speed_multiplier=10  → window closes after  30 real seconds
    # At speed_multiplier=60  → window closes after   5 real seconds
    window_size_seconds: int = 300

    # How many SIMULATED seconds of silence before a meter is considered
    # unresponsive within a window.
    # At speed_multiplier=1   → 120 real seconds before flagging silence
    # At speed_multiplier=10  → 12 real seconds before flagging silence
    silence_threshold_seconds: int = 120


@dataclass
class PowerBIConfig:
    """
    Controls the Power BI streaming dataset push.

    Set enabled=False during development to skip the push entirely —
    aggregates will print to terminal instead.
    """

    # Master switch — flip to True when you have a Power BI workspace ready.
    enabled: bool = False

    # Power BI streaming dataset push URL.
    # Get this from Power BI: New Dashboard → Add Tile → Streaming Dataset
    # → API → Create → copy the "Push URL" here.
    push_url: str = ""
    # How many zone aggregates to batch before pushing.
    # 1 = push immediately after every window close (lowest latency).
    # Higher = fewer HTTP calls but slightly more latency.
    batch_size: int = 1

    # Timeout in seconds for each HTTP push request.
    request_timeout_seconds: int = 5


@dataclass
class ConsumerConfig:
    """Top-level config — the only thing consumers need to import."""
    kafka:       KafkaConsumerConfig = field(default_factory=KafkaConsumerConfig)
    aggregation: AggregationConfig   = field(default_factory=AggregationConfig)
    powerbi:     PowerBIConfig       = field(default_factory=PowerBIConfig)
