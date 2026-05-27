"""
settings.py — Central configuration for the Gridoscope producer engine.

All tuneable parameters live here. Nothing is hardcoded in the simulation
logic itself — this makes it easy to swap between a lightweight dev run
and a full-scale production-like simulation without touching any other file.
"""

from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime


# ---------------------------------------------------------------------------
# Kafka connection
# ---------------------------------------------------------------------------

@dataclass
class KafkaConfig:
    """
    Connection details for your Kafka broker.

    In development you'll point this at a local broker or a small MSK cluster.
    In production this becomes your full MSK bootstrap string.
    """

    bootstrap_servers: str = "localhost:9092"

    # The topic that receives all scheduled meter readings.
    # Partitioned by zone_id so consumers can own a full zone.
    readings_topic: str = "meter.readings"

    # Separate topic for fault and alert events.
    # These are event-driven (not scheduled) so they deserve their own topic
    # — consumers can subscribe to just alerts without processing all readings.
    alerts_topic: str = "meter.alerts"

    # How many times to retry a failed send before giving up.
    # In production you'd want this higher (5-10).
    retries: int = 3

    # Maximum time (ms) to wait before flushing a batch of messages.
    # Lower = lower latency, higher = better throughput.
    # 100ms is a reasonable middle ground for this use case.
    linger_ms: int = 100

    # Maximum size (bytes) of a single batch sent to the broker.
    # 16KB is a good default — large enough for efficiency, small enough
    # that individual meter events don't get held up waiting for a full batch.
    batch_size: int = 16_384


# ---------------------------------------------------------------------------
# Simulation behaviour
# ---------------------------------------------------------------------------

@dataclass
class SimulationConfig:
    """
    Controls the scale and behaviour of the simulation engine.

    The most important parameter here is speed_multiplier — it lets you
    run the simulation faster than real time during development so you
    can generate hours of data in minutes.
    """

    # Simulation start time. None means start from current real time.
    # Set this to a specific datetime to replay from a fixed point.
    # e.g. sim_start=datetime(2026, 1, 1, 6, 0, 0) starts at 6am on Jan 1
    sim_start: Optional[datetime] = None

    # Total number of meters to simulate.
    # 500 is realistic for a medium-sized district utility deployment.
    total_meters: int = 500

    # Mix of customer types across the meter fleet.
    # These should sum to 1.0.
    # Residential-heavy mirrors real utility distributions.
    residential_pct: float = 0.70   # 350 meters
    commercial_pct: float = 0.24    # 120 meters
    industrial_pct: float = 0.06    #  30 meters

    # City zones. Each zone gets a proportional share of meters.
    # Partitioning Kafka by zone means one consumer owns all meters in a zone —
    # critical for stateful per-zone aggregation without cross-consumer coordination.
    zones: list = field(default_factory=lambda: [
        "ZONE-NORTH",
        "ZONE-SOUTH",
        "ZONE-EAST",
        "ZONE-WEST",
        "ZONE-CENTRAL",
    ])

    # How much faster than real time to run the simulation.
    # speed_multiplier = 1   → real time (30 simulated seconds = 30 wall-clock seconds)
    # speed_multiplier = 10  → 10× faster (30 simulated seconds = 3 wall-clock seconds)
    # speed_multiplier = 60  → 1 simulated minute per wall-clock second
    #
    # Use high values (60-120) to rapidly generate historical data for testing.
    # Use 1-2 for a realistic live demo where the dashboard updates feel natural.
    speed_multiplier: float = 1.0

    # Jitter range in seconds added to each meter's sleep interval.
    # Without jitter, all 500 meters fire at exactly the same second,
    # creating artificial burst spikes. Real meters drift slightly —
    # this models that drift.
    interval_jitter_seconds: float = 5.0

    # Seed for the random number generator.
    # Set this to a fixed integer (e.g. 42) for reproducible runs during
    # testing. Set to None for a fresh random run each time.
    random_seed: Optional[int] = None


# ---------------------------------------------------------------------------
# Meter reading intervals (seconds)
# ---------------------------------------------------------------------------

@dataclass
class IntervalConfig:
    """
    How often each meter type sends a scheduled reading.

    These reflect real Advanced Metering Infrastructure (AMI) standards:
    - Residential: 5-minute intervals (300s) — standard utility billing granularity
    - Commercial:  2-minute intervals (120s) — tighter monitoring for demand charges
    - Industrial:  1-minute intervals  (60s) — demand response and machinery tracking

    These are divided by speed_multiplier at runtime so the simulator
    respects your chosen time acceleration.
    """
    residential_seconds: int = 300   # 5 minutes
    commercial_seconds: int  = 120   # 2 minutes
    industrial_seconds: int  = 60    # 1 minute


# ---------------------------------------------------------------------------
# Fault simulation parameters
# ---------------------------------------------------------------------------

@dataclass
class FaultConfig:
    """
    Controls how often and how severely faults occur in the network.

    The probabilities here are per-reading-cycle, so a base_fault_probability
    of 0.001 means roughly 1 in 1000 normal readings triggers a fault transition.
    Across 500 meters that gives you occasional but realistic fault events.
    """

    # Base probability of transitioning from NORMAL → DEGRADED per cycle.
    # Applied to every meter every time it generates a reading.
    base_fault_probability: float = 0.001

    # Multiplier applied to fault probability during high-temperature scenarios.
    # At 40°C, fault rate triples. Reflects real thermal stress on equipment.
    heat_stress_multiplier: float = 3.0

    # Temperature threshold (°C) above which heat stress begins scaling up.
    heat_stress_threshold_c: float = 30.0

    # How much older meters (higher meter index) are more fault-prone.
    # meter_age_factor = meter_index / total_meters, then multiplied here.
    age_fault_multiplier: float = 2.0

    # Once in DEGRADED state, probability of escalating to full FAULT per cycle.
    degraded_to_fault_probability: float = 0.05

    # Once in DEGRADED state, probability of self-healing back to NORMAL per cycle.
    # Self-healing is more likely than escalating — degraded meters often recover.
    degraded_recovery_probability: float = 0.15

    # Once in FAULT state, probability of being reset (technician intervention).
    fault_recovery_probability: float = 0.02

    # Probability per cycle of a NORMAL meter going SILENT (comms failure).
    silence_probability: float = 0.0005

    # Once SILENT, probability per cycle of reconnecting.
    silence_recovery_probability: float = 0.05

    # Voltage drift range (volts) when a meter is in DEGRADED state.
    # Applied as a random offset from nominal voltage.
    degraded_voltage_drift_range: tuple = (-20.0, 20.0)

    # How much noisier readings get in DEGRADED state.
    # Multiplied against the meter's normal consumption_std.
    degraded_noise_multiplier: float = 3.0

    # Power factor drop when degraded. Real degraded meters show poor PF.
    degraded_pf_drop: float = 0.15


# ---------------------------------------------------------------------------
# Scenario engine
# ---------------------------------------------------------------------------

@dataclass
class ScenarioConfig:
    """
    Predefined scenarios that override normal simulation behaviour
    at specific times. The scenario engine reads this list and injects
    effects into affected meters when the simulation clock hits trigger_hour.

    This lets you demonstrate specific dashboard behaviours on demand —
    e.g. trigger a heatwave to show the fault rate spike, or a zone outage
    to show the silent meter alert cascade.
    """

    scenarios: list = field(default_factory=lambda: [
        {
            "name": "evening_peak",
            "trigger_hour": 18,          # 6pm
            "affected_zones": ["ZONE-NORTH", "ZONE-WEST"],
            "effect": "demand_spike",
            "demand_multiplier": 1.8,
            "duration_minutes": 90,
        },
        {
            "name": "heatwave",
            "trigger_hour": 13,          # 1pm
            "affected_zones": ["ALL"],
            "effect": "demand_spike_plus_faults",
            "demand_multiplier": 1.6,
            "fault_rate_increase": 3.0,
            "duration_minutes": 180,
        },
        {
            "name": "zone_outage",
            "trigger_hour": 2,           # 2am — low-traffic window
            "affected_zones": ["ZONE-SOUTH"],
            "effect": "mass_silence",
            "pct_meters_affected": 0.70,
            "duration_minutes": 45,
        },
        {
            "name": "morning_commercial_ramp",
            "trigger_hour": 8,
            "affected_zones": ["ZONE-CENTRAL"],
            "effect": "demand_spike",
            "demand_multiplier": 1.5,
            "duration_minutes": 60,
        },
    ])


# ---------------------------------------------------------------------------
# Top-level config object — the only thing you need to import elsewhere
# ---------------------------------------------------------------------------

@dataclass
class GridoscopeConfig:
    """
    Aggregates all config sections into a single object.

    Usage:
        from config.settings import GridoscopeConfig
        cfg = GridoscopeConfig()

    Or override specific values:
        cfg = GridoscopeConfig(
            simulation=SimulationConfig(speed_multiplier=10.0, total_meters=50),
            kafka=KafkaConfig(bootstrap_servers="my-msk-endpoint:9092")
        )
    """
    kafka:      KafkaConfig      = field(default_factory=KafkaConfig)
    simulation: SimulationConfig = field(default_factory=SimulationConfig)
    intervals:  IntervalConfig   = field(default_factory=IntervalConfig)
    faults:     FaultConfig      = field(default_factory=FaultConfig)
    scenarios:  ScenarioConfig   = field(default_factory=ScenarioConfig)
