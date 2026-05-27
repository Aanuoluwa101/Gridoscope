"""
meter_profile.py — Defines what a meter IS and how to generate a fleet of them.

A MeterProfile is the static identity of a meter — the characteristics that
don't change from reading to reading. Think of it as the meter's personality.
The simulation engine uses this profile to shape every event that meter produces.

Separating static profile from dynamic state (which lives in MeterStateMachine)
is an important design decision. It makes the code easier to reason about:
  - Profile  = "what kind of meter is this?"
  - State    = "what is this meter doing right now?"
"""

import random
import uuid
from dataclasses import dataclass, field
from typing import Optional

from config import GridoscopeConfig


# ---------------------------------------------------------------------------
# Customer type constants
# ---------------------------------------------------------------------------

RESIDENTIAL = "residential"
COMMERCIAL  = "commercial"
INDUSTRIAL  = "industrial"


# ---------------------------------------------------------------------------
# MeterProfile dataclass
# ---------------------------------------------------------------------------

@dataclass
class MeterProfile:
    """
    The static identity of a single smart meter.

    Every field here is set once at startup and never changes during the
    simulation. These values shape the meter's readings across its entire
    lifetime — they are the source of realistic variation across the fleet.
    """

    # Unique identifier. Format: MTR-XXXX (zero-padded index).
    # Also used as the Kafka message key within a zone so Kafka can
    # route consistently if you ever need per-meter partitioning.
    meter_id: str

    # Which city zone this meter belongs to.
    # This is the Kafka partition key — all meters in a zone land on
    # the same partition, owned by the same consumer.
    zone_id: str

    # One of: residential | commercial | industrial
    # Drives interval, consumption profile, and peak hours.
    customer_type: str

    # Average power draw in kilowatts under neutral (non-peak) conditions.
    # This is the anchor point — all demand multipliers scale from here.
    base_consumption_kw: float

    # Standard deviation as a fraction of base_consumption_kw.
    # Controls how noisy normal readings are. Higher = more variable meter.
    # e.g. 0.05 means readings fluctuate roughly ±5% around the base.
    consumption_std: float

    # Hours of the day (0-23) when this meter draws significantly more power.
    # Residential: morning + evening. Commercial: business hours. Industrial: flat.
    peak_hours: list

    # How much base_consumption_kw is multiplied during peak_hours.
    # e.g. 2.5 means this meter draws 2.5× its base during peak periods.
    peak_multiplier: float

    # Nominal (expected) voltage. Residential: ~120V (US). Commercial: ~240V.
    # Industrial: ~480V. Deviations from this flag potential faults.
    voltage_nominal: float

    # Expected current draw in amps at base consumption.
    # Derived from base_consumption_kw at startup, used to generate
    # realistic current readings in each event.
    current_nominal: float

    # Expected power factor. Healthy value: 0.90–0.99.
    # Commercial/industrial meters often have slightly lower PF due to
    # inductive loads (motors, HVAC compressors).
    power_factor_nominal: float

    # Base probability of transitioning to a fault state per reading cycle.
    # Older (higher-indexed) meters have a higher value here — set at profile
    # generation time by the fault config's age_fault_multiplier.
    fault_probability: float

    # Reading interval in seconds for this meter type.
    # Already divided by speed_multiplier at profile generation time so
    # every meter just sleeps for interval_seconds without needing to
    # know about time acceleration.
    interval_seconds: float

    # Cumulative kWh reading — starts at a realistic non-zero value
    # (meters in the field have been running for years, not just installed).
    # This is the odometer on the meter. We increment it each reading.
    cumulative_kwh: float = field(default_factory=lambda: random.uniform(1000, 50000))

    # Optional: a human-readable address label for the meter.
    # Useful in dashboard tooltips but not used in simulation logic.
    address_label: Optional[str] = None


# ---------------------------------------------------------------------------
# Profile factory functions — one per customer type
# ---------------------------------------------------------------------------

def _make_residential_profile(
    meter_id: str,
    zone_id: str,
    meter_index: int,
    cfg: GridoscopeConfig,
    rng: random.Random,
) -> MeterProfile:
    """
    Generate a residential meter profile.

    Residential meters have:
    - Low base draw (1.2–3.5 kW) — a house, not a factory
    - Morning peak (7–9am) and evening peak (6–10pm)
    - Higher weekend consumption (people home all day)
    - 120V nominal (US standard residential)
    - Slight variation in peak multiplier — not every house behaves identically
    """
    base_kw = rng.uniform(1.2, 3.5)
    voltage = 120.0

    # current (amps) = power (watts) / voltage
    # power_factor accounts for reactive load in household appliances
    pf = rng.uniform(0.92, 0.99)
    current = (base_kw * 1000) / (voltage * pf)

    # Age-adjusted fault probability: older meters fail more often
    age_factor = 1.0 + (meter_index / cfg.simulation.total_meters) * cfg.faults.age_fault_multiplier
    fault_prob = cfg.faults.base_fault_probability * age_factor

    return MeterProfile(
        meter_id=meter_id,
        zone_id=zone_id,
        customer_type=RESIDENTIAL,
        base_consumption_kw=base_kw,
        consumption_std=rng.uniform(0.04, 0.08),   # 4–8% normal noise
        peak_hours=list(range(7, 10)) + list(range(18, 23)),  # 7-9am, 6-10pm
        peak_multiplier=rng.uniform(2.0, 3.0),     # 2–3× at peak
        voltage_nominal=voltage,
        current_nominal=current,
        power_factor_nominal=pf,
        fault_probability=fault_prob,
        interval_seconds=cfg.intervals.residential_seconds / cfg.simulation.speed_multiplier,
    )


def _make_commercial_profile(
    meter_id: str,
    zone_id: str,
    meter_index: int,
    cfg: GridoscopeConfig,
    rng: random.Random,
) -> MeterProfile:
    """
    Generate a commercial meter profile.

    Commercial meters have:
    - Higher base draw (8–25 kW) — offices, retail, restaurants
    - Business-hours peak (9am–6pm, weekdays only)
    - Near-idle on nights and weekends
    - 240V nominal (US commercial single-phase)
    - Lower power factor than residential — inductive loads from HVAC
    """
    base_kw = rng.uniform(8.0, 25.0)
    voltage = 240.0
    pf = rng.uniform(0.85, 0.94)    # lower PF — more inductive loads
    current = (base_kw * 1000) / (voltage * pf)

    age_factor = 1.0 + (meter_index / cfg.simulation.total_meters) * cfg.faults.age_fault_multiplier
    fault_prob = cfg.faults.base_fault_probability * age_factor * 0.8  # slightly more reliable infra

    return MeterProfile(
        meter_id=meter_id,
        zone_id=zone_id,
        customer_type=COMMERCIAL,
        base_consumption_kw=base_kw,
        consumption_std=rng.uniform(0.06, 0.12),   # more variable than residential
        peak_hours=list(range(9, 19)),              # 9am–6pm
        peak_multiplier=rng.uniform(1.8, 2.8),
        voltage_nominal=voltage,
        current_nominal=current,
        power_factor_nominal=pf,
        fault_probability=fault_prob,
        interval_seconds=cfg.intervals.commercial_seconds / cfg.simulation.speed_multiplier,
    )


def _make_industrial_profile(
    meter_id: str,
    zone_id: str,
    meter_index: int,
    cfg: GridoscopeConfig,
    rng: random.Random,
) -> MeterProfile:
    """
    Generate an industrial meter profile.

    Industrial meters have:
    - Very high base draw (40–120 kW) — manufacturing, warehouses, data centres
    - Relatively flat consumption — machinery runs on schedule, not by time of day
    - 480V nominal (US three-phase industrial standard)
    - Lowest power factor — heavy inductive motor loads
    - Occasional large step-changes when equipment starts/stops (modelled as
      periodic spike events rather than through peak_hours)
    """
    base_kw = rng.uniform(40.0, 120.0)
    voltage = 480.0
    pf = rng.uniform(0.78, 0.90)
    current = (base_kw * 1000) / (voltage * pf)

    age_factor = 1.0 + (meter_index / cfg.simulation.total_meters) * cfg.faults.age_fault_multiplier
    fault_prob = cfg.faults.base_fault_probability * age_factor * 1.5  # harsher environment

    return MeterProfile(
        meter_id=meter_id,
        zone_id=zone_id,
        customer_type=INDUSTRIAL,
        base_consumption_kw=base_kw,
        consumption_std=rng.uniform(0.02, 0.05),   # very consistent baseline
        peak_hours=[],                              # no time-of-day peak — always on
        peak_multiplier=1.0,                        # flat profile
        voltage_nominal=voltage,
        current_nominal=current,
        power_factor_nominal=pf,
        fault_probability=fault_prob,
        interval_seconds=cfg.intervals.industrial_seconds / cfg.simulation.speed_multiplier,
    )


# ---------------------------------------------------------------------------
# Fleet generator — creates all 500 meter profiles at once
# ---------------------------------------------------------------------------

def generate_meter_fleet(cfg: GridoscopeConfig) -> list[MeterProfile]:
    """
    Generate the full fleet of meter profiles based on config.

    This runs once at startup. The resulting list is passed to the simulation
    engine which creates a MeterStateMachine for each profile.

    Design decisions:
    - Meters are distributed round-robin across zones so each zone gets a
      proportional mix of residential/commercial/industrial types.
    - A seeded RNG is used throughout so the fleet is reproducible when
      cfg.simulation.random_seed is set. Reproducibility is valuable during
      development — same fleet, same faults, same patterns every run.
    - Meter indices are global (not per-zone) so age-based fault probability
      is consistent across zones.
    """

    rng = random.Random(cfg.simulation.random_seed)

    total       = cfg.simulation.total_meters
    n_res       = int(total * cfg.simulation.residential_pct)
    n_com       = int(total * cfg.simulation.commercial_pct)
    n_ind       = total - n_res - n_com   # remainder goes to industrial

    # Build a list of (customer_type, factory_function) pairs
    # in the proportion defined by config, then shuffle them so
    # types are distributed evenly across zones rather than clustered.
    type_assignments = (
        [(RESIDENTIAL, _make_residential_profile)] * n_res +
        [(COMMERCIAL,  _make_commercial_profile)]  * n_com +
        [(INDUSTRIAL,  _make_industrial_profile)]  * n_ind
    )
    rng.shuffle(type_assignments)

    zones      = cfg.simulation.zones
    n_zones    = len(zones)
    fleet: list[MeterProfile] = []

    for index, (customer_type, factory_fn) in enumerate(type_assignments):
        # Assign zone round-robin so each zone gets roughly equal meter counts
        zone_id  = zones[index % n_zones]
        meter_id = f"MTR-{index:04d}"   # MTR-0000 through MTR-0499

        profile = factory_fn(
            meter_id=meter_id,
            zone_id=zone_id,
            meter_index=index,
            cfg=cfg,
            rng=rng,
        )
        fleet.append(profile)

    print(f"[Fleet] Generated {len(fleet)} meters: "
          f"{n_res} residential, {n_com} commercial, {n_ind} industrial "
          f"across {n_zones} zones")

    return fleet
