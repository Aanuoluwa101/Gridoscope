"""
meter_state_machine.py — The heart of each meter's runtime behaviour.

Every meter in the fleet gets its own MeterStateMachine instance. This object
tracks the meter's current state (NORMAL, DEGRADED, FAULT, SILENT) and is
responsible for generating the next event based on that state.

Why a state machine?
  Real IoT devices don't randomly jump between health states. They degrade
  gradually before failing, self-heal when conditions improve, and sometimes
  go silent without warning. A state machine models these transitions explicitly
  and makes the logic easy to reason about and extend.

State transitions:
  NORMAL ──[fault trigger]──▶ DEGRADED ──[escalate]──▶ FAULT
  NORMAL ──[comms failure]──▶ SILENT
  DEGRADED ──[self-heal]──▶ NORMAL
  FAULT ──[technician reset]──▶ NORMAL
  SILENT ──[reconnect]──▶ NORMAL
"""

import random
import json
from dataclasses import dataclass, asdict
from datetime import datetime
from enum import Enum
from typing import Optional

from meter_profile import MeterProfile, RESIDENTIAL, INDUSTRIAL
from config import GridoscopeConfig


# ---------------------------------------------------------------------------
# State enumeration
# ---------------------------------------------------------------------------

class MeterState(Enum):
    """
    The four possible health states of a meter.

    NORMAL   — everything working as expected, readings are clean
    DEGRADED — still reporting but readings are erratic, voltage drifting
    FAULT    — meter is malfunctioning, readings are unreliable or extreme
    SILENT   — meter has stopped transmitting entirely (comms failure)
    """
    NORMAL   = "normal"
    DEGRADED = "degraded"
    FAULT    = "fault"
    SILENT   = "silent"


# ---------------------------------------------------------------------------
# Event dataclass — one reading event to be sent to Kafka
# ---------------------------------------------------------------------------

@dataclass
class MeterEvent:
    """
    A single reading event from one meter at one point in time.

    This is the schema of every message that lands in the meter.readings
    or meter.alerts Kafka topic. Keeping it as a dataclass means we can
    easily serialise it to JSON and also enforce field types at creation time.
    """

    # --- Identity fields ---
    event_id: str           # UUID — unique per event, useful for deduplication
    meter_id: str
    zone_id: str
    customer_type: str
    timestamp: str          # ISO 8601 UTC — consistent format for downstream parsing

    # --- Consumption fields ---
    kwh_reading: float      # Cumulative odometer reading on this meter
    kwh_delta: float        # Consumption since last reading (the main analytical field)

    # --- Electrical health fields ---
    voltage: float          # Actual voltage measured at this reading
    current_amps: float     # Actual current
    power_factor: float     # Ratio of real to apparent power (healthy: >0.90)
    power_kw: float         # Derived: voltage × current × power_factor / 1000
    frequency_hz: float     # Grid frequency — should be 60Hz ±0.5 in healthy grid

    # --- Meter health fields ---
    meter_state: str        # Current state name — for monitoring and alerting
    is_anomaly: bool        # True if kwh_delta is abnormally high for this meter
    signal_strength_dbm: int  # Comms signal quality (negative dBm: -50 good, -90 poor)

    # --- Metadata ---
    firmware_version: str   # Useful for tracking firmware rollout issues
    event_type: str = "reading"  # "reading" for normal events, "alert" for fault events

    def to_json(self) -> str:
        """Serialise to JSON string for Kafka message value."""
        return json.dumps(asdict(self))


# ---------------------------------------------------------------------------
# Simulation clock — handles time acceleration
# ---------------------------------------------------------------------------

class SimulationClock:
    """
    A clock that advances faster than wall time when speed_multiplier > 1.

    This is what makes time acceleration work. Instead of reading datetime.utcnow()
    directly, every part of the simulation reads time from this clock.

    How it works:
      The clock records the real wall-clock time at startup (wall_start).
      Every call to now() computes:
        elapsed_wall_seconds = (current wall time) - wall_start
        simulated_time = sim_start + (elapsed_wall_seconds × speed_multiplier)

    So if speed_multiplier = 60, one real second advances the simulation by
    one simulated minute. You can generate 24 hours of data in 24 minutes.
    """

    def __init__(self, speed_multiplier: float, sim_start: Optional[datetime] = None):
        import time
        self._speed          = speed_multiplier
        self._wall_start     = time.monotonic()
        # Simulation can start at any datetime — defaults to now
        self._sim_start      = sim_start or datetime.utcnow()

    def now(self) -> datetime:
        """Return the current simulated datetime."""
        import time
        elapsed_wall   = time.monotonic() - self._wall_start
        elapsed_sim    = elapsed_wall * self._speed
        from datetime import timedelta
        return self._sim_start + timedelta(seconds=elapsed_sim)

    def hour(self) -> int:
        """Convenience: current simulated hour (0-23)."""
        return self.now().hour

    def is_weekend(self) -> bool:
        """Convenience: is the current simulated day a Saturday or Sunday?"""
        return self.now().weekday() >= 5


# ---------------------------------------------------------------------------
# MeterStateMachine
# ---------------------------------------------------------------------------

class MeterStateMachine:
    """
    Manages the runtime state of a single meter and generates its events.

    One instance is created per meter at simulation startup and lives for
    the entire run. It holds:
    - The meter's static profile (what kind of meter it is)
    - The meter's current dynamic state (NORMAL, DEGRADED, etc.)
    - A rolling history of recent kWh deltas for anomaly detection
    - Any active scenario effects injected by the ScenarioEngine
    """

    # How many past readings to keep for rolling average calculation.
    # 12 readings = 1 hour of history for a residential meter (5 min intervals).
    ROLLING_WINDOW_SIZE = 12

    def __init__(
        self,
        profile: MeterProfile,
        cfg: GridoscopeConfig,
        clock: SimulationClock,
        rng: random.Random,
    ):
        self.profile = profile
        self.cfg     = cfg
        self.clock   = clock
        self.rng     = rng

        self.state = MeterState.NORMAL

        # Degraded state parameters — set when entering DEGRADED, cleared on recovery
        self._voltage_drift      = 0.0
        self._pf_degradation     = 0.0
        self._noise_multiplier   = 1.0

        # Rolling history of kWh deltas for anomaly detection.
        # Pre-populate with the nominal delta so anomaly detection works
        # immediately without a cold-start window.
        nominal_delta = self._nominal_delta()
        self._delta_history: list[float] = [nominal_delta] * self.ROLLING_WINDOW_SIZE

        # Scenario effects injected externally by the ScenarioEngine.
        # demand_multiplier_override: when set, multiplies demand instead of
        # the normal time-of-day curve. Used for heatwave/evening peak scenarios.
        self._demand_multiplier_override: Optional[float] = None
        self._fault_rate_override:        Optional[float] = None

        self._hourly_avg: dict[int, float] = {}

    # -----------------------------------------------------------------------
    # Main entry point — called every interval to produce one event
    # -----------------------------------------------------------------------

    def generate_event(self) -> Optional[MeterEvent]:
        """
        Produce the next meter event based on current state.

        Returns None when the meter is SILENT — no event should be sent
        to Kafka. The consumer detects silence by noticing the absence
        of expected events, not by receiving a "silent" message.

        This is important: real silent meters don't announce themselves.
        Your consumer has to infer silence from missing heartbeats.
        """

        # Advance the state machine first — maybe the meter changes state
        # this cycle before we generate the reading
        self._transition_state()

        if self.state == MeterState.SILENT:
            # No event — consumer will detect via timeout
            return None

        sim_time    = self.clock.now()
        kwh_delta   = self._compute_kwh_delta(sim_time)
        voltage     = self._compute_voltage()
        current     = self._compute_current(kwh_delta, voltage)
        pf          = self._compute_power_factor()
        power_kw    = (voltage * current * pf) / 1000.0
        freq        = self._compute_frequency()
        is_anomaly  = self._detect_anomaly(kwh_delta, hour=sim_time.hour)
        signal      = self._compute_signal_strength()

        self.profile.cumulative_kwh += kwh_delta

        # Update rolling delta history for future anomaly detection
        self._delta_history.append(kwh_delta)
        if len(self._delta_history) > self.ROLLING_WINDOW_SIZE:
            self._delta_history.pop(0)

        import uuid as _uuid
        event = MeterEvent(
            event_id          = str(_uuid.uuid4()),
            meter_id          = self.profile.meter_id,
            zone_id           = self.profile.zone_id,
            customer_type     = self.profile.customer_type,
            timestamp         = sim_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            kwh_reading       = round(self.profile.cumulative_kwh, 3),
            kwh_delta         = round(kwh_delta, 4),
            voltage           = round(voltage, 2),
            current_amps      = round(current, 3),
            power_factor      = round(pf, 3),
            power_kw          = round(power_kw, 3),
            frequency_hz      = round(freq, 3),
            meter_state       = self.state.value,
            is_anomaly        = is_anomaly,
            signal_strength_dbm = signal,
            firmware_version  = "v3.2.1",
            event_type        = "alert" if self.state != MeterState.NORMAL else "reading",
        )
        return event

    # -----------------------------------------------------------------------
    # State transition logic
    # -----------------------------------------------------------------------

    def _transition_state(self) -> None:
        """
        Evaluate whether the meter should change health state this cycle.

        Each state has its own transition probabilities. Transitions are
        probabilistic — we roll a random number each cycle and compare it
        against the relevant threshold.
        """
        roll = self.rng.random()

        if self.state == MeterState.NORMAL:
            fault_prob   = self._adjusted_fault_probability()
            silence_prob = self.cfg.faults.silence_probability

            if roll < fault_prob:
                self._enter_degraded()
            elif roll < fault_prob + silence_prob:
                self._enter_silent()

        elif self.state == MeterState.DEGRADED:
            recovery_prob  = self.cfg.faults.degraded_recovery_probability
            escalate_prob  = self.cfg.faults.degraded_to_fault_probability

            if roll < recovery_prob:
                self._enter_normal()
            elif roll < recovery_prob + escalate_prob:
                self._enter_fault()
            # else: stay degraded this cycle

        elif self.state == MeterState.FAULT:
            if roll < self.cfg.faults.fault_recovery_probability:
                self._enter_normal()

        elif self.state == MeterState.SILENT:
            if roll < self.cfg.faults.silence_recovery_probability:
                self._enter_normal()

    def _adjusted_fault_probability(self) -> float:
        """
        Compute the fault probability for this cycle, accounting for:
        - The meter's base fault probability (from its profile)
        - Any scenario-injected fault rate override (e.g. during heatwave)
        - Temperature stress at the current simulation time
          (we approximate temperature by simulated hour — hotter mid-afternoon)

        In a more complete implementation you'd pull a real temperature
        from Open-Meteo here. For now we model it with a simple hourly curve.
        """
        base = self.profile.fault_probability

        # Approximate temperature stress from time of day.
        # Peak heat stress at 14:00 (2pm), zero stress overnight.
        hour = self.clock.hour()
        if 12 <= hour <= 17:
            # Scale from 0 at noon to 1.0 at 2pm and back to 0 at 5pm
            stress = max(0, 1.0 - abs(hour - 14) / 3.0)
            base *= 1.0 + stress * (self.cfg.faults.heat_stress_multiplier - 1.0)

        if self._fault_rate_override is not None:
            base *= self._fault_rate_override

        return base

    # -----------------------------------------------------------------------
    # State entry helpers — set or clear state-specific parameters
    # -----------------------------------------------------------------------

    def _enter_normal(self) -> None:
        self.state               = MeterState.NORMAL
        self._voltage_drift      = 0.0
        self._pf_degradation     = 0.0
        self._noise_multiplier   = 1.0

    def _enter_degraded(self) -> None:
        self.state = MeterState.DEGRADED
        cfg = self.cfg.faults
        # Randomise the degradation characteristics — not all degraded meters
        # look the same. Some drift high, some low.
        self._voltage_drift    = self.rng.uniform(*cfg.degraded_voltage_drift_range)
        self._pf_degradation   = self.rng.uniform(0, cfg.degraded_pf_drop)
        self._noise_multiplier = cfg.degraded_noise_multiplier

    def _enter_fault(self) -> None:
        self.state             = MeterState.FAULT
        # Fault meters show extreme voltage deviation
        self._voltage_drift    = self.rng.choice([-1, 1]) * self.rng.uniform(30, 60)
        self._pf_degradation   = self.rng.uniform(0.2, 0.4)
        self._noise_multiplier = 6.0   # very noisy

    def _enter_silent(self) -> None:
        self.state = MeterState.SILENT
        # No parameters to set — silent meters don't produce events

    # -----------------------------------------------------------------------
    # Reading computation helpers
    # -----------------------------------------------------------------------

    def _nominal_delta(self) -> float:
        """
        Compute the expected kWh consumed in one interval under neutral conditions.

        kWh = power_kw × (interval_seconds / 3600)
        This is the baseline around which all demand multipliers scale.
        """
        interval_hours = self.profile.interval_seconds / 3600.0
        return self.profile.base_consumption_kw * interval_hours

    def _compute_kwh_delta(self, sim_time: datetime) -> float:
        """
        Compute how many kWh this meter consumed since its last reading.

        The demand multiplier stack works as follows (applied in order):
        1. Time-of-day × customer-type curve (peak hours)
        2. Weekend adjustment for residential
        3. Industrial step-change events (random large spikes)
        4. Scenario override (heatwave, evening peak, etc.)
        5. Gaussian noise (the natural variation in any reading)

        Layering these multipliers produces readings that have realistic
        seasonality, peaks, and randomness — not just flat or purely random values.
        """
        interval_hours = self.profile.interval_seconds / 3600.0
        hour           = sim_time.hour
        is_weekend     = sim_time.weekday() >= 5

        # Step 1: time-of-day peak multiplier
        if self.profile.peak_hours and hour in self.profile.peak_hours:
            multiplier = self.profile.peak_multiplier
        else:
            # Off-peak: scale down. Overnight residential is very low.
            if self.profile.customer_type == RESIDENTIAL and 0 <= hour <= 5:
                multiplier = 0.25
            elif self.profile.customer_type != RESIDENTIAL and (is_weekend or hour < 7 or hour > 20):
                multiplier = 0.15   # commercial near-idle outside business hours
            else:
                multiplier = 1.0

        # Step 2: weekend uplift for residential (people home all day)
        if self.profile.customer_type == RESIDENTIAL and is_weekend:
            multiplier *= 1.15

        # Step 3: industrial machinery step-change events
        # ~2% chance per cycle of a large load switching on or off
        if self.profile.customer_type == INDUSTRIAL and self.rng.random() < 0.02:
            multiplier *= self.rng.uniform(1.4, 2.2)

        # Step 4: scenario override takes precedence over the time-of-day curve
        if self._demand_multiplier_override is not None:
            multiplier = self._demand_multiplier_override

        # Step 5: fault state amplifies noise heavily
        effective_std = self.profile.consumption_std * self._noise_multiplier

        # Apply multiplier and noise
        base_delta = self.profile.base_consumption_kw * multiplier * interval_hours
        noise      = self.rng.gauss(0, effective_std * base_delta)
        delta      = max(0.0, base_delta + noise)

        return delta

    def _compute_voltage(self) -> float:
        """
        Compute measured voltage, accounting for normal grid fluctuation
        and any fault-state drift.

        Healthy grid voltage fluctuates ±2% around nominal. Faults push
        this further outside the acceptable range, which downstream anomaly
        detection should flag.
        """
        # Normal grid fluctuation: ±2% of nominal
        normal_noise = self.rng.gauss(0, self.profile.voltage_nominal * 0.01)
        return self.profile.voltage_nominal + normal_noise + self._voltage_drift

    def _compute_current(self, kwh_delta: float, voltage: float) -> float:
        """
        Back-calculate current from the power we just determined.

        current = power_watts / voltage
        We clamp voltage to a minimum to avoid division-by-zero in fault states.
        """
        power_watts = (kwh_delta / (self.profile.interval_seconds / 3600.0)) * 1000.0
        safe_voltage = max(voltage, 10.0)
        return power_watts / safe_voltage

    def _compute_power_factor(self) -> float:
        """Power factor degrades with meter health. Clamp to [0.5, 1.0]."""
        pf = self.profile.power_factor_nominal - self._pf_degradation
        return max(0.5, min(1.0, pf))

    def _compute_frequency(self) -> float:
        """
        Grid frequency in Hz. Should be 60.0 ± 0.5 Hz (US standard).
        Fault meters occasionally show larger frequency deviations.
        """
        std = 0.02 if self.state == MeterState.NORMAL else 0.2
        return self.rng.gauss(60.0, std)

    # def _detect_anomaly(self, kwh_delta: float) -> bool:
    #     """
    #     Flag a reading as anomalous if it's more than 3× the rolling average.

    #     This is a simple statistical threshold. In production you'd use
    #     a proper anomaly detection model, but this gives you realistic flags
    #     for your dashboard without overcomplicating the simulator.
    #     """
    #     if not self._delta_history:
    #         return False
    #     rolling_avg = sum(self._delta_history) / len(self._delta_history)
    #     if rolling_avg == 0:
    #         return False
    #     return kwh_delta > rolling_avg * 3.0


    def _detect_anomaly(self, kwh_delta: float, hour: int) -> bool:
        """
        Flag a reading as anomalous only if it's abnormal for this
        specific hour of the day.

        We maintain a per-hour exponential moving average (EMA) rather
        than a flat rolling window. EMA gives more weight to recent
        readings while never fully forgetting older ones.

        A spike threshold of 4× gives more headroom than 3× — reduces
        false positives during demand transitions like morning ramp-up.
        """
        SPIKE_THRESHOLD = 4.0
        EMA_ALPHA       = 0.2   # smoothing factor: 0=never update, 1=no memory

        if hour not in self._hourly_avg:
            # First reading for this hour — initialise, no anomaly verdict yet
            self._hourly_avg[hour] = kwh_delta
            return False

        expected = self._hourly_avg[hour]

        # Update the EMA for this hour with the new reading
        # (do this AFTER the anomaly check so we don't let the spike
        # contaminate the baseline)
        is_spike = expected > 0 and kwh_delta > expected * SPIKE_THRESHOLD

        if not is_spike:
            # Normal reading — update the baseline
            self._hourly_avg[hour] = (EMA_ALPHA * kwh_delta) + ((1 - EMA_ALPHA) * expected)

        return is_spike

    def _compute_signal_strength(self) -> int:
        """
        Simulate RF signal strength in dBm (negative scale).
        -50 dBm = excellent, -90 dBm = very poor.
        DEGRADED meters tend to have worse signal — often correlated in the field.
        """
        if self.state == MeterState.NORMAL:
            return int(self.rng.gauss(-65, 8))
        elif self.state == MeterState.DEGRADED:
            return int(self.rng.gauss(-78, 10))
        else:
            return int(self.rng.gauss(-88, 5))

    # -----------------------------------------------------------------------
    # Scenario injection — called by ScenarioEngine
    # -----------------------------------------------------------------------

    def apply_scenario(
        self,
        demand_multiplier: Optional[float] = None,
        fault_rate_multiplier: Optional[float] = None,
        force_silent: bool = False,
    ) -> None:
        """
        Inject a scenario effect into this meter.

        Called by the ScenarioEngine at the appropriate simulation time.
        Effects stay active until clear_scenario() is called.
        """
        self._demand_multiplier_override = demand_multiplier
        self._fault_rate_override        = fault_rate_multiplier
        if force_silent:
            self._enter_silent()

    def clear_scenario(self) -> None:
        """Remove any active scenario effects — return to profile-driven behaviour."""
        self._demand_multiplier_override = None
        self._fault_rate_override        = None
