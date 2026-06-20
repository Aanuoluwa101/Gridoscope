"""
dry_run.py — Run the Gridoscope simulation without Kafka.

Validates the simulation engine — profiles, state machines, fault logic,
scenarios — by printing everything to the terminal instead of sending to Kafka.

Usage:
  python dry_run.py                             # 10 meters, real time
  python dry_run.py --meters 50 --speed 10      # bigger fleet, faster
  python dry_run.py --quiet                     # stats only, no per-event lines
  python dry_run.py --seed 42                   # reproducible run

Demo mode — force specific behaviours immediately without waiting for them
to occur randomly. Use --demo to pick what you want to see:

  python dry_run.py --demo list                 # show all available demos
  python dry_run.py --demo degraded             # some meters enter DEGRADED state
  python dry_run.py --demo fault                # some meters enter full FAULT state
  python dry_run.py --demo silent               # some meters go SILENT
  python dry_run.py --demo anomaly              # force anomalous readings
  python dry_run.py --demo heatwave             # activate heatwave scenario
  python dry_run.py --demo evening_peak         # activate evening peak scenario
  python dry_run.py --demo zone_outage          # take down 70% of ZONE-SOUTH
  python dry_run.py --demo all                  # everything at once

  # Combine with other flags:
  python dry_run.py --demo all --meters 20 --speed 5 --seed 42
"""

import argparse
import asyncio
import logging
import random
import sys
import os
from collections import defaultdict
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "producer"))

from config import (
    GridoscopeConfig, KafkaConfig, SimulationConfig,
    IntervalConfig, FaultConfig, ScenarioConfig,
)
from meter_profile import generate_meter_fleet, RESIDENTIAL, COMMERCIAL, INDUSTRIAL
from meter_state_machine import MeterStateMachine, SimulationClock, MeterEvent
from scenario_engine import ScenarioEngine


# ---------------------------------------------------------------------------
# Terminal colours
# ---------------------------------------------------------------------------

class C:
    RESET   = "\033[0m"
    BOLD    = "\033[1m"
    GREEN   = "\033[92m"
    YELLOW  = "\033[93m"
    RED     = "\033[91m"
    CYAN    = "\033[96m"
    MAGENTA = "\033[95m"
    BLUE    = "\033[94m"
    DIM     = "\033[2m"
    WHITE   = "\033[97m"
    ORANGE  = "\033[38;5;208m"

def coloured(text: str, *codes: str) -> str:
    return "".join(codes) + text + C.RESET


# ---------------------------------------------------------------------------
# Demo mode registry
# Every key here is a valid --demo argument.
# "all" is handled separately as a shortcut for everything else.
# ---------------------------------------------------------------------------

DEMO_MODES = {
    "degraded":     "Force ~30% of meters into DEGRADED state immediately",
    "fault":        "Force ~15% of meters into full FAULT state immediately",
    "silent":       "Force ~20% of meters into SILENT state immediately",
    "anomaly":      "Inject a one-shot spike so anomalous readings appear on next cycle",
    "heatwave":     "Activate heatwave scenario: demand ×1.6, fault rate ×3.0 on all meters",
    "evening_peak": "Activate evening peak: demand ×1.8 on ZONE-NORTH and ZONE-WEST",
    "zone_outage":  "Take 70% of ZONE-SOUTH meters SILENT immediately",
    "all":          "Activate all of the above simultaneously",
}


def print_demo_menu() -> None:
    """Print available demo modes. Called when --demo list is passed."""
    print()
    print(coloured("  Available --demo values:", C.BOLD, C.WHITE))
    print()
    for key, description in DEMO_MODES.items():
        print(f"  {coloured(key, C.CYAN):<30}  {description}")
    print()
    print(coloured("  Examples:", C.DIM))
    print(coloured("    python dry_run.py --demo fault", C.DIM))
    print(coloured("    python dry_run.py --demo all --meters 20 --speed 5", C.DIM))
    print(coloured("    python dry_run.py --demo zone_outage --seed 42", C.DIM))
    print()


def apply_demo_mode(
    demo: str,
    state_machines: list,
    meters_by_zone: dict,
    cfg: GridoscopeConfig,
    rng: random.Random,
) -> None:
    """
    Force specific conditions onto the fleet at startup.

    In normal operation you'd wait a long time for a fault to occur
    randomly (base probability ~0.001 per cycle). Demo mode bypasses that
    by directly calling the state machine internals — putting meters into
    the exact states we want to observe, immediately.

    Each demo prints a banner explaining what was injected and which meters
    are affected, so the output is self-documenting.
    """
    demos_to_run = [k for k in DEMO_MODES if k != "all"] if demo == "all" else [demo]

    for d in demos_to_run:
        _apply_single_demo(d, state_machines, meters_by_zone, cfg, rng)


def _apply_single_demo(
    demo: str,
    state_machines: list,
    meters_by_zone: dict,
    cfg: GridoscopeConfig,
    rng: random.Random,
) -> None:
    """Apply one specific demo injection and print a description banner."""

    print()
    print(coloured(f"  ▶ DEMO INJECTION: {demo.upper()}", C.BOLD, C.ORANGE))

    # ------------------------------------------------------------------
    if demo == "degraded":
        # DEGRADED meters still send events but readings become noisy —
        # voltage drifts outside normal range, power factor drops,
        # kwh_delta variance increases. These should be visible immediately
        # in the event stream as unusual voltage and PF values.
        targets = [sm for sm in state_machines if rng.random() < 0.30]
        for sm in targets:
            sm._enter_degraded()
        print(coloured(
            f"  {len(targets)} meters set to DEGRADED. "
            "Look for voltage outside ±2% of nominal and lower PF.",
            C.YELLOW
        ))
        _print_affected(targets)

    # ------------------------------------------------------------------
    elif demo == "fault":
        # FAULT meters send events tagged event_type="alert".
        # Their voltage deviates 30-60V from nominal. In a real system
        # every one of these would fire an alert on the ops dashboard.
        targets = [sm for sm in state_machines if rng.random() < 0.15]
        for sm in targets:
            sm._enter_fault()
        print(coloured(
            f"  {len(targets)} meters set to FAULT. "
            "Watch for [FAULT] state and 🔴 ALERT markers in the stream.",
            C.RED
        ))
        _print_affected(targets)

    # ------------------------------------------------------------------
    elif demo == "silent":
        # SILENT meters produce no events — they disappear from the stream.
        # In verbose mode you'll see "-- SILENT --" lines.
        # In a real consumer these would be detected by a timeout:
        # "meter X has not sent a reading in > 2 minutes → raise alert."
        targets = [sm for sm in state_machines if rng.random() < 0.20]
        for sm in targets:
            sm._enter_silent()
        print(coloured(
            f"  {len(targets)} meters set to SILENT. "
            "They produce no events — watch for '-- SILENT --' lines.",
            C.DIM
        ))
        _print_affected(targets)

    # ------------------------------------------------------------------
    elif demo == "anomaly":
        # Patch the _compute_kwh_delta method on ~25% of meters so their
        # NEXT reading fires at 6× normal — guaranteed to exceed the
        # anomaly threshold (3× or 4× rolling average).
        # The patch is one-shot: it removes itself after firing once,
        # returning the meter to normal behaviour immediately after.
        targets = [sm for sm in state_machines if rng.random() < 0.25]

        for sm in targets:
            original_fn = sm._compute_kwh_delta.__func__

            def make_spike(orig, target):
                fired = [False]

                def spiked(self_inner, sim_time):
                    if not fired[0]:
                        fired[0] = True
                        return orig(self_inner, sim_time) * 6.0   # one big spike
                    else:
                        # Self-remove: restore normal method after spike fires
                        import types
                        target._compute_kwh_delta = types.MethodType(orig, target)
                        return orig(self_inner, sim_time)

                return spiked

            import types
            sm._compute_kwh_delta = types.MethodType(make_spike(original_fn, sm), sm)

        print(coloured(
            f"  {len(targets)} meters will spike 6× on their next reading. "
            "Watch for ⚠ ANOMALY markers.",
            C.YELLOW
        ))
        _print_affected(targets)

    # ------------------------------------------------------------------
    elif demo == "heatwave":
        # Apply heatwave scenario effects directly to all meters —
        # bypasses the ScenarioEngine time-trigger so you see the effect
        # immediately rather than waiting for simulation clock = 13:00.
        # Demand ×1.6, fault rate ×3.0 means DEGRADED transitions happen
        # much more frequently while heatwave is active.
        for sm in state_machines:
            sm.apply_scenario(demand_multiplier=1.6, fault_rate_multiplier=3.0)
        print(coloured(
            f"  Heatwave active on all {len(state_machines)} meters. "
            "Demand ×1.6, fault rate ×3.0.",
            C.ORANGE
        ))
        print(coloured(
            "  Expect higher kWh deltas and more frequent DEGRADED transitions.",
            C.DIM
        ))

    # ------------------------------------------------------------------
    elif demo == "evening_peak":
        # Apply evening peak demand spike to ZONE-NORTH and ZONE-WEST.
        # Compare these zones against ZONE-SOUTH, ZONE-EAST, ZONE-CENTRAL
        # in the periodic stats to see the zone-level demand difference.
        peak_zones = ["ZONE-NORTH", "ZONE-WEST"]
        targets    = []
        for zone in peak_zones:
            zone_meters = meters_by_zone.get(zone, [])
            for sm in zone_meters:
                sm.apply_scenario(demand_multiplier=1.8)
            targets.extend(zone_meters)
        print(coloured(
            f"  Evening peak active on {len(targets)} meters "
            f"in {', '.join(peak_zones)}. Demand ×1.8.",
            C.CYAN
        ))
        print(coloured(
            "  Compare zone kWh totals in the stats ticker — "
            "NORTH and WEST should run visibly higher.",
            C.DIM
        ))

    # ------------------------------------------------------------------
    elif demo == "zone_outage":
        # Simulate a communication or substation failure in ZONE-SOUTH.
        # 70% of meters go SILENT immediately.
        # The remaining 30% continue normally — partial outage is realistic.
        # In a live dashboard this pattern (sudden zone-wide silence)
        # would trigger a zone-level alert distinct from individual meter faults.
        zone        = "ZONE-SOUTH"
        zone_meters = meters_by_zone.get(zone, [])
        targets     = [sm for sm in zone_meters if rng.random() < 0.70]
        for sm in targets:
            sm._enter_silent()
        print(coloured(
            f"  Zone outage: {len(targets)}/{len(zone_meters)} meters "
            f"in {zone} are now SILENT.",
            C.RED
        ))
        print(coloured(
            f"  {len(zone_meters) - len(targets)} meters in {zone} continue normally. "
            "In a live system this pattern triggers a zone-level alert.",
            C.DIM
        ))

    print()


def _print_affected(targets: list) -> None:
    """Print a compact list of affected meter IDs."""
    ids    = [sm.profile.meter_id for sm in targets]
    shown  = ", ".join(ids[:10])
    suffix = f" ... +{len(ids)-10} more" if len(ids) > 10 else ""
    print(coloured(f"  Meters: {shown}{suffix}", C.DIM))


# ---------------------------------------------------------------------------
# Fleet summary
# ---------------------------------------------------------------------------

def print_fleet_summary(fleet, cfg: GridoscopeConfig, demo: str = None) -> None:
    n_res = sum(1 for m in fleet if m.customer_type == RESIDENTIAL)
    n_com = sum(1 for m in fleet if m.customer_type == COMMERCIAL)
    n_ind = sum(1 for m in fleet if m.customer_type == INDUSTRIAL)

    print()
    print(coloured("=" * 65, C.BOLD, C.CYAN))
    title = "  Gridoscope — Dry Run Mode"
    if demo:
        title += f"  {coloured(f'[DEMO: {demo.upper()}]', C.BOLD, C.ORANGE)}"
    print(coloured(title, C.BOLD, C.CYAN))
    print(coloured("=" * 65, C.BOLD, C.CYAN))

    # Fleet overview
    print()
    print(coloured("  FLEET OVERVIEW", C.BOLD, C.WHITE))
    print(f"  {'Total meters':<28} {coloured(str(len(fleet)), C.BOLD, C.GREEN)}")
    print(f"  {'Residential':<28} {n_res}  ({n_res/len(fleet)*100:.0f}%)")
    print(f"  {'Commercial':<28} {n_com}  ({n_com/len(fleet)*100:.0f}%)")
    print(f"  {'Industrial':<28} {n_ind}  ({n_ind/len(fleet)*100:.0f}%)")
    print(f"  {'Speed multiplier':<28} {coloured(str(cfg.simulation.speed_multiplier) + '×', C.BOLD, C.YELLOW)}")
    print(f"  {'Random seed':<28} {cfg.simulation.random_seed or 'None (random)'}")

    # Zone distribution
    print()
    print(coloured("  ZONE DISTRIBUTION", C.BOLD, C.WHITE))
    zone_counts: dict = defaultdict(lambda: {RESIDENTIAL: 0, COMMERCIAL: 0, INDUSTRIAL: 0})
    for m in fleet:
        zone_counts[m.zone_id][m.customer_type] += 1

    print(f"  {'Zone':<20} {'Residential':>12} {'Commercial':>12} {'Industrial':>12} {'Total':>8}")
    print(f"  {'-'*20} {'-'*12} {'-'*12} {'-'*12} {'-'*8}")
    for zone in sorted(zone_counts.keys()):
        counts = zone_counts[zone]
        total  = sum(counts.values())
        print(
            f"  {zone:<20}"
            f" {counts[RESIDENTIAL]:>12}"
            f" {counts[COMMERCIAL]:>12}"
            f" {counts[INDUSTRIAL]:>12}"
            f" {coloured(str(total), C.BOLD):>8}"
        )

    # Intervals
    print()
    print(coloured("  READING INTERVALS", C.BOLD, C.WHITE))
    res_w = cfg.intervals.residential_seconds / cfg.simulation.speed_multiplier
    com_w = cfg.intervals.commercial_seconds  / cfg.simulation.speed_multiplier
    ind_w = cfg.intervals.industrial_seconds  / cfg.simulation.speed_multiplier
    print(f"  {'Residential':<28} {res_w:.1f}s wall-clock  ({cfg.intervals.residential_seconds}s simulated)")
    print(f"  {'Commercial':<28} {com_w:.1f}s wall-clock  ({cfg.intervals.commercial_seconds}s simulated)")
    print(f"  {'Industrial':<28} {ind_w:.1f}s wall-clock  ({cfg.intervals.industrial_seconds}s simulated)")

    # Throughput
    print()
    print(coloured("  ESTIMATED THROUGHPUT", C.BOLD, C.WHITE))
    epm = (
        (n_res / cfg.intervals.residential_seconds) * 60 +
        (n_com / cfg.intervals.commercial_seconds)  * 60 +
        (n_ind / cfg.intervals.industrial_seconds)  * 60
    )
    print(f"  {'Events/minute (simulated)':<28} {epm:.0f}")
    print(f"  {'Events/hour   (simulated)':<28} {epm * 60:,.0f}")
    print(f"  {'Events/day    (simulated)':<28} {epm * 60 * 24:,.0f}")

    # Sample profiles
    print()
    print(coloured("  SAMPLE METER PROFILES", C.BOLD, C.WHITE))
    print(f"  {'Meter':<10} {'Type':<14} {'Zone':<16} {'Base kW':>8} {'Voltage':>9} {'PF':>6} {'Interval':>10}")
    print(f"  {'-'*10} {'-'*14} {'-'*16} {'-'*8} {'-'*9} {'-'*6} {'-'*10}")
    type_colours = {RESIDENTIAL: C.GREEN, COMMERCIAL: C.YELLOW, INDUSTRIAL: C.MAGENTA}
    samples = (
        random.sample([m for m in fleet if m.customer_type == RESIDENTIAL], min(2, n_res)) +
        random.sample([m for m in fleet if m.customer_type == COMMERCIAL],  min(2, n_com)) +
        random.sample([m for m in fleet if m.customer_type == INDUSTRIAL],  min(2, n_ind))
    )
    for m in samples:
        col = type_colours[m.customer_type]
        print(
            f"  {m.meter_id:<10}"
            f" {coloured(m.customer_type, col):<14}"
            f" {m.zone_id:<16}"
            f" {m.base_consumption_kw:>8.2f}"
            f" {m.voltage_nominal:>8.1f}V"
            f" {m.power_factor_nominal:>6.2f}"
            f" {m.interval_seconds:>8.0f}s"
        )

    # Fault probabilities
    print()
    print(coloured("  FAULT PROBABILITIES", C.BOLD, C.WHITE))
    fc = cfg.faults
    print(f"  {'Normal → Degraded':<35} {fc.base_fault_probability:.4f} per cycle")
    print(f"  {'Degraded → Fault':<35} {fc.degraded_to_fault_probability:.2f} per cycle")
    print(f"  {'Degraded → Recovery':<35} {fc.degraded_recovery_probability:.2f} per cycle")
    print(f"  {'Normal → Silent':<35} {fc.silence_probability:.4f} per cycle")
    print(f"  {'Silent → Recovery':<35} {fc.silence_recovery_probability:.2f} per cycle")

    # Scenarios
    print()
    print(coloured("  CONFIGURED SCENARIOS", C.BOLD, C.WHITE))
    for s in cfg.scenarios.scenarios:
        zones_str = (
            ", ".join(s["affected_zones"])
            if isinstance(s["affected_zones"], list)
            else s["affected_zones"]
        )
        print(
            f"  {coloured(s['name'], C.CYAN):<28}"
            f" trigger={s['trigger_hour']:02d}:00"
            f"  duration={s['duration_minutes']}min"
            f"  zones={zones_str}"
        )

    print()
    print(coloured("=" * 65, C.BOLD, C.CYAN))
    if demo:
        print(coloured(f"  Applying demo '{demo}' before first events...", C.ORANGE))
    print(coloured("  Streaming events — Ctrl+C to stop", C.DIM))
    print(coloured("=" * 65, C.BOLD, C.CYAN))
    print()


# ---------------------------------------------------------------------------
# Event printer
# ---------------------------------------------------------------------------

STATE_COLOURS = {
    "normal":   C.GREEN,
    "degraded": C.YELLOW,
    "fault":    C.RED,
    "silent":   C.DIM,
}
TYPE_COLOURS = {
    RESIDENTIAL: C.GREEN,
    COMMERCIAL:  C.YELLOW,
    INDUSTRIAL:  C.MAGENTA,
}

def print_event(event: MeterEvent, sim_time: datetime) -> None:
    state_col   = STATE_COLOURS.get(event.meter_state, C.RESET)
    type_col    = TYPE_COLOURS.get(event.customer_type, C.RESET)
    anomaly_str = coloured(" ⚠ ANOMALY", C.BOLD, C.YELLOW) if event.is_anomaly else ""
    alert_str   = coloured(" 🔴 ALERT",  C.BOLD, C.RED)    if event.event_type == "alert" else ""
    time_str    = sim_time.strftime("%H:%M:%S")
    print(
        f"  {coloured(time_str, C.DIM)}"
        f"  {event.meter_id:<10}"
        f"  {event.zone_id:<14}"
        f"  {coloured(f'{event.customer_type:<12}', type_col)}"
        f"  Δ{event.kwh_delta:>6.4f}kWh"
        f"  {event.power_kw:>6.2f}kW"
        f"  {event.voltage:>6.1f}V"
        f"  PF:{event.power_factor:.2f}"
        f"  {coloured(f'[{event.meter_state.upper()}]', state_col)}"
        f"{anomaly_str}{alert_str}"
    )

def print_silent_meter(meter_id: str, zone_id: str, sim_time: datetime) -> None:
    time_str = sim_time.strftime("%H:%M:%S")
    print(
        f"  {coloured(time_str, C.DIM)}"
        f"  {meter_id:<10}"
        f"  {zone_id:<14}"
        f"  {coloured('-- SILENT --  no event transmitted', C.DIM)}"
    )


# ---------------------------------------------------------------------------
# Stats ticker
# ---------------------------------------------------------------------------

class StatsTicker:
    def __init__(self, print_every: int = 50):
        self.print_every   = print_every
        self.event_count   = 0
        self.anomaly_count = 0
        self.alert_count   = 0
        self.silent_count  = 0
        self.state_counts  = defaultdict(int)
        self.zone_kwh      = defaultdict(float)

    def record(self, event: MeterEvent) -> None:
        self.event_count += 1
        self.state_counts[event.meter_state] += 1
        self.zone_kwh[event.zone_id]         += event.kwh_delta
        if event.is_anomaly:            self.anomaly_count += 1
        if event.event_type == "alert": self.alert_count   += 1

    def record_silent(self) -> None:
        self.silent_count += 1

    def maybe_print(self, clock: SimulationClock) -> None:
        if self.event_count > 0 and self.event_count % self.print_every == 0:
            self._print(clock)

    def _print(self, clock: SimulationClock) -> None:
        sim_time = clock.now().strftime("%Y-%m-%d %H:%M:%S")
        print()
        print(coloured(
            f"  ── FLEET STATUS  sim_time={sim_time}  events={self.event_count} ──",
            C.BOLD, C.CYAN
        ))
        total = self.event_count
        for state, count in sorted(self.state_counts.items()):
            col = STATE_COLOURS.get(state, C.RESET)
            bar = "█" * int((count / total) * 30)
            print(f"  {coloured(state.upper()+':', col):<22}  {bar:<30}  {count:>5} ({count/total*100:.1f}%)")
        print()
        print(f"  {'Zone':<16}  {'kWh consumed':>16}")
        for zone, kwh in sorted(self.zone_kwh.items()):
            print(f"  {zone:<16}  {kwh:>14.3f} kWh")
        print()
        print(f"  Anomalous readings : {coloured(str(self.anomaly_count), C.BOLD, C.YELLOW)}")
        print(f"  Alert events       : {coloured(str(self.alert_count),   C.BOLD, C.RED)}")
        print(f"  Silent cycles      : {coloured(str(self.silent_count),  C.DIM)}")
        print(coloured("  " + "─" * 62, C.DIM))
        print()


# ---------------------------------------------------------------------------
# Per-meter coroutine
# ---------------------------------------------------------------------------

async def dry_run_meter(
    state_machine: MeterStateMachine,
    clock: SimulationClock,
    ticker: StatsTicker,
    jitter_range: float,
    verbose: bool,
) -> None:
    while True:
        event = state_machine.generate_event()
        if event is None:
            if verbose:
                print_silent_meter(
                    state_machine.profile.meter_id,
                    state_machine.profile.zone_id,
                    clock.now(),
                )
            ticker.record_silent()
        else:
            if verbose:
                print_event(event, clock.now())
            ticker.record(event)

        ticker.maybe_print(clock)
        jitter        = random.uniform(0, jitter_range)
        sleep_seconds = state_machine.profile.interval_seconds + jitter
        await asyncio.sleep(sleep_seconds)


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

async def run_dry_run(cfg: GridoscopeConfig, verbose: bool, demo: str = None) -> None:

    fleet = generate_meter_fleet(cfg)
    print_fleet_summary(fleet, cfg, demo=demo)

    clock    = SimulationClock(speed_multiplier=cfg.simulation.speed_multiplier)
    demo_rng = random.Random(cfg.simulation.random_seed)

    state_machines = []
    for i, profile in enumerate(fleet):
        meter_rng = random.Random(
            None if cfg.simulation.random_seed is None
            else cfg.simulation.random_seed + i
        )
        sm = MeterStateMachine(profile=profile, cfg=cfg, clock=clock, rng=meter_rng)
        state_machines.append(sm)

    meters_by_zone: dict = {zone: [] for zone in cfg.simulation.zones}
    for sm in state_machines:
        meters_by_zone[sm.profile.zone_id].append(sm)

    # Apply demo injections BEFORE the event loop starts.
    # The very first events you see will already reflect the injected state.
    if demo:
        apply_demo_mode(demo, state_machines, meters_by_zone, cfg, demo_rng)

    ticker          = StatsTicker(print_every=50)
    scenario_engine = ScenarioEngine(cfg=cfg, clock=clock, meters_by_zone=meters_by_zone)

    if verbose:
        print(
            f"  {'TIME':<10}"
            f"  {'METER':<10}"
            f"  {'ZONE':<14}"
            f"  {'TYPE':<12}"
            f"  {'kWh DELTA':>10}"
            f"  {'POWER':>8}"
            f"  {'VOLTAGE':>8}"
            f"  {'PF':>6}"
            f"  STATE"
        )
        print(f"  {'-'*105}")

    async def staggered_meter(sm, i):
        stagger = (i / len(state_machines)) * sm.profile.interval_seconds
        await asyncio.sleep(stagger)
        await dry_run_meter(
            state_machine=sm,
            clock=clock,
            ticker=ticker,
            jitter_range=cfg.simulation.interval_jitter_seconds / cfg.simulation.speed_multiplier,
            verbose=verbose,
        )

    tasks = [staggered_meter(sm, i) for i, sm in enumerate(state_machines)]
    tasks.append(scenario_engine.run())

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Gridoscope dry run — simulate meters without Kafka",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python dry_run.py                              # 10 meters, real time
  python dry_run.py --meters 20 --speed 10       # faster run
  python dry_run.py --quiet                      # stats only
  python dry_run.py --seed 42                    # reproducible
  python dry_run.py --demo list                  # show all demo options
  python dry_run.py --demo degraded              # some meters → DEGRADED
  python dry_run.py --demo fault                 # some meters → FAULT
  python dry_run.py --demo silent                # some meters → SILENT
  python dry_run.py --demo anomaly               # force anomalous readings
  python dry_run.py --demo heatwave              # heatwave scenario
  python dry_run.py --demo evening_peak          # evening peak scenario
  python dry_run.py --demo zone_outage           # take down ZONE-SOUTH
  python dry_run.py --demo all                   # everything at once
  python dry_run.py --demo all --meters 20 --speed 5 --seed 42
        """,
    )
    parser.add_argument("--meters", type=int,   default=10,   help="Number of meters (default: 10)")
    parser.add_argument("--speed",  type=float, default=1.0,  help="Speed multiplier (default: 1.0)")
    parser.add_argument("--seed",   type=int,   default=None, help="Random seed for reproducible runs")
    parser.add_argument("--quiet",  action="store_true",      help="Suppress per-event output")
    parser.add_argument(
        "--demo",
        type=str,
        default=None,
        metavar="MODE",
        help=(
            "Force a specific condition immediately. "
            "Pass 'list' to see all options."
        ),
    )
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    args = parse_args()

    if args.demo == "list":
        print_demo_menu()
        sys.exit(0)

    if args.demo and args.demo not in DEMO_MODES:
        print(coloured(f"\n  Unknown demo mode: '{args.demo}'", C.RED))
        print_demo_menu()
        sys.exit(1)

    cfg = GridoscopeConfig(
        simulation=SimulationConfig(
            total_meters=args.meters,
            speed_multiplier=args.speed,
            random_seed=args.seed,
        ),
        kafka=KafkaConfig(bootstrap_servers="localhost:9092"),
    )

    try:
        asyncio.run(run_dry_run(cfg, verbose=not args.quiet, demo=args.demo))
    except KeyboardInterrupt:
        print()
        print(coloured("  Dry run stopped.", C.DIM))
        print()