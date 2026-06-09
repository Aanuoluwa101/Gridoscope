"""
engine.py — The simulation engine. Wires all components together and runs them.

This is the top-level coordinator. It:
1. Loads config
2. Generates the meter fleet
3. Creates a MeterStateMachine for each meter
4. Starts the ScenarioEngine as a background task
5. Runs all 500 meter coroutines concurrently via asyncio.gather()

Each meter coroutine is a simple loop:
  wait for interval → generate event → send to Kafka → repeat

The asyncio event loop multiplexes all 500 coroutines on a single thread.
While one coroutine is awaiting its sleep interval, others run. While one
is awaiting the Kafka send, others run. This is how we achieve 500-meter
concurrency without 500 threads.
"""

import asyncio
import logging
import random
import sys
from datetime import datetime

from config import GridoscopeConfig, ScenarioConfig
from meter_profile import generate_meter_fleet, MeterProfile
from meter_state_machine import MeterStateMachine, SimulationClock
from scenario_engine import ScenarioEngine
from kafka_producer import GridoscopeProducer

# Configure logging — structured enough to be useful, not so verbose it scrolls forever
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Per-meter coroutine
# ---------------------------------------------------------------------------

async def run_meter(
    profile: MeterProfile,
    state_machine: MeterStateMachine,
    producer: GridoscopeProducer,
    jitter_range: float,
) -> None:
    """
    The async coroutine that drives a single meter for the life of the simulation.

    This is intentionally simple — the complexity lives in MeterStateMachine.
    This function just drives the clock: wait, generate, send, repeat.

    Parameters
    ----------
    profile       : static meter identity (for interval lookup)
    state_machine : the meter's runtime state and event generator
    producer      : shared Kafka producer (one instance, many coroutines)
    jitter_range  : max seconds of random jitter to add to each sleep
                    prevents all 500 meters firing simultaneously
    """
    while True:
        # Generate the next event (returns None if meter is SILENT)
        event = state_machine.generate_event()

        if event is not None:
            await producer.send_event(event)

        # Sleep for this meter's interval plus a random jitter.
        # asyncio.sleep() suspends THIS coroutine but lets others run —
        # this is the key to concurrent execution without threads.
        jitter       = random.uniform(0, jitter_range)
        sleep_seconds = profile.interval_seconds + jitter
        await asyncio.sleep(sleep_seconds)


# ---------------------------------------------------------------------------
# Main simulation runner
# ---------------------------------------------------------------------------

async def run_simulation(cfg: GridoscopeConfig) -> None:
    """
    Start and run the full simulation.

    Sets up all components, then hands control to asyncio.gather() which
    runs all meter coroutines (plus the scenario engine) concurrently until
    interrupted (Ctrl+C) or an unhandled exception occurs.
    """
    logger.info("=" * 60)
    logger.info("Gridoscope Producer Engine starting")
    logger.info("  Meters         : %d", cfg.simulation.total_meters)
    logger.info("  Speed          : %.1f×", cfg.simulation.speed_multiplier)
    logger.info("  Kafka broker   : %s", cfg.kafka.bootstrap_servers)
    logger.info("  Random seed    : %s", cfg.simulation.random_seed)
    logger.info("=" * 60)

    # Seed global random for anything not using the per-fleet RNG
    if cfg.simulation.random_seed is not None:
        random.seed(cfg.simulation.random_seed)

    # Step 1: Generate the full meter fleet (500 profiles)
    fleet = generate_meter_fleet(cfg)

    # Step 2: Create a shared simulation clock
    # All meters and the scenario engine share this single clock instance
    # so everyone agrees on what time it is.
    clock = SimulationClock(
        speed_multiplier=cfg.simulation.speed_multiplier,
        sim_start=cfg.simulation.sim_start, 
        )

    # Step 3: Create a MeterStateMachine for each profile
    # We use a separate RNG per state machine (seeded from the profile index)
    # so meters behave independently — one meter's random rolls don't affect another's.
    state_machines: list[MeterStateMachine] = []
    for i, profile in enumerate(fleet):
        meter_rng = random.Random(
            None if cfg.simulation.random_seed is None
            else cfg.simulation.random_seed + i  # unique seed per meter
        )
        sm = MeterStateMachine(
            profile=profile,
            cfg=cfg,
            clock=clock,
            rng=meter_rng,
        )
        state_machines.append(sm)

    # Step 4: Build the zone → state_machines mapping for the ScenarioEngine
    meters_by_zone: dict[str, list[MeterStateMachine]] = {
        zone: [] for zone in cfg.simulation.zones
    }
    for sm in state_machines:
        meters_by_zone[sm.profile.zone_id].append(sm)

    logger.info("[Engine] Zone distribution:")
    for zone, meters in meters_by_zone.items():
        logger.info("  %s: %d meters", zone, len(meters))

    # Step 5: Start the Kafka producer and run everything
    async with GridoscopeProducer(cfg.kafka) as producer:

        # Scenario engine runs as a background coroutine
        scenario_engine = ScenarioEngine(
            cfg=cfg,
            clock=clock,
            meters_by_zone=meters_by_zone,
        )

        # Build the list of all coroutines to run concurrently:
        # - One coroutine per meter (500 total)
        # - One scenario engine coroutine
        meter_tasks = [
            run_meter(
                profile=sm.profile,
                state_machine=sm,
                producer=producer,
                jitter_range=cfg.simulation.interval_jitter_seconds / cfg.simulation.speed_multiplier,
            )
            for sm in state_machines
        ]
        scenario_task = scenario_engine.run()

        all_tasks = meter_tasks + [scenario_task]

        logger.info("[Engine] Launching %d meter coroutines + scenario engine...", len(meter_tasks))

        # asyncio.gather() starts all coroutines and runs them concurrently.
        # They all share the same event loop — cooperative multitasking.
        # The simulation runs until Ctrl+C or an exception propagates.
        try:
            await asyncio.gather(*all_tasks)
        except asyncio.CancelledError:
            logger.info("[Engine] Simulation cancelled. Shutting down.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    """
    CLI entry point. Modify GridoscopeConfig() here to customise the run.

    Examples:

    # Full-scale real-time run (production-like):
    cfg = GridoscopeConfig()

    # Fast development run (10× speed, 50 meters, fixed seed):
    cfg = GridoscopeConfig(
        simulation=SimulationConfig(
            speed_multiplier=10.0,
            total_meters=50,
            random_seed=42,
        )
    )

    # Point at a real MSK cluster:
    cfg = GridoscopeConfig(
        kafka=KafkaConfig(bootstrap_servers="your-msk-endpoint:9092")
    )
    """
    from config import SimulationConfig, KafkaConfig

    cfg = GridoscopeConfig(
        simulation=SimulationConfig(
            total_meters=5,
            speed_multiplier=100,    # ← change to 10.0 for fast dev runs
            random_seed=42,         # ← set to an int for reproducible runs
            sim_start=datetime(2026, 6, 6, 17, 0, 0),
        ),
        kafka=KafkaConfig(
            bootstrap_servers="localhost:9092",
        ),
        scenarios=ScenarioConfig(scenarios=[]),
    )

    try:
        asyncio.run(run_simulation(cfg))
    except KeyboardInterrupt:
        logger.info("[Engine] Stopped by user.")


if __name__ == "__main__":
    main()
