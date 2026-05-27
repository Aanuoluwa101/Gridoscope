"""
scenario_engine.py — Injects predefined events into the simulation at scheduled times.

The ScenarioEngine runs as a background coroutine alongside the meter coroutines.
It watches the simulation clock and activates/deactivates scenarios based on
their configured trigger_hour and duration_minutes.

Why have a scenario engine?
  Pure random behaviour is good for baseline realism, but a good demo and a
  good portfolio project needs *interesting moments* — a heatwave that drives
  up fault rates, an evening peak that lights up the demand gauges, a zone
  outage that cascades into silent meter alerts. The scenario engine creates
  these moments deliberately, on a schedule.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional

from meter_state_machine import MeterStateMachine, SimulationClock
from config import GridoscopeConfig

logger = logging.getLogger(__name__)


class ScenarioEngine:
    """
    Monitors the simulation clock and applies scenario effects to meters.

    The engine keeps a registry of which scenarios are currently active
    and which have already fired. Each scenario fires once per simulated day
    (when the hour matches trigger_hour) and expires after duration_minutes.
    """

    def __init__(
        self,
        cfg: GridoscopeConfig,
        clock: SimulationClock,
        meters_by_zone: dict[str, list[MeterStateMachine]],
    ):
        """
        Parameters
        ----------
        cfg             : full Gridoscope config
        clock           : shared simulation clock
        meters_by_zone  : dict mapping zone_id → list of MeterStateMachine instances
                          The engine uses this to target specific zones when
                          applying effects.
        """
        self.cfg            = cfg
        self.clock          = clock
        self.meters_by_zone = meters_by_zone

        # Track which scenarios are currently active and when they expire.
        # Key: scenario name, Value: simulated datetime when effect should end.
        self._active_scenarios: dict[str, datetime] = {}

        # Track which scenarios have already fired today (by simulated date)
        # so we don't re-trigger them every time the hour matches.
        self._fired_today: set[str] = set()
        self._last_simulated_date: Optional[datetime.date] = None

    async def run(self) -> None:
        """
        Main loop — runs as an asyncio coroutine for the life of the simulation.

        Checks the clock every 5 real seconds (adjusted for speed_multiplier)
        to see if any scenarios should start or stop. This doesn't need to be
        on the meter interval — scenario transitions are coarse-grained.
        """
        # Check interval: every 5 wall-clock seconds
        check_interval = 5.0

        logger.info("[ScenarioEngine] Started. %d scenarios loaded.",
                    len(self.cfg.scenarios.scenarios))

        while True:
            sim_now  = self.clock.now()
            sim_date = sim_now.date()

            # Reset fired_today at midnight of each simulated day
            if self._last_simulated_date != sim_date:
                self._fired_today.clear()
                self._last_simulated_date = sim_date
                logger.info("[ScenarioEngine] New simulated day: %s", sim_date)

            # Check each configured scenario
            for scenario in self.cfg.scenarios.scenarios:
                name = scenario["name"]

                # Should this scenario activate this hour?
                if sim_now.hour == scenario["trigger_hour"] and name not in self._fired_today:
                    self._activate_scenario(scenario, sim_now)
                    self._fired_today.add(name)

            # Expire scenarios that have run their duration
            expired = [
                name for name, expires_at in self._active_scenarios.items()
                if sim_now >= expires_at
            ]
            for name in expired:
                self._deactivate_scenario(name)

            await asyncio.sleep(check_interval)

    # -----------------------------------------------------------------------
    # Scenario activation
    # -----------------------------------------------------------------------

    def _activate_scenario(self, scenario: dict, sim_now: datetime) -> None:
        """Apply a scenario's effects to the target meters."""
        name             = scenario["name"]
        duration_minutes = scenario.get("duration_minutes", 60)
        expires_at       = sim_now + timedelta(
            minutes=duration_minutes / self.cfg.simulation.speed_multiplier
        )
        effect           = scenario["effect"]
        affected_zones   = scenario["affected_zones"]

        # Resolve "ALL" to every zone
        if affected_zones == ["ALL"] or affected_zones == "ALL":
            affected_zones = self.cfg.simulation.zones

        target_meters = self._get_meters_for_zones(affected_zones)

        logger.info(
            "[ScenarioEngine] Activating scenario '%s' | effect=%s | zones=%s | "
            "duration=%dmin | meters_affected=%d",
            name, effect, affected_zones, duration_minutes, len(target_meters)
        )

        if effect == "demand_spike":
            multiplier = scenario.get("demand_multiplier", 1.5)
            for meter in target_meters:
                meter.apply_scenario(demand_multiplier=multiplier)

        elif effect == "demand_spike_plus_faults":
            multiplier        = scenario.get("demand_multiplier", 1.5)
            fault_rate_boost  = scenario.get("fault_rate_increase", 2.0)
            for meter in target_meters:
                meter.apply_scenario(
                    demand_multiplier=multiplier,
                    fault_rate_multiplier=fault_rate_boost,
                )

        elif effect == "mass_silence":
            # Only silence a percentage of meters in the affected zones
            pct = scenario.get("pct_meters_affected", 0.7)
            import random
            silenced_count = 0
            for meter in target_meters:
                if random.random() < pct:
                    meter.apply_scenario(force_silent=True)
                    silenced_count += 1
            logger.info(
                "[ScenarioEngine] Zone outage: %d meters forced SILENT in %s",
                silenced_count, affected_zones
            )

        self._active_scenarios[name] = expires_at

    def _deactivate_scenario(self, name: str) -> None:
        """Remove scenario effects and return all meters to normal behaviour."""
        logger.info("[ScenarioEngine] Deactivating scenario '%s'", name)

        # Clear effects on all meters — they'll resume profile-driven behaviour
        for meters in self.meters_by_zone.values():
            for meter in meters:
                meter.clear_scenario()

        del self._active_scenarios[name]

    # -----------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------

    def _get_meters_for_zones(self, zone_ids: list[str]) -> list[MeterStateMachine]:
        """Flatten the meters from multiple zones into a single list."""
        result = []
        for zone_id in zone_ids:
            result.extend(self.meters_by_zone.get(zone_id, []))
        return result
