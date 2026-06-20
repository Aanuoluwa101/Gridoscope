"""
powerbi_sink.py — Pushes ZoneAggregate objects to a Power BI streaming dataset.

Power BI streaming datasets accept JSON via a simple HTTP POST to a push URL.
No SDK needed — it's just a REST call.

How to get the push URL:
    1. Open Power BI (app.powerbi.com)
    2. Create a new Dashboard
    3. Add tile → Streaming data → Add streaming dataset
    4. Choose "API" as the source
    5. Define the fields (they must match the JSON keys we send — see SCHEMA below)
    6. Copy the "Push URL" into ConsumerConfig → powerbi → push_url

Power BI streaming datasets have a rate limit of ~10 pushes/second per dataset.
At one push per zone per 5-minute window that's nowhere near the limit.

When powerbi.enabled = False (the default during development):
    The sink prints the aggregate to the terminal instead of pushing.
    This lets you validate the aggregation logic without needing a
    Power BI workspace set up.
"""

import json
import logging
import asyncio
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Optional

import aiohttp

from aggregator import ZoneAggregate
from config import PowerBIConfig

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Power BI field schema
#
# These field names must match exactly what you define when creating the
# streaming dataset in Power BI. If they don't match, the push succeeds
# but the data doesn't appear on tiles.
#
# To create the dataset, paste this JSON into Power BI's schema editor:
#
# {
#   "name": "Gridoscope Meter Readings",
#   "fields": [
#     {"name": "zone_id",                    "type": "text"},
#     {"name": "window_start_ts",            "type": "datetime"},
#     {"name": "window_end_ts",              "type": "datetime"},
#     {"name": "total_kwh",                  "type": "number"},
#     {"name": "avg_power_kw",               "type": "number"},
#     {"name": "peak_power_kw",              "type": "number"},
#     {"name": "avg_power_factor",           "type": "number"},
#     {"name": "avg_voltage",                "type": "number"},
#     {"name": "avg_frequency_hz",           "type": "number"},
#     {"name": "active_meter_count",         "type": "number"},
#     {"name": "silent_meter_count",         "type": "number"},
#     {"name": "degraded_count",             "type": "number"},
#     {"name": "fault_count",                "type": "number"},
#     {"name": "anomaly_count",              "type": "number"},
#     {"name": "demand_vs_prev_window_pct",  "type": "number"}
#   ]
# }
# ---------------------------------------------------------------------------


class PowerBISink:
    """
    Async sink that pushes ZoneAggregate objects to Power BI.

    One instance is shared across all zone consumers in the process.
    Uses a single aiohttp session for connection pooling — more efficient
    than opening a new HTTP connection for every push.

    Usage:
        async with PowerBISink(cfg) as sink:
            await sink.push(aggregate)
    """

    def __init__(self, cfg: PowerBIConfig):
        self.cfg      = cfg
        self._session: Optional[aiohttp.ClientSession] = None
        self._push_count  = 0
        self._error_count = 0
        self._lock        = asyncio.Lock()
        self._last_push   = 0.0

    async def __aenter__(self):
        if self.cfg.enabled:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.cfg.request_timeout_seconds)
            )
            logger.info("[PowerBI] Sink initialised. Push URL configured.")
        else:
            logger.info("[PowerBI] Sink running in DRY mode — aggregates printed to terminal.")
        return self

    async def __aexit__(self, *args):
        if self._session:
            await self._session.close()
        logger.info(
            "[PowerBI] Sink closed. pushed=%d errors=%d",
            self._push_count, self._error_count,
        )

    async def push(self, aggregate: ZoneAggregate) -> None:
        """
        Push one ZoneAggregate to Power BI (or print it if disabled).

        Power BI expects a JSON array — even for a single row.
        """
        if not self.cfg.enabled:
            # print("\n POWERBI NOT ENABLED\n")
            self._print_aggregate(aggregate)
            return

        if not self.cfg.push_url:
            logger.warning("[PowerBI] push_url not configured. Set it in ConsumerConfig.")
            return

        payload = [self._to_powerbi_row(aggregate)]

        async with self._lock:
            gap = self.cfg.min_push_interval_seconds - (asyncio.get_event_loop().time() - self._last_push)
            if gap > 0:
                await asyncio.sleep(gap)
            self._last_push = asyncio.get_event_loop().time()

        try:
            async with self._session.post(
                self.cfg.push_url,
                json=payload,
            ) as response:
                if response.status == 200:
                    self._push_count += 1
                    logger.debug(
                        "[PowerBI] Pushed aggregate for %s | window=%s",
                        aggregate.zone_id, aggregate.window_end_ts
                    )
                    self._print_aggregate(aggregate)
                else:
                    body = await response.text()
                    self._error_count += 1
                    logger.warning(
                        "[PowerBI] Push failed for %s | status=%d | body=%s",
                        aggregate.zone_id, response.status, body[:200]
                    )

        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            self._error_count += 1
            logger.warning("[PowerBI] HTTP error for %s: %s", aggregate.zone_id, exc)

    # -----------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------

    @staticmethod
    def _to_powerbi_row(agg: ZoneAggregate) -> dict:
        """
        Convert a ZoneAggregate into the flat dict Power BI expects.
        None values are replaced with 0.0 — Power BI doesn't accept nulls
        in numeric streaming dataset fields.
        """
        row = asdict(agg)
        # Replace None with 0.0 for numeric fields
        if row["demand_vs_prev_window_pct"] is None:
            row["demand_vs_prev_window_pct"] = 0.0
        return row

    @staticmethod
    def _print_aggregate(agg: ZoneAggregate) -> None:
        """
        Pretty-print a ZoneAggregate to terminal when Power BI is disabled.
        This is what you see during development to validate the output.
        """
        delta_str = (
            f"{agg.demand_vs_prev_window_pct:+.1f}% vs prev"
            if agg.demand_vs_prev_window_pct is not None
            else "first window"
        )

        # Colour-code based on fault/anomaly counts
        status = "OK"
        if agg.fault_count > 0:    status = "⚠ FAULTS"
        if agg.silent_meter_count > 3: status = "⚠ SILENT METERS"

        print(
            f"\n  ┌─ Window closed: {agg.zone_id} ─────────────────────────────\n"
            f"  │  Period    : {agg.window_start_ts}  →  {agg.window_end_ts}\n"
            f"  │  Demand    : {agg.total_kwh:.3f} kWh total  |  "
            f"avg {agg.avg_power_kw:.2f} kW  |  peak {agg.peak_power_kw:.2f} kW\n"
            f"  │  Delta     : {delta_str}\n"
            f"  │  Grid      : {agg.avg_voltage:.1f}V  |  PF {agg.avg_power_factor:.3f}  "
            f"|  {agg.avg_frequency_hz:.3f} Hz\n"
            f"  │  Meters    : {agg.active_meter_count} active  |  "
            f"{agg.silent_meter_count} silent  |  "
            f"{agg.degraded_count} degraded  |  {agg.fault_count} fault\n"
            f"  │  Anomalies : {agg.anomaly_count}\n"
            f"  │  Status    : {status}\n"
            f"  └──────────────────────────────────────────────────────────"
        )
