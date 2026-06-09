"""
runner.py — Entry point for the meter.readings consumer group.

Spawns 5 ZoneConsumer coroutines (one per zone/partition) and runs them
concurrently using asyncio.gather(). All 5 share a single PowerBISink
instance — one HTTP session, one connection pool.

Usage:
    python consumers/runner.py

    # Or with Power BI enabled:
    POWERBI_PUSH_URL="https://api.powerbi.com/beta/.../datasets/.../rows" \
    python consumers/runner.py
"""

import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from consumers.config import (
    ConsumerConfig, KafkaConsumerConfig,
    AggregationConfig, PowerBIConfig, ZONE_PARTITION,
)
from consumer import ZoneConsumer
from powerbi_sink import PowerBISink

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Meter count per zone
# Needed by ZoneAggregator to detect silent meters.
# Must match the fleet distribution from producer/config/settings.py.
# In production you'd fetch this from a metadata store rather than
# hardcoding it — but for this project keeping it here is fine.
# ---------------------------------------------------------------------------
METERS_PER_ZONE: dict[str, int] = {
    "ZONE-NORTH":   1,
    "ZONE-SOUTH":   1,
    "ZONE-EAST":    1,
    "ZONE-WEST":    1,
    "ZONE-CENTRAL": 1,
}


async def run_consumer_group(cfg: ConsumerConfig) -> None:
    """
    Start all 5 zone consumers and run them until interrupted.

    Each consumer is a coroutine. asyncio.gather() runs them all
    concurrently on one event loop — same pattern as the producer engine.
    """
    logger.info("=" * 60)
    logger.info("Gridoscope — meter.readings consumer group starting")
    logger.info("  Group ID       : %s", cfg.kafka.readings_group_id)
    logger.info("  Broker         : %s", cfg.kafka.bootstrap_servers)
    logger.info("  Topic          : %s", cfg.kafka.readings_topic)
    logger.info("  Zones          : %d", len(ZONE_PARTITION))
    logger.info("  Speed mult     : %.1f×", cfg.aggregation.speed_multiplier)
    logger.info("  Window size    : %ds simulated (%.1fs wall-clock)",
                cfg.aggregation.window_size_seconds,
                cfg.aggregation.window_size_seconds / cfg.aggregation.speed_multiplier)
    logger.info("  Power BI       : %s", "enabled" if cfg.powerbi.enabled else "dry mode (terminal)")
    logger.info("=" * 60)

    async with PowerBISink(cfg.powerbi) as sink:

        # Create one consumer per partition
        consumers = [
            ZoneConsumer(
                partition=partition,
                cfg=cfg,
                sink=sink,
                meter_count=METERS_PER_ZONE.get(zone_id, 100),
            )
            for zone_id, partition in ZONE_PARTITION.items()
        ]

        # Start all consumers (connects to Kafka, assigns partitions)
        for c in consumers:
            await c.start()

        logger.info("All %d zone consumers connected and running.", len(consumers))
        logger.info("Waiting for messages... (Ctrl+C to stop)")

        # Run all consumer loops concurrently
        # return_exceptions=True means one failing consumer doesn't
        # immediately cancel the others — they keep running.
        try:
            await asyncio.gather(
                *[c.run() for c in consumers],
                return_exceptions=True,
            )
        except asyncio.CancelledError:
            logger.info("Consumer group cancelled.")
        finally:
            # Graceful shutdown — stop all consumers in parallel
            logger.info("Shutting down consumers...")
            await asyncio.gather(*[c.stop() for c in consumers])
            logger.info("All consumers stopped.")


def main():
    """
    Entry point. Customise config here before running.

    To enable Power BI: set powerbi.enabled=True and provide push_url.
    You can also read the push URL from an environment variable:

        push_url=os.environ.get("POWERBI_PUSH_URL", "")
    """
    cfg = ConsumerConfig(
        kafka=KafkaConsumerConfig(
            bootstrap_servers="localhost:9092",
            auto_offset_reset="latest",   # change to "latest" in production
        ),
        aggregation=AggregationConfig(
            # Must match the speed_multiplier in producer/engine.py.
            # This is the only value you need to change when switching speeds.
            # Everything time-based scales automatically from this one number.
            #
            # speed_multiplier=1.0  → real time, windows close every 5 real minutes
            # speed_multiplier=10.0 → 10× faster, windows close every 30 real seconds
            # speed_multiplier=60.0 → 60× faster, windows close every 5 real seconds
            speed_multiplier=100,            # ← match producer's speed_multiplier

            window_size_seconds=300,         # 5 simulated minutes per window
            silence_threshold_seconds=120,   # 2 simulated minutes before flagging silence
            anomaly_spike_threshold=4.0,
            ema_alpha=0.2,
        ),
        powerbi=PowerBIConfig(
            enabled=False,                   # flip to True with a push_url when ready
            push_url=os.environ.get("POWERBI_PUSH_URL", "https://api.powerbi.com/beta/8c22ea0a-63a5-44e1-bb22-e44db5cc72e3/datasets/64c0422a-b30d-4cd2-9b2b-8b24cbb682ca/rows?experience=power-bi&key=5iiD9fYub39Lk7HmFs8gh5Bl8S4NKjoZjOH20SuEMMxHl7QdHf2g79KKypKsOaDSwx0iW%2BfHK5PtG5ieV1inMg%3D%3D"),
        ),
    )

    try:
        asyncio.run(run_consumer_group(cfg))
    except KeyboardInterrupt:
        logger.info("Stopped by user.")


if __name__ == "__main__":
    main()
