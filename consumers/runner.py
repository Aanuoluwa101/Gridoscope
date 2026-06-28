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
# Must match the fleet distribution the producer generates — producer assigns
# zones round-robin (meter_index % n_zones), so the count just needs to match
# TOTAL_METERS split evenly across zones, with the remainder going to the
# first few zones in list order.
# In production you'd fetch this from a metadata store rather than
# computing it — but for this project keeping it here is fine.
# ---------------------------------------------------------------------------
def _meters_per_zone(total_meters: int) -> dict[str, int]:
    zones = list(ZONE_PARTITION.keys())
    base, remainder = divmod(total_meters, len(zones))
    return {
        zone: base + (1 if i < remainder else 0)
        for i, zone in enumerate(zones)
    }


METERS_PER_ZONE: dict[str, int] = _meters_per_zone(int(os.environ.get("TOTAL_METERS", "5")))


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
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            security_protocol=os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
            aws_region=os.environ.get("AWS_REGION", "us-east-1"),
            auto_offset_reset="latest",
        ),
        aggregation=AggregationConfig(
            # Must match the speed_multiplier in producer/engine.py.
            # This is the only value you need to change when switching speeds.
            # Everything time-based scales automatically from this one number.
            #
            # speed_multiplier=1.0  → real time, windows close every 5 real minutes
            # speed_multiplier=10.0 → 10× faster, windows close every 30 real seconds
            # speed_multiplier=60.0 → 60× faster, windows close every 5 real seconds 
            speed_multiplier=float(os.environ.get("SPEED_MULTIPLIER", "100")),  # ← match producer's speed_multiplier

            window_size_seconds=300,         # 5 simulated minutes per window
            silence_threshold_seconds=120,   # 2 simulated minutes before flagging silence
            anomaly_spike_threshold=4.0,
            ema_alpha=0.2,
        ),
        powerbi=PowerBIConfig(
            enabled=os.environ.get("POWERBI_ENABLED", "false").lower() == "true",
            push_url=os.environ.get("POWERBI_PUSH_URL", ""),
        ),
    )

    try:
        asyncio.run(run_consumer_group(cfg))
    except KeyboardInterrupt:
        logger.info("Stopped by user.")


if __name__ == "__main__":
    main()
