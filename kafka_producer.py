"""
kafka_producer.py — Async Kafka producer wrapper for GridPulse.

Wraps aiokafka's AIOKafkaProducer with:
- Connection lifecycle management (start / stop)
- Serialisation of MeterEvent objects to JSON bytes
- Separate routing for readings vs alert events
- Basic metrics logging so you can see throughput in the terminal

Why aiokafka?
  The standard kafka-python library is synchronous — each send() call blocks
  the thread until the broker acknowledges. With 500 concurrent meters that
  would require 500 threads, which is expensive and doesn't scale.

  aiokafka is the async equivalent. send() is a coroutine — it suspends the
  current coroutine while waiting for the broker, allowing other meter
  coroutines to run in the meantime. This is how we run 500 meters
  concurrently in a single process without threads.
"""

import asyncio
import logging
import time
from typing import Optional

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from producers.models.meter_profile import MeterProfile
from producers.simulation.meter_state_machine import MeterEvent
from producers.config.settings import KafkaConfig

logger = logging.getLogger(__name__)


class GridPulseProducer:
    """
    Async Kafka producer with connection lifecycle and routing logic.

    Usage:
        async with GridPulseProducer(cfg) as producer:
            await producer.send_event(event)
    """

    def __init__(self, cfg: KafkaConfig):
        self.cfg      = cfg
        self._producer: Optional[AIOKafkaProducer] = None

        # Simple throughput counters for terminal logging
        self._events_sent   = 0
        self._alerts_sent   = 0
        self._send_errors   = 0
        self._last_log_time = time.monotonic()
        self._log_interval  = 30.0   # log throughput stats every 30 seconds

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *args):
        await self.stop()

    async def start(self) -> None:
        """
        Create and start the Kafka producer connection.

        Called once at simulation startup. The producer maintains a
        persistent connection to the broker — we don't reconnect per message.
        """
        logger.info("[Producer] Connecting to Kafka at %s", self.cfg.bootstrap_servers)

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.cfg.bootstrap_servers,
            # How long to buffer messages before sending a batch.
            # linger_ms > 0 means we wait a little to build up a batch,
            # which is more efficient than sending one message at a time.
            linger_ms=self.cfg.linger_ms,
            # Maximum size of one batch in bytes.
            batch_size=self.cfg.batch_size,
            # Retry failed sends up to this many times before giving up.
            # Between retries Kafka uses exponential backoff automatically.
            retries=self.cfg.retries,
            # acks="all" means the broker waits for all in-sync replicas to
            # confirm before acknowledging. Slowest but most durable.
            # For a simulation project acks=1 (leader only) is fine.
            acks=1,
            # Compress batches with snappy — reduces network bandwidth by ~50%
            # on JSON payloads with minimal CPU overhead.
            compression_type="snappy",
        )

        await self._producer.start()
        logger.info("[Producer] Connected successfully.")

    async def stop(self) -> None:
        """Flush remaining messages and close the connection gracefully."""
        if self._producer:
            logger.info("[Producer] Flushing and closing Kafka connection...")
            await self._producer.stop()
            logger.info("[Producer] Connection closed. Total sent: %d readings, %d alerts, %d errors.",
                        self._events_sent, self._alerts_sent, self._send_errors)

    async def send_event(self, event: MeterEvent) -> None:
        """
        Send a single meter event to the appropriate Kafka topic.

        Routing:
        - Normal readings → meter.readings topic
        - Fault/alert events → meter.alerts topic (AND meter.readings)
          Alert events go to both topics so the alert consumer gets them
          immediately while the readings history remains complete.

        Partition key:
          We use zone_id as the message key. Kafka hashes the key to determine
          which partition receives the message. Because we consistently use
          zone_id, all messages from the same zone always land on the same
          partition — which is what allows the consumer to do stateful
          per-zone aggregation without coordination between consumers.
        """
        if self._producer is None:
            raise RuntimeError("Producer not started. Use 'async with GridPulseProducer(cfg)'")

        key   = event.zone_id.encode("utf-8")
        value = event.to_json().encode("utf-8")

        try:
            # Always send to readings topic
            await self._producer.send(
                topic=self.cfg.readings_topic,
                key=key,
                value=value,
            )
            self._events_sent += 1

            # Also send to alerts topic if this is a fault event
            if event.event_type == "alert":
                await self._producer.send(
                    topic=self.cfg.alerts_topic,
                    key=key,
                    value=value,
                )
                self._alerts_sent += 1

        except KafkaError as exc:
            # Don't crash the simulation on a single send failure —
            # log it and continue. In production you'd route these to
            # a dead-letter queue or an alert channel.
            self._send_errors += 1
            logger.warning("[Producer] Send failed for %s: %s", event.meter_id, exc)

        # Periodically log throughput stats to the terminal
        await self._maybe_log_stats()

    async def _maybe_log_stats(self) -> None:
        """Log throughput every 30 real seconds so you can see the simulation is healthy."""
        now = time.monotonic()
        if now - self._last_log_time >= self._log_interval:
            rate = self._events_sent / max(1, now - self._last_log_time + self._log_interval)
            logger.info(
                "[Producer] Stats | readings_sent=%d | alerts_sent=%d | "
                "errors=%d | approx_rate=%.1f msg/s",
                self._events_sent, self._alerts_sent, self._send_errors, rate
            )
            self._last_log_time = now
