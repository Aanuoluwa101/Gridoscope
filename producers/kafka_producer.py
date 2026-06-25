"""
kafka_producer.py — Async Kafka producer wrapper for Gridoscope.

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
import os
import time
from datetime import datetime, timezone
from typing import Optional

from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import KafkaError, TopicAlreadyExistsError

from meter_profile import MeterProfile
from meter_state_machine import MeterEvent
from config import KafkaConfig

logger = logging.getLogger(__name__)


ZONE_PARTITION = {
    "ZONE-NORTH":   int(os.environ.get("ZONE_NORTH_PARTITION",   "0")),
    "ZONE-SOUTH":   int(os.environ.get("ZONE_SOUTH_PARTITION",   "1")),
    "ZONE-EAST":    int(os.environ.get("ZONE_EAST_PARTITION",    "2")),
    "ZONE-WEST":    int(os.environ.get("ZONE_WEST_PARTITION",    "3")),
    "ZONE-CENTRAL": int(os.environ.get("ZONE_CENTRAL_PARTITION", "4")),
}

class GridoscopeProducer:
    """
    Async Kafka producer with connection lifecycle and routing logic.

    Usage:
        async with GridoscopeProducer(cfg) as producer:
            await producer.send_event(event)
    """

    def __init__(self, cfg: KafkaConfig):
        self.cfg      = cfg
        self._producer: Optional[AIOKafkaProducer] = None

        # Simple throughput counters for terminal logging
        self._events_sent     = 0
        self._alerts_sent     = 0
        self._send_errors     = 0
        self._last_log_time   = time.monotonic()
        self._log_interval    = 30.0   # log throughput stats every 30 seconds
        self._latest_sim_time = "N/A"  # most recent simulated timestamp seen

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *args):
        await self.stop()

    async def _ensure_topics(self, auth_kwargs: dict) -> None:
        replication_factor = int(os.environ.get("KAFKA_TOPIC_REPLICATION_FACTOR", "3"))
        topics = [
            NewTopic(name=self.cfg.readings_topic, num_partitions=5, replication_factor=replication_factor),
            NewTopic(name=self.cfg.alerts_topic,   num_partitions=1, replication_factor=replication_factor),
        ]
        admin = AIOKafkaAdminClient(
            bootstrap_servers=self.cfg.bootstrap_servers, **auth_kwargs
        )
        await admin.start()
        try:
            await admin.create_topics(topics)
            logger.info("[Producer] Topics created: %s, %s",
                        self.cfg.readings_topic, self.cfg.alerts_topic)
        except TopicAlreadyExistsError:
            logger.info("[Producer] Topics already exist, skipping creation.")
        except Exception as e:
            logger.warning("[Producer] Topic creation skipped (%s) — relying on broker auto-create.", e)
        finally:
            await admin.close()

    async def start(self) -> None:
        """
        Create and start the Kafka producer connection.

        Called once at simulation startup. The producer maintains a
        persistent connection to the broker — we don't reconnect per message.
        """
        logger.info("[Producer] Connecting to Kafka at %s (%s)",
                    self.cfg.bootstrap_servers, self.cfg.security_protocol)

        auth_kwargs = {}
        if self.cfg.security_protocol == "SASL_SSL":
            import ssl
            from msk_iam_auth import MSKIAMTokenProvider
            auth_kwargs = dict(
                security_protocol="SASL_SSL",
                sasl_mechanism="OAUTHBEARER",
                sasl_oauth_token_provider=MSKIAMTokenProvider(region=self.cfg.aws_region),
                ssl_context=ssl.create_default_context(),
            )

        await self._ensure_topics(auth_kwargs)  # making sure topics are available

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.cfg.bootstrap_servers,
            **auth_kwargs,
            # How long to buffer messages before sending a batch.
            # linger_ms > 0 means we wait a little to build up a batch,
            # which is more efficient than sending one message at a time.
            linger_ms=self.cfg.linger_ms,
            # Maximum size of one batch in bytes.
            max_batch_size=self.cfg.batch_size,
            
            # Retry failed sends up to this many times before giving up.
            # Between retries Kafka uses exponential backoff automatically.
            
            # retries=self.cfg.retries,
            retry_backoff_ms=self.cfg.retries,
            
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
            raise RuntimeError("Producer not started. Use 'async with GridoscopeProducer(cfg)'")

        partition = ZONE_PARTITION.get(event.zone_id, 0)
        key   = event.zone_id.encode("utf-8")
        value = event.to_json().encode("utf-8")

        # Stamp the Kafka record with the simulated time so the S3 sink's
        # "Record" timestamp extractor partitions by simulated hour, not wall clock.
        sim_ts_ms = int(
            datetime.strptime(event.timestamp, "%Y-%m-%dT%H:%M:%SZ")
            .replace(tzinfo=timezone.utc)
            .timestamp() * 1000
        )
        self._latest_sim_time = event.timestamp

        try:
            # Always send to readings topic
            await self._producer.send(
                topic=self.cfg.readings_topic,
                key=key,
                value=value,
                partition=partition,
                timestamp_ms=sim_ts_ms,
            )
            self._events_sent += 1

            # Also send to alerts topic if this is a fault event
            if event.event_type == "alert":
                await self._producer.send(
                    topic=self.cfg.alerts_topic,
                    key=key,
                    value=value,
                    timestamp_ms=sim_ts_ms,
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
                "[Producer] Stats | sim_time=%s | readings_sent=%d | alerts_sent=%d | "
                "errors=%d | approx_rate=%.1f msg/s",
                self._latest_sim_time, self._events_sent, self._alerts_sent,
                self._send_errors, rate,
            )
            self._last_log_time = now
