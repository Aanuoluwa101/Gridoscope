"""
consumer.py — The Kafka consumer loop for one zone (one partition).

One instance of ZoneConsumer runs per zone. It:
  1. Connects to Kafka and claims its assigned partition explicitly
  2. Polls for messages in a tight async loop
  3. Passes each parsed event to the ZoneAggregator
  4. Pushes ZoneAggregate outputs to the PowerBISink when windows close
  5. Commits offsets after each successful poll batch

Partition assignment:
  Unlike a standard consumer group where Kafka assigns partitions
  automatically, we assign partitions manually (assign() not subscribe()).

  Why? Because our aggregation is stateful per zone — the ZoneAggregator
  maintains rolling averages and open window buckets in memory. If Kafka
  rebalanced and moved a partition to a different consumer, that in-memory
  state would be lost, causing a window to be partially counted.

  Manual assignment means: this consumer ALWAYS owns partition N.
  Rebalancing never happens because there's nothing to rebalance.
  The tradeoff: if this consumer dies, its partition is unread until
  the runner restarts it. For a dev/portfolio project that's acceptable.
  In production you'd add state persistence (e.g. Redis) to survive restarts.
"""

import asyncio
import json
import logging
from typing import Optional

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.errors import KafkaError

from config import ConsumerConfig, PARTITION_ZONE
from aggregator import ZoneAggregator
from powerbi_sink import PowerBISink

logger = logging.getLogger(__name__)


class ZoneConsumer:
    """
    Async Kafka consumer for one zone partition.

    Parameters
    ----------
    partition   : int — the Kafka partition this consumer owns (0-4)
    cfg         : ConsumerConfig
    sink        : shared PowerBISink instance
    meter_count : number of meters assigned to this zone
                  passed to ZoneAggregator for silent meter detection
    """

    # How many messages to fetch per poll call.
    # 100 is a good balance — large enough to amortise network round-trips,
    # small enough that processing one batch doesn't block the event loop long.
    MAX_POLL_RECORDS = 100

    # How long (ms) to wait for messages if the topic is quiet.
    # 1000ms = 1 second. The consumer yields back to the event loop while
    # waiting, so other coroutines (other zones) continue running.
    POLL_TIMEOUT_MS = 1000

    def __init__(
        self,
        partition: int,
        cfg: ConsumerConfig,
        sink: PowerBISink,
        meter_count: int,
    ):
        self.partition  = partition
        self.zone_id    = PARTITION_ZONE[partition]
        self.cfg        = cfg
        self.sink       = sink

        self._consumer: Optional[AIOKafkaConsumer] = None
        self._running   = False

        # One aggregator per consumer — owns the state for this zone
        self._aggregator = ZoneAggregator(
            zone_id=self.zone_id,
            cfg=cfg.aggregation,
            expected_meter_count=meter_count,
        )

        self._messages_processed = 0
        self._poll_errors        = 0

    async def start(self) -> None:
        """Initialise the Kafka consumer and assign the partition."""
        logger.info("[Consumer][%s] Starting on partition %d (%s)",
                    self.zone_id, self.partition, self.cfg.kafka.security_protocol)

        auth_kwargs = {}
        if self.cfg.kafka.security_protocol == "SASL_SSL":
            import ssl
            from msk_iam_auth import MSKIAMTokenProvider
            auth_kwargs = dict(
                security_protocol="SASL_SSL",
                sasl_mechanism="OAUTHBEARER",
                sasl_oauth_token_provider=MSKIAMTokenProvider(region=self.cfg.kafka.aws_region),
                ssl_context=ssl.create_default_context(),
            )

        self._consumer = AIOKafkaConsumer(
            bootstrap_servers=self.cfg.kafka.bootstrap_servers,
            **auth_kwargs,

            # Consumer group ID — all 5 zone consumers share this group.
            # Kafka uses this to track committed offsets per group.
            # Even though we use manual partition assignment, the group ID
            # is still needed for offset storage.
            group_id=self.cfg.kafka.readings_group_id,

            # Where to start if no committed offset exists for this group.
            auto_offset_reset=self.cfg.kafka.auto_offset_reset,

            # We commit offsets manually after processing each batch.
            # This gives us control over exactly when "processed" is declared.
            # auto_commit=True could commit offsets before we've finished
            # processing a batch — risking data loss on crash.
            enable_auto_commit=False,

            # Maximum messages returned per poll call
            max_poll_records=self.MAX_POLL_RECORDS,

            # Value deserialiser — Kafka delivers bytes, we decode to str.
            # JSON parsing happens separately so we can handle parse errors
            # gracefully without crashing the consumer.
            value_deserializer=lambda b: b.decode("utf-8"),
            key_deserializer=lambda b: b.decode("utf-8") if b else None,
        )

        await self._consumer.start()

        # Manually assign this consumer to its specific partition.
        # This is the key to stable state — this consumer always owns
        # partition N and never gets rebalanced away from it.
        tp = TopicPartition(self.cfg.kafka.readings_topic, self.partition)
        self._consumer.assign([tp])

        logger.info(
            "[Consumer][%s] Assigned to %s partition %d",
            self.zone_id, self.cfg.kafka.readings_topic, self.partition
        )

    async def stop(self) -> None:
        """Gracefully shut down — flush the current window and close Kafka."""
        self._running = False

        # Flush any partial window so it isn't silently dropped on shutdown
        final_aggregate = self._aggregator.flush()
        if final_aggregate:
            logger.info(
                "[Consumer][%s] Flushing partial window on shutdown (%d readings)",
                self.zone_id, self._messages_processed
            )
            try:
                await self.sink.push(final_aggregate)
            except Exception as exc:
                logger.warning(
                    "[Consumer][%s] Could not push final window on shutdown: %s",
                    self.zone_id, exc
                )

        if self._consumer:
            await self._consumer.stop()

        logger.info(
            "[Consumer][%s] Stopped. messages_processed=%d poll_errors=%d",
            self.zone_id, self._messages_processed, self._poll_errors
        )

    async def run(self) -> None:
        """
        Main poll loop — runs for the life of the consumer.

        Structure:
          while running:
            poll a batch of messages
            for each message:
              parse JSON
              pass to aggregator
              if aggregator returns an aggregate: push to sink
            commit offsets for this batch
        """
        self._running = True

        try:
            async for message in self._consumer:
                if not self._running:
                    break

                # Parse the JSON payload
                event = self._parse_message(message)
                if event is None:
                    continue   # skip unparseable messages

                # Pass to aggregator — get back an aggregate if window closed
                aggregate = self._aggregator.ingest(event)

                if aggregate is not None:
                    # Window just closed — push to Power BI (or print in dev mode)
                    await self.sink.push(aggregate)

                self._messages_processed += 1

                # Commit offset after every message.
                # In production you'd batch this (commit every N messages)
                # to reduce broker load. For a dev project, per-message
                # commits are simpler and the overhead is negligible.
                await self._consumer.commit()

        except KafkaError as exc:
            self._poll_errors += 1
            logger.error("[Consumer][%s] Kafka error: %s", self.zone_id, exc)
            raise

        except asyncio.CancelledError:
            logger.info("[Consumer][%s] Cancelled.", self.zone_id)
            raise

    # -----------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------

    def _parse_message(self, message) -> Optional[dict]:
        """
        Parse a raw Kafka message into a dict.

        Returns None on parse failure so the consumer can skip bad messages
        without crashing. In production you'd route these to a dead-letter topic.
        """
        try:
            event = json.loads(message.value)

            # Basic validation — these fields are required for aggregation
            required = {"meter_id", "zone_id", "kwh_delta", "power_kw",
                        "voltage", "power_factor", "frequency_hz",
                        "meter_state", "timestamp"}
            missing = required - event.keys()
            if missing:
                logger.warning(
                    "[Consumer][%s] Message missing fields %s — skipping. offset=%d",
                    self.zone_id, missing, message.offset
                )
                return None

            return event

        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.warning(
                "[Consumer][%s] Failed to parse message at offset %d: %s",
                self.zone_id, message.offset, exc
            )
            return None
