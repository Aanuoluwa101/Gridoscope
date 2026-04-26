"""
test_producer.py — Minimal cluster test. One event per zone, nothing fancy.

Sends 5 events (one per zone) to meter.readings and shows exactly which
partition each one landed on. Use this to confirm:
  - Kafka cluster is reachable
  - Topics exist
  - Zone-based partitioning is working correctly (same zone = same partition)

Usage:
    python test_producer.py
"""

from confluent_kafka import Producer
import uuid
import json
from datetime import datetime

producer = Producer({"bootstrap.servers": "localhost:9092"})


ZONE_PARTITION = {
    "ZONE-NORTH":   0,
    "ZONE-SOUTH":   1,
    "ZONE-EAST":    2,
    "ZONE-WEST":    3,
    "ZONE-CENTRAL": 4,
}


def delivery_report(error, message):
    if error:
        print(f"  ❌ Delivery failed: {error}")
    else:
        key = message.key().decode("utf-8") if message.key() else "None"
        print(
            f"  ✅  zone={key:<16}"
            f"  topic={message.topic()}"
            f"  partition={message.partition()}"
            f"  offset={message.offset()}"
        )


ZONES = [
    "ZONE-NORTH",
    "ZONE-SOUTH",
    "ZONE-EAST",
    "ZONE-WEST",
    "ZONE-CENTRAL",
]

print()
print("Sending one test event per zone to meter.readings...")
print()

for zone in ZONES:
    event = {
        "event_id":      str(uuid.uuid4()),
        "meter_id":      "MTR-TEST",
        "zone_id":       zone,
        "customer_type": "residential",
        "timestamp":     datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "kwh_delta":     0.025,
        "voltage":       120.0,
        "meter_state":   "normal",
    }

    producer.produce(
        topic="meter.readings",
        key=zone.encode("utf-8"),       # zone_id is the partition key
        value=json.dumps(event).encode("utf-8"),
        partition=ZONE_PARTITION[zone],
        callback=delivery_report,
    )

producer.flush()
print()
print("Done. Check Kafka UI → Topics → meter.readings → Messages to browse events.")
print("Each zone should consistently map to the same partition on every run.")
print()