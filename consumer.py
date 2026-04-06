from confluent_kafka import Consumer
import json


consumer_config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "order-tracker",
        "auto.offset.reset": "earliest", # what to do if the consumer    
    }

# a unique string that identifies the consumer group this consumer belongs to
# Identifies a group of consumers that are instances of the same program

# what to do if the consumer can't find where it left off reading messages


consumer = Consumer(consumer_config)

consumer.subscribe(["orders"])

print("✅ Consumer is running and subscribed to orders topic")


while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"❌ Error: {msg.error()}")
        continue

    value = msg.value().decode("utf-8") # convert to json string
    order = json.loads(value)
    
    