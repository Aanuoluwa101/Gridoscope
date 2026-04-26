from confluent_kafka import Producer
import uuid 
import json


producer = Producer({"bootstrap.servers": "localhost:9092"})


def delivery_report(error, message):
    if error:
        print(f"❌ Delivery Failed: {error}")
    else:
        print(f"✅ Delvery Successful: {message.value().decode("utf-8")}")
        print(f"✅ Message Delivered to topic {message.topic()}, Partition {message.partition()}, at offset {message.offset()}")
        print(type(message.topic()), type(message.partition()), type(message.offset()), type(message.value()))
        # topic: str 
        # partition: int 
        # offset: int 
        # value: bytes

order = {
    "order_id": str(uuid.uuid4()),
    "user": "aea",
    "item": "rice",
    "quantity": 1
}


value = json.dumps(order).encode("utf-8")

producer.produce(
    topic="orders", 
    value=value,
    partition=0, # optional, if not specified, the producer will use a partitioner to determine which partition to send the message to
    callback=delivery_report
    )

producer.flush() # makes sure that all buffered events are sent in the case of any crash
