# Kafka

Kafka-Based Messaging

Now, with Kafka in the mix:

1. Client A sends message → WebSocket Server A.

2\. Server A publishes message → Kafka topic.

3\. Kafka writes message to disk, replicates to followers (for fault tolerance).

4\. Server B (serving Client B) consumes the message from Kafka.

5\. Server B pushes it to Client B via WebSocket.





1\. What is a “message” in Kafka?

A message (also called a record) is the basic unit of data in Kafka.

Each message consists of:

* a key (optional) — used for partitioning,
* a value — the actual payload (JSON, bytes, etc.),
* some metadata (timestamp, offset, headers, etc.).

So, if your chat app sends “Hey!” as a message, that’s one Kafka record.





Why Kafka Latency is Higher

Kafka is not in-memory only. It:

* Writes messages to disk (append-only log).
* Replicates to other brokers for durability.
* Batches data to optimize throughput (not individual message delivery).



Kafka is optimized for throughput, not necessarily low latency per message — so it shines when 

you have huge volumes of events, not necessarily when you need instantaneous delivery per user.





Kafka expects fewer, stable, long-lived producers and consumers, not millions of ephemeral ones.

Kafka is optimized for long-lived, high-throughput streams —

like application logs, sensor data, clickstreams — where topics persist for days or weeks.

Kafka expects:

* Topics to live for a long time,
* Producers and consumers to stay connected for a while,
* Large batches of data to be appended efficiently

If you created a topic for each short conversation, and then deleted it after use, Kafka would constantly:

* Create and destroy topics (expensive operations),
* Flush and compact logs,
* Rebalance metadata — all of which are slow.

Kafka can’t handle that rapid churn well.



Now, in Kafka’s design:

* A topic is like a durable log (a file-like structure on disk).
* Creating or deleting a topic is not lightweight — it involves filesystem operations, 

replication setup, partition assignments, and metadata updates across brokers.





Kafka scales by adding partitions within topics (to parallelize reads/writes), not by adding more topics.

Kafka is optimized for a small number of topics (say hundreds or thousands, not millions) — each carrying a very high volume of messages.

