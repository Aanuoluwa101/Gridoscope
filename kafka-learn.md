# kafka

message queue or stream processing system



Now we have a large number of events and the server holding our

queue is struggling to keep up - running out of space





If we naively scale horizontally, we have a couple of problems

1. **Events are out of order:** the producer randomly distributes the events across servers and

&#x20;  because the consumer is reading off of both queues, we lose order of events

&#x20;

&#x20;  Kafka provides a solution: in order to scale, messages sent and received through kafka

&#x20;  require a user-specified distribution strategy



&#x20;  Distributing items into different queues based on something they're associated with e.g game, microservice



&#x20;  **TOPICS AND PARTITIONS**

&#x20;  Example of topics: basketball, football

&#x20;  partitions: Nig VS Brazil, Argentina vs Portugal



2\. **Double event reading with more consumers**

&#x20;  When our queue scales and we have more events, chances are our consumers can't keep up.

&#x20;  Naively adding more consumers can cause a problem of double reading events



&#x20;  **CONSUMER GROUPS**





Kafka cluster

**Broker**: a server (physical or virtual) that hold the queue

**Partitions**: called **queue** - ordered, immutable, sequence of messages on the disk. Like a log file

&#x20;           each broker can have more than one partition. Partitions are physical

**Topic**: Logical groupings of partitions. It's just a grouping in code





**Lifecycle of a message in a Kafka cluster**

Message is sent by producer

Message is called **record** in kafka

* Key
* value
* timestamp (used for ordering if no ordering is specified)
* headers





the key is also known as the partition key. It determines which partition the message should go into

If no key is specified, kafka round-robins



If there's a key, we hash and take modulo and that gives us the partition

We then determine which broker the partition is located on. There's an internal controller in kafka that

keeps broker:partitions mapping





Consumers read messages based on offsets

Kafka keeps a record of the last offset read by a consumer





Kafka does **replication**

Each partition has a leader and followers - which can live on the same or different brokers

The leader handles all the reads and writes. The followers just get the data and serve as backup



So for example, on a broker, you can have 3 partitions: Topic A partition leader, Topic B partition leader, Topic B partition follower etc





**WHEN TO USE KAFKA**

1. Async processing
2. In-order message processing
3. Decouple producer and consumer for independent scaling
4. Stream: processing data in realtime e.g live dashboards, location updates
5. Pub/Sub: where streams of messages need to be processed by multiple consumers simulteanously

6\. middleman between microservices







A consumer group can contain many consumers that service a single partition



Topic: Orders

partitions: EU orders, US orders etc





Kafka's data persistence is one of the things that make it different from traditional message brokers





**Active controller:** The broker that serves as the cluster's controller. One per cluster per time.

tracks which broker is the leader for each partition. reassigns partitions in case of broker failure, manages cluster state and administrations



Kafka keeps a journal of which messages have been read by which consumers from which topic





**PLAINTEXT://:9092**

* who uses it: consumers, producers, other brokers
* producing and consuming messages, Partition replication between brokers





**CONTROLLER://:9093**

used for internal coordination between controllers. Controller-to-controller (Raft)

* who uses it: only controller nodes; **even broker-only nodes don't listen on it**
* Leader election (Raft), Metadata replication, Cluster decisions (topic creation, partition changes)





Every node listed in KAFKA\_CONTROLLER\_QUORUM\_VOTERS must have the controller role.

Only nodes with the controller role:

* Participate in leader election
* Store and replicate cluster metadata
* Vote in the quorum







KAFKA\_CONTROLLER\_QUORUM\_VOTERS: Who are the decision-makers (controllers) in this Kafka cluster?

KAFKA\_OFFSETS\_TOPIC\_REPLICATION\_FACTOR: How many brokers should store copies of this critical metadata?

KAFKA\_LISTENERS: Defines what ports this node listens on

KAFKA\_LISTENER\_SECURITY\_PROTOCOL\_MAP: Maps listener names → security protocols

KAFKA\_CONTROLLER\_LISTENER\_NAMES: This is the listener used for controller (Raft) communication







* Kafka tracks consumer progress (what messages have been read) in an internal topic called **\_\_consumer\_offsets**
* Replication factor ≤ number of brokers



Every broker knows metadata, but only controllers decide metadata.



**Cluster Metadata (KRaft metadata log)**

* lives in Kraft metdata log and can stored and replicated only among controllers
* Stores topics, partitions, leader assignments, config changes, ISR (in-sync replicas)
* The brain of the cluster. Broker-only nodes only cache it and apply updates



**\_\_consumer\_offsets**

* Internal topic
* Stores Consumer group offsets, Group coordination state
* Lives on any broker





&#x20;



