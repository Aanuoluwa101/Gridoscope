1\. CLUSTER\_ID not KAFKA\_CLUSTER\_ID

2\. base64 string expected, not just anything

3\. I configured KAFKA\_CONTROLLER\_LISTENER\_NAMES: CONTROLLER on a broker only node, 

it asked me to set up a security map for it. The node died but other ones continued

I tried removing this variable since it's a broker only node but it seems it's needed





Partitions are not semantic splits

A partition is an append-only log that lives on disk and is replicated across brokers 

The key (e.g user\_id), passed by the producer determines which partition data is written to

If no key is provided, kafak round robins



Kafka only gurantees ordering within a partition

partitions are there for parallelism and scalability (high throughput)



replication factor: 1 leader, 2 followers

It means there'll be 3 copies of that partition

3 partitions with RF=3 means we have 9 partitions



ISR stands for **In-Sync Replicas** — the set of replicas that are fully caught up with the leader.





Each partition is a directory on the disk

Messages are stored as segment files inside that directory (partition)

All partitions (dirs) live in **kraft-logs** dir

partition directory looks like **{topic}-{partition}**

Important files in this directory

1. **.log**: segement file that contains the actual messages. looks like 00000000000000.log
2. **.index**: offsets:byte position index 
3. **.timeindex:** timestamps:offsets index





00000000000000000823.log     ← second segment starting at offset 823

log.segment.bytes, log.segment.ms







**commands**

kafka-topics --bootstrap-server localhost:9092 --list

kafka-topics --bootstrap-server localhost:9092 --create --topic fixtures --partitions 2

kafka-topics --bootstrap-server localhost:9092 --topic orders --describe
kafka-consumer-groups \
--bootstrap-server kafka1:9092 \
--list

kafka-consumer-groups --bootstrap-server kafka1:9092 --describe --group console-consumer-89557
kafka-metadata-quorum --bootstrap-server kafka1:9092 describe --status
kafka-dump-log \
  --files /tmp/kraft-logs/__cluster_metadata-0/00000000000000000000.log \
  --print-data-log
kafka-console-consumer \
  --bootstrap-server kafka1:9092 \
  --topic orders \
  --from-beginning \
  --property print.partition=true




**Questions**

1. What happens if the entry point dies

2\. What are the factors the influence what topics and number of 

partitions and RF etc

3\. what happens when we increase or decrease partitions







The other thing worth knowing: if you add more partitions later, 

hash(key) % new\_partition\_count changes — so "eu" might suddenly 

land on a different partition. This is why Kafka  

strongly discourages changing partition counts on existing topics that use keyed messages.


  --property print.partition=true
use that with console consumer to see what message is in what partition


LAG is just LOG-END-OFFSET - CURRENT-OFFSET. It's the number of messages sitting in the partition that this group hasn't processed yet. 
In production, lag is the single most important metric to watch — a growing lag means your consumers are falling behind your producers.


❌ Within a consumer group: NO — only one consumer can read a partition at a time
✅ Across different consumer groups: YES — multiple consumers can read the same partition


wo consumer groups both reading the orders topic each get their own copy of every message and track their own offsets separately. 
Kafka cannot guarantee global ordering across partitions

consumer group rebalancing happens fast but can still sometimes cause lag spikes in production.

__cluster_metadata-0 = “the single, globally ordered log of everything Kafka knows about itself
It’s named like that because Kafka treats metadata as a special internal topic with one partition

Only the controller leader writes to the metadata file. Followers simply replicate it.
The log is committed only after a majority of voters have acknowledged it. 

__consumer_offsets is shared globally across the entire Kafka cluster,
BUT it stores offsets separately for each consumer group



MaxFollowerLag: 35597 and MaxFollowerLagTimeMs: -1
The -1 on the time lag means no voter is actually behind in time — all three voters are fully caught up.  
The offset number is misleading here; the time value is what matters for health.






