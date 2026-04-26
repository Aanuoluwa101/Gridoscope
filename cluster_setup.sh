docker exec gridpulse-kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --topic meter.readings \
  --partitions 5 --replication-factor 1

docker exec gridpulse-kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --topic meter.alerts \
  --partitions 5 --replication-factor 1