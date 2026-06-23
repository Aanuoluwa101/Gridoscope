

docker exec gridoscope-kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --topic meter.readings \
  --if-not-exists \
  --partitions 5 --replication-factor 1

docker exec gridoscope-kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --topic meter.alerts \
  --if-not-exists \
  --partitions 5 --replication-factor 1


