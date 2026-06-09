# create policy
aws iam create-policy \
  --policy-name gridoscope-kafka-connect-s3-dev \
  --policy-document file://gridoscope-s3-policy-dev.json


# arn:aws:iam::733024282612:policy/gridoscope-kafka-connect-s3-dev
# create user 
aws iam create-user \
  --user-name gridoscope-kafka-connect-dev


# attach policy to user
aws iam attach-user-policy \
  --user-name gridoscope-kafka-connect-dev \
  --policy-arn arn:aws:iam::733024282612:policy/gridoscope-kafka-connect-s3-dev


# verify attachement 
aws iam list-attached-user-policies \
  --user-name gridoscope-kafka-connect-dev


# create access key for user
aws iam create-access-key \
  --user-name gridoscope-kafka-connect-dev



curl http://localhost:8083/connectors

curl -X POST \
  -H "Content-Type: application/json" \
  --data @s3_sink_readings.json \
  http://localhost:8083/connectors

# Check it's running
curl -s http://localhost:8083/connectors/gridoscope-s3-sink-readings/status | python -m json.tool




# Delete existing connector
curl -X DELETE http://localhost:8083/connectors/gridoscope-s3-sink-readings

# Redeploy with updated config
curl -X POST \
  -H "Content-Type: application/json" \
  --data @docker/s3_sink_readings.json \
  http://localhost:8083/connectors