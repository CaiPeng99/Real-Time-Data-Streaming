#!/bin/bash

SPARK_IMAGE="my-spark-with-cassandra"
SPARK_MASTER="spark://spark-master:7077"
NETWORK_NAME="realtimestreaming_proj_confluent"
LOCAL_APP_DIR=$(pwd)
APP_MAIN_FILE="spark_stream.py"
CONTAINER_APP_DIR="/app"

# Lookup Cassandra IP
CASSANDRA_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' cassandra)


# Check if the Docker network exists
if ! docker network inspect "$NETWORK_NAME" >/dev/null 2>&1; then
  echo "❌ Docker network '$NETWORK_NAME' not found."
  echo "👉 Please run 'docker-compose up -d' first to start the cluster."
  exit 1
fi

echo "🚀 Submitting Spark job to $SPARK_MASTER using $SPARK_IMAGE..."

docker run -it --rm \
  --network "$NETWORK_NAME" \
  -v "$LOCAL_APP_DIR:$CONTAINER_APP_DIR" \
  "$SPARK_IMAGE" \
  bin/spark-submit \
  --master "$SPARK_MASTER" \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1\
  "$CONTAINER_APP_DIR/$APP_MAIN_FILE"
