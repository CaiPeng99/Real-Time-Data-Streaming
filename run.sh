#!/bin/bash

export SPARK_HOME="/Users/caipeng/spark-3.5.5-scala2.13"
export PATH="$SPARK_HOME/bin:$PATH"

$SPARK_HOME/bin/spark-submit \
  --master spark://192.168.0.92:7077 \
  --packages com.datastax.spark:spark-cassandra-connector_2.13:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0 \
  spark_stream.py

