#!/bin/bash
echo "This script will include commands to search for documents given the query using Spark RDD"
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 \"your query string\""
    exit 1
fi
QUERY="$1"
echo "Starting search for query: \"$QUERY\""
# Set Python interpreter paths for Spark (driver and executors).
export PYSPARK_PYTHON=/usr/bin/python3
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --archives .venv.tar.gz#.venv \
    --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 \
    --conf spark.cassandra.connection.host=cassandra-server \
    query.py "$QUERY"
