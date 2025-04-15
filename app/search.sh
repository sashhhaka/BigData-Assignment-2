#!/bin/bash
if [ $# -eq 0 ]; then
    echo "Usage: ./search.sh \"<your search query>\""
    exit 1
fi
# The query is all arguments combined
QUERY="$*"
echo "Searching for: $QUERY"
export PYSPARK_PYTHON=/usr/bin/python3
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3
spark-submit \
    --master yarn \
    --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 \
    --conf spark.cassandra.connection.host=cassandra-server \
    query.py "$QUERY"