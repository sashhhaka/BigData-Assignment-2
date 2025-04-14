#!/bin/bash
echo "This script will include commands to search for documents given the query using Spark RDD"
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 \"your query string\""
    exit 1
fi
QUERY="$1"
echo "Starting search for query: \"$QUERY\""
# Activate the virtual environment.
source .venv/bin/activate
# Set Python interpreter paths for Spark (driver and executors).
export PYSPARK_DRIVER_PYTHON=$(which python)
export PYSPARK_PYTHON=./.venv/bin/python
# Submit the Spark job in cluster mode on YARN.
# Check if HDFS is in safe mode
SAFEMODE_STATUS=$(hdfs dfsadmin -safemode get 2>/dev/null)
if [[ $SAFEMODE_STATUS == *"ON"* ]]; then
    echo "HDFS is in safe mode. Forcing exit..."
    hdfs dfsadmin -safemode forceExit
fi
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --archives .venv.tar.gz#.venv \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./.venv/bin/python \
    --conf spark.executorEnv.PYSPARK_PYTHON=./.venv/bin/python \
    --packages com.datastax.spark:spark-cassandra-connector_2.12:3.0.0 \
    query.py "$QUERY"
