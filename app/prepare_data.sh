#!/bin/bash
source .venv/bin/activate
export PYSPARK_SUBMIT_ARGS="--driver-memory 8g --executor-memory 8g pyspark-shell"
export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON
# DOWNLOAD a.parquet or any parquet file before you run this
hdfs dfs -put -f a.parquet / && \
    spark-submit --driver-memory 8g prepare_data.py && \
    echo "Putting data to hdfs" && \
    hdfs dfs -put data / && \
    hdfs dfs -ls /data && \
    hdfs dfs -ls /index/data && \
    echo "done data preparation!"