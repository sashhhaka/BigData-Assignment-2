#!/bin/bash
#!/bin/bash
# index.sh - Script to run the Hadoop MapReduce pipelines for indexing.
# Usage: ./index.sh [input_path]
# Default input_path is /index/data in HDFS if no argument is provided.
#!/bin/bash
# index.sh: Script to run Hadoop MapReduce pipelines for indexing.
# Usage: ./index.sh [input_path]
# If no input_path is provided, it defaults to /index/data in HDFS.
# Set default input path from HDFS if not provided as argument.
INPUT_PATH=${1:-/index/data}
# Define temporary output directories for the two pipelines.
PIPELINE1_OUTPUT=/tmp/mapreduce_pipeline1_output
PIPELINE2_OUTPUT=/tmp/mapreduce_pipeline2_output
echo "This script include commands to run mapreduce jobs using hadoop streaming to index documents"
echo "Input file is : $INPUT_PATH"
echo $1
hdfs dfs -ls /
# Remove previous HDFS output directories (if any)
hadoop fs -rm -r $PIPELINE1_OUTPUT 2>/dev/null
hadoop fs -rm -r $PIPELINE2_OUTPUT 2>/dev/null
echo "Running Pipeline 1: Aggregating term frequencies and document statistics..."
hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
    -input $INPUT_PATH \
    -output $PIPELINE1_OUTPUT \
    -mapper "python3 mapreduce/mapper1.py" \
    -reducer "python3 mapreduce/reducer1.py" \
    -file mapreduce/mapper1.py \
    -file mapreduce/reducer1.py
echo "Pipeline 1 completed. Output is stored in HDFS directory: $PIPELINE1_OUTPUT."
echo "Running Pipeline 2: Calculating vocabulary document frequencies..."
hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
    -input $PIPELINE1_OUTPUT \
    -output $PIPELINE2_OUTPUT \
    -mapper "python3 mapreduce/mapper2.py" \
    -reducer "python3 mapreduce/reducer2.py" \
    -file mapreduce/mapper2.py \
    -file mapreduce/reducer2.py
echo "Pipeline 2 completed. Vocabulary output is stored in HDFS directory: $PIPELINE2_OUTPUT."
echo "Indexing tasks complete. Verify results and check Cassandra tables for inserted data if using direct insertion."

