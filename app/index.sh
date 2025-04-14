#!/bin/bash
echo "Checking MapReduce environment:"
echo "Python version:"
python3 --version
echo "Contents of the mapreduce folder:"
ls -la mapreduce/
INPUT_PATH=${1:-/index/data}
echo "Checking availability of input data:"
hdfs dfs -ls $INPUT_PATH | head -10
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
chmod +x mapreduce/mapper1.py
chmod +x mapreduce/reducer1.py
chmod +x mapreduce/mapper2.py
chmod +x mapreduce/reducer2.py
echo "Running Pipeline 1: Aggregating term frequencies and document statistics..."
hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar \
    -input $INPUT_PATH \
    -output $PIPELINE1_OUTPUT \
    -mapper "python3 mapper1.py" \
    -reducer "python3 reducer1.py" \
    -file mapreduce/mapper1.py \
    -file mapreduce/reducer1.py
echo "Pipeline 1 completed. Output is stored in HDFS directory: $PIPELINE1_OUTPUT."
echo "Running Pipeline 2: Calculating vocabulary document frequencies..."
hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar \
    -input $PIPELINE1_OUTPUT \
    -output $PIPELINE2_OUTPUT \
    -mapper "python3 mapper2.py" \
    -reducer "python3 reducer2.py" \
    -file mapreduce/mapper2.py \
    -file mapreduce/reducer2.py
echo "Pipeline 2 completed. Vocabulary output is stored in HDFS directory: $PIPELINE2_OUTPUT."
# Load MapReduce output into Cassandra using app.py
echo "Loading MapReduce output into Cassandra..."
python app.py load $PIPELINE1_OUTPUT $PIPELINE2_OUTPUT
echo "Indexing tasks complete. Data has been loaded into Cassandra."