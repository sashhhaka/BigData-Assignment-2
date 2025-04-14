#!/usr/bin/env python3
import sys
import math
import re
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

def tokenize(text):
    return re.findall(r'\w+', text.lower())

def bm25_score(tf, df, dl, avg_dl, N, k1=1.2, b=0.75):
    # Correct IDF formula for BM25
    idf = math.log((N - df + 0.5) / (df + 0.5) + 1.0)
    # TF saturation formula for BM25
    numerator = tf * (k1 + 1)
    denominator = tf + k1 * ((1 - b) + b * (dl / avg_dl))
    return idf * (numerator / denominator)

def main():
    if len(sys.argv) < 2:
        sys.stderr.write("Usage: query.py 'query text'\n")
        sys.exit(1)

    query_text = sys.argv[1]
    print(f"Search query: {query_text}")
    query_terms = list(set(tokenize(query_text)))
    print(f"Query terms: {query_terms}")

    # Initialize Spark
    conf = SparkConf().setAppName("BM25 Search Engine")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    # Load data from Cassandra
    inv_index_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="inverted_index", keyspace="bigdata") \
        .load()

    vocab_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="vocabulary", keyspace="bigdata") \
        .load()

    doc_stats_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="doc_stats", keyspace="bigdata") \
        .load()

    # Load document titles data
    try:
        doc_data_df = spark.read.option("sep", "\t").csv("/index/data")
        doc_titles_rdd = doc_data_df.select("_c0", "_c1").rdd.map(lambda row: (row._c0, row._c1))
        doc_titles = dict(doc_titles_rdd.collect())
        print(f"Loaded titles for {len(doc_titles)} documents")
    except Exception as e:
        print(f"Failed to load titles: {str(e)}")
        doc_titles = {}

    # Convert to RDD for operations
    inv_index_rdd = inv_index_df.rdd.map(lambda row: (row.term, (row.doc_id, row.tf)))
    vocab_rdd = vocab_df.rdd.map(lambda row: (row.term, row.doc_freq))
    doc_stats_rdd = doc_stats_df.rdd.map(lambda row: (row.doc_id, row.doc_length))

    # Collection statistics
    N = doc_stats_rdd.count()
    avg_dl = doc_stats_rdd.map(lambda x: x[1]).mean()
    print(f"Documents in collection: {N}, average length: {avg_dl}")

    # Filter by query terms
    query_index_rdd = inv_index_rdd.filter(lambda x: x[0] in query_terms)
    query_by_doc = query_index_rdd.map(lambda x: (x[1][0], (x[0], x[1][1])))

    # Join with document lengths
    joined = query_by_doc.join(doc_stats_rdd)

    # Term frequency dictionary
    vocab_dict = dict(vocab_rdd.collect())
    vocab_bc = sc.broadcast(vocab_dict)

    # Calculate BM25
    scores = joined.map(lambda x: (
        x[0],  # doc_id
        bm25_score(
            tf=x[1][0][1],  # term frequency
            df=vocab_bc.value.get(x[1][0][0], 1),  # document frequency
            dl=x[1][1],  # document length
            avg_dl=avg_dl,
            N=N
        )
    ))

    # Sum scores for each document
    doc_scores = scores.reduceByKey(lambda a, b: a + b)
    top_docs = doc_scores.takeOrdered(10, key=lambda x: -x[1])

    print("\nTop 10 relevant documents:")
    for doc_id, score in top_docs:
        title = doc_titles.get(doc_id, "Title unavailable")
        print(f"ID: {doc_id}, Title: {title}, BM25: {score:.4f}")

    spark.stop()

if __name__ == '__main__':
    main()