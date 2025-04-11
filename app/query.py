import pyspark
import sys

print(sys.argv[1:])

#!/usr/bin/env python3
import sys
import math
import re
from pyspark.sql import SparkSession

def tokenize(text):
    # Tokenize by lowercasing and extracting word characters (adjust as needed)
    return re.findall(r'\w+', text.lower())

def bm25_score(tf, df, dl, avg_dl, N, k1=1.0, b=0.75):
    # Compute BM25 idf and score components
    idf = math.log((N - df + 0.5) / (df + 0.5))
    numerator = tf * (k1 + 1)
    denominator = tf + k1 * ((1 - b) + b * (dl / avg_dl))
    return idf * (numerator / denominator)

def main():
    if len(sys.argv) < 2:
        sys.stderr.write("Usage: query.py 'query text'\n")
        sys.exit(1)
    query_text = sys.argv[1]
    query_terms = list(set(tokenize(query_text)))  # using unique terms

    # Initialize SparkSession with configuration to connect to Cassandra.
    spark = SparkSession.builder \
        .appName("BM25 Ranker") \
        .config("spark.cassandra.connection.host", "localhost") \
        .getOrCreate()
    sc = spark.sparkContext

    # Load inverted index from Cassandra (columns: term, doc_id, tf)
    inv_index_df = spark.read \
         .format("org.apache.spark.sql.cassandra") \
         .options(table="inverted_index", keyspace="bigdata") \
         .load()
    inv_index_rdd = inv_index_df.rdd.map(lambda row: (row.term, (row.doc_id, row.tf)))

    # Load vocabulary from Cassandra (columns: term, doc_freq)
    vocab_df = spark.read \
         .format("org.apache.spark.sql.cassandra") \
         .options(table="vocabulary", keyspace="bigdata") \
         .load()
    vocab_rdd = vocab_df.rdd.map(lambda row: (row.term, row.doc_freq))

    # Load document statistics from Cassandra (columns: doc_id, doc_length, doc_title)
    doc_stats_df = spark.read \
         .format("org.apache.spark.sql.cassandra") \
         .options(table="doc_stats", keyspace="bigdata") \
         .load()
    doc_stats_rdd = doc_stats_df.rdd.map(lambda row: (row.doc_id, (row.doc_length, row.doc_title)))

    # Compute global stats: total docs (N) and average document length (avg_dl)
    stats = doc_stats_rdd.map(lambda x: x[1][0]).cache()
    N = stats.count()
    avg_dl = stats.mean()

    # Filter the inverted index for query terms.
    query_index_rdd = inv_index_rdd.filter(lambda x: x[0] in query_terms)
    # Rearrange to key by doc_id for joining with document stats.
    query_by_doc = query_index_rdd.map(lambda x: (x[1][0], (x[0], x[1][1])))
    # Join with doc_stats_rdd to incorporate document length and title.
    joined = query_by_doc.join(doc_stats_rdd)
    # joined: (doc_id, ((term, tf), (doc_length, doc_title)))

    # Prepare vocabulary as a broadcast variable for fast lookups.
    vocab_dict = dict(vocab_rdd.collect())
    vocab_bc = sc.broadcast(vocab_dict)

    # Compute BM25 partial score for each record.
    scores = joined.map(lambda x: (
        x[0],
        bm25_score(
            tf = x[1][0][1],
            df = vocab_bc.value.get(x[1][0][0], 1),  # default df=1 if not found
            dl = x[1][1][0],
            avg_dl = avg_dl,
            N = N
        )
    ))

    # Sum BM25 scores per document (in case multiple query terms match).
    doc_scores = scores.reduceByKey(lambda a, b: a + b)
    # Retrieve top 10 documents by score.
    top_docs = doc_scores.takeOrdered(10, key=lambda x: -x[1])

    # Join with doc_stats to get document titles.
    top_docs_rdd = sc.parallelize(top_docs).map(lambda x: (x[0], x[1])).join(doc_stats_rdd)
    # top_docs_rdd: (doc_id, (score, (doc_length, doc_title)))
    results = top_docs_rdd.map(lambda x: (x[0], x[1][1][1], x[1][0])).collect()

    print("Top 10 relevant documents:")
    for doc in results:
        print(f"Doc ID: {doc[0]}, Title: {doc[1]}, BM25 Score: {doc[2]:.4f}")

    spark.stop()

if __name__ == '__main__':
    main()
