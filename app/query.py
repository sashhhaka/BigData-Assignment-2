#!/usr/bin/env python3
import sys
import math
import re
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

def tokenize(text):
    return re.findall(r'\w+', text.lower())

def bm25_score(tf, df, dl, avg_dl, N, k1=1.2, b=0.75):
    # Исправленная формула IDF
    idf = math.log((N - df + 0.5) / (df + 0.5) + 1.0)
    # Формула BM25
    numerator = tf * (k1 + 1)
    denominator = tf + k1 * ((1 - b) + b * (dl / avg_dl))
    return idf * (numerator / denominator)

def main():
    print("Запуск поискового движка")
    if len(sys.argv) < 2:
        sys.stderr.write("Использование: query.py 'текст запроса'\n")
        sys.exit(1)

    query_text = sys.argv[1]
    print(f"Поисковый запрос: {query_text}")
    query_terms = list(set(tokenize(query_text)))
    print(f"Термины запроса: {query_terms}")

    # Инициализация Spark
    conf = SparkConf().setAppName("BM25 Ранжирование документов")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    # Загрузка данных из Cassandra
    inv_index_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="inverted_index", keyspace="bigdata") \
        .load()
    inv_index_rdd = inv_index_df.rdd.map(lambda row: (row.term, (row.doc_id, row.tf)))

    vocab_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="vocabulary", keyspace="bigdata") \
        .load()
    vocab_rdd = vocab_df.rdd.map(lambda row: (row.term, row.doc_freq))

    doc_stats_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="doc_stats", keyspace="bigdata") \
        .load()
    doc_stats_rdd = doc_stats_df.rdd.map(lambda row: (row.doc_id, row.doc_length))

    # Вычисление глобальных статистик
    stats = doc_stats_rdd.map(lambda x: x[1]).cache()
    N = stats.count()
    avg_dl = stats.mean()
    print(f"Документов в коллекции: {N}, средняя длина: {avg_dl}")

    # Вывод диагностической информации
    for term in query_terms:
        doc_freq = vocab_rdd.filter(lambda x: x[0] == term).collect()
        if doc_freq:
            print(f"Термин '{term}' найден в {doc_freq[0][1]} документах")
        else:
            print(f"Термин '{term}' не найден в коллекции")

    # Фильтрация индекса по терминам запроса
    query_index_rdd = inv_index_rdd.filter(lambda x: x[0] in query_terms)
    query_by_doc = query_index_rdd.map(lambda x: (x[1][0], (x[0], x[1][1])))

    # Соединение с данными о длине документов
    joined = query_by_doc.join(doc_stats_rdd)
    # joined: (doc_id, ((term, tf), doc_length))

    vocab_dict = dict(vocab_rdd.collect())
    vocab_bc = sc.broadcast(vocab_dict)

    # Расчет BM25 для каждого вхождения термина
    scores = joined.map(lambda x: (
        x[0],  # doc_id
        bm25_score(
            tf=x[1][0][1],  # частота термина
            df=vocab_bc.value.get(x[1][0][0], 1),  # встречаемость в документах
            dl=x[1][1],  # длина документа
            avg_dl=avg_dl,
            N=N
        )
    ))

    # Суммирование баллов для каждого документа
    doc_scores = scores.reduceByKey(lambda a, b: a + b)
    top_docs = doc_scores.takeOrdered(10, key=lambda x: -x[1])

    print("\nТоп-10 релевантных документов:")
    for doc_id, score in top_docs:
        print(f"Документ: {doc_id}, BM25 балл: {score:.4f}")

    spark.stop()

if __name__ == '__main__':
    main()