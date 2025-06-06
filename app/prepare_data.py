#!/usr/bin/python
from pathvalidate import sanitize_filename
from tqdm import tqdm
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re

spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.maxResultSize", "4g") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "4g") \
    .getOrCreate()

df = spark.read.parquet("/a.parquet")
n = 1000
df = df.select(['id', 'title', 'text']).sample(fraction=100 * n / df.count(), seed=0).limit(n)


def clean_text(text):
    if not text:
        return ""
    return re.sub(r'\s+', ' ', text).strip()


# Register the UDF
clean_udf = udf(clean_text, StringType())

# Apply normalization to title column
df = df.withColumn("title", clean_udf("title"))


def create_doc(row):
    filename = "data/" + sanitize_filename(str(row['id']) + "_" + row['title']).replace(" ", "_") + ".txt"
    with open(filename, "w") as f:
        f.write(row['text'])


df.foreach(create_doc)

df.write \
    .option("sep", "\t") \
    .mode("overwrite") \
    .csv("/index/data")
