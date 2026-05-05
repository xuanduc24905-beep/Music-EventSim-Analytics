"""
00_batch_processing.py — Batch Layer: Ingestion & Cleaning
Đọc raw parquet /music/raw/ → filter NextSong → dedup → cast event_ts
→ ghi /music/batch/clean/  (input chung cho 01_eda và 02_analytics)
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, IntegerType, TimestampType,
)
import sys

HDFS       = "hdfs://namenode:9000"
RAW_PATH   = f"{HDFS}/music/raw/"
CLEAN_PATH = f"{HDFS}/music/batch/clean"

RAW_SCHEMA = StructType([
    StructField("artist",          StringType(),    True),
    StructField("song",            StringType(),    True),
    StructField("duration",        DoubleType(),    True),
    StructField("ts",              LongType(),      True),
    StructField("userId",          IntegerType(),   True),
    StructField("sessionId",       IntegerType(),   True),
    StructField("page",            StringType(),    True),
    StructField("level",           StringType(),    True),
    StructField("location",        StringType(),    True),
    StructField("userAgent",       StringType(),    True),
    StructField("gender",          StringType(),    True),
    StructField("firstName",       StringType(),    True),
    StructField("lastName",        StringType(),    True),
    StructField("registration",    LongType(),      True),
    StructField("itemInSession",   IntegerType(),   True),
    StructField("status",          IntegerType(),   True),
    StructField("method",          StringType(),    True),
    StructField("event_ts",        TimestampType(), True),
    StructField("kafka_timestamp", TimestampType(), True),
    StructField("ingestion_time",  TimestampType(), True),
])

spark = SparkSession.builder \
    .appName("Music Batch Processing") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "4") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

try:
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jvm.java.net.URI.create(HDFS),
        spark._jsc.hadoopConfiguration()
    )
    files = list(fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(RAW_PATH)))
    if not any(".parquet" in str(f.getPath()) for f in files):
        print("[ERROR] /music/raw/ chưa có parquet files. Chạy streaming job trước.")
        spark.stop()
        sys.exit(1)
except Exception as e:
    print(f"[ERROR] Không kiểm tra được HDFS: {e}")
    spark.stop()
    sys.exit(1)

df_raw = spark.read.schema(RAW_SCHEMA).parquet(RAW_PATH)
raw_count = df_raw.count()
print(f"Raw events       : {raw_count:,}")

df_clean = (
    df_raw
    .filter(F.col("page") == "NextSong")
    .dropDuplicates(["userId", "sessionId", "ts"])
    .withColumn(
        "event_ts",
        F.when(F.col("event_ts").isNull(), F.to_timestamp(F.col("ts") / 1000))
         .otherwise(F.col("event_ts"))
    )
)

clean_count = df_clean.count()
print(f"Clean NextSong   : {clean_count:,}  ({raw_count - clean_count:,} dropped)")

df_clean.write.mode("overwrite").parquet(CLEAN_PATH)
print(f"Saved → {CLEAN_PATH}")
spark.stop()
