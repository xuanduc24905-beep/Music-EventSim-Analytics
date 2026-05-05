"""
03_spark_streaming.py — Spark Structured Streaming.
Đọc từ Kafka topic 'music-events', parse JSON, filter page=NextSong,
window aggregation 1 phút cho top songs và active users,
write parquet ra HDFS /music/streaming/ trigger mỗi 2s.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, IntegerType, TimestampType
)

spark = SparkSession.builder \
    .appName("Music Kafka Streaming") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "12g") \
    .config("spark.executor.cores", "8") \
    .config("spark.sql.shuffle.partitions", "32") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("artist",        StringType(),  True),
    StructField("song",          StringType(),  True),
    StructField("duration",      DoubleType(),  True),
    StructField("ts",            LongType(),    True),
    StructField("userId",        IntegerType(), True),
    StructField("sessionId",     IntegerType(), True),
    StructField("page",          StringType(),  True),
    StructField("level",         StringType(),  True),
    StructField("location",      StringType(),  True),
    StructField("userAgent",     StringType(),  True),
    StructField("gender",        StringType(),  True),
    StructField("firstName",     StringType(),  True),
    StructField("lastName",      StringType(),  True),
    StructField("registration",  LongType(),    True),
    StructField("itemInSession", IntegerType(), True),
    StructField("status",        IntegerType(), True),
    StructField("method",        StringType(),  True),
])

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "music-events") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

df_parsed = df_kafka.select(
    F.from_json(F.col("value").cast("string"), schema).alias("data"),
    F.col("timestamp").alias("kafka_timestamp"),
).select("data.*", "kafka_timestamp")

# Only process NextSong events
df_songs = df_parsed.filter(F.col("page") == "NextSong") \
    .withColumn("event_ts", F.to_timestamp(F.col("ts") / 1000)) \
    .withColumn("ingestion_time", F.current_timestamp()) \
    .dropna(subset=["userId", "artist", "song"])

# ── Batch Layer accumulation: write ALL events to /music/raw/ ─
# 01_eda.py and 02_analytics.py read from here
query_raw = df_songs.writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:9000/music/raw/") \
    .option("checkpointLocation", "hdfs://namenode:9000/music/streaming_checkpoint/raw") \
    .outputMode("append") \
    .trigger(processingTime="2 seconds") \
    .start()

# ── Speed Layer: write recent events to /music/streaming/ ─────
# 05_export.py reads from here for the speed/merged views
query_hdfs = df_songs.writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:9000/music/streaming/") \
    .option("checkpointLocation", "hdfs://namenode:9000/music/streaming_checkpoint/speed") \
    .outputMode("append") \
    .trigger(processingTime="2 seconds") \
    .start()

# ── Window aggregation: top songs per 1-minute window ────────
df_windowed = df_songs \
    .withWatermark("event_ts", "2 minutes") \
    .groupBy(
        F.window("event_ts", "1 minute"),
        "artist",
        "song",
    ).agg(
        F.count("*").alias("play_count"),
        F.approx_count_distinct("userId").alias("active_users"),
        F.round(F.avg("duration"), 1).alias("avg_duration_sec"),
    ) \
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "artist", "song", "play_count", "active_users", "avg_duration_sec",
    )

query_window = df_windowed.writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:9000/music/streaming_window/") \
    .option("checkpointLocation", "hdfs://namenode:9000/music/streaming_checkpoint/window") \
    .outputMode("append") \
    .trigger(processingTime="2 seconds") \
    .start()

# ── Console: active users summary every 10s ───────────────────
query_console = df_songs \
    .groupBy("level") \
    .agg(
        F.count("*").alias("plays"),
        F.approx_count_distinct("userId").alias("active_users"),
        F.approx_count_distinct("artist").alias("unique_artists"),
    ) \
    .writeStream \
    .format("console") \
    .outputMode("complete") \
    .trigger(processingTime="10 seconds") \
    .start()

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("Stopping streaming...")
finally:
    query_raw.stop()
    query_hdfs.stop()
    query_window.stop()
    query_console.stop()
    spark.stop()
    print("Streaming stopped.")
