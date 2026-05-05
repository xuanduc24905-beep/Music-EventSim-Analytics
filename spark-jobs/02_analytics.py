"""
02_analytics.py — Music Streaming Analytics (Batch Layer).
Tính:
  - Top 10 songs / top 10 artists
  - Plays by hour of day
  - Free vs paid ratio
  - Session length distribution
  - User retention (new vs returning)
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, IntegerType, TimestampType
)
import subprocess, sys

RAW_SCHEMA = StructType([
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
    StructField("event_ts",      TimestampType(),True),
    StructField("kafka_timestamp",TimestampType(),True),
    StructField("ingestion_time",TimestampType(),True),
])

spark = SparkSession.builder \
    .appName("Music Analytics") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .config("spark.sql.shuffle.partitions", "16") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

result = subprocess.run(
    ["hdfs", "dfs", "-ls", "hdfs://namenode:9000/music/raw/"],
    capture_output=True, text=True
)
parquet_ready = any(".parquet" in line for line in result.stdout.splitlines())
if not parquet_ready:
    print("[ERROR] /music/raw/ chưa có parquet files.")
    print("        Hãy chạy 03_spark_streaming.py trước và đợi ít nhất 30 giây.")
    spark.stop()
    sys.exit(1)

df = spark.read.schema(RAW_SCHEMA).parquet("hdfs://namenode:9000/music/raw/") \
         .filter(F.col("page") == "NextSong") \
         .withColumn("event_ts", F.to_timestamp(F.col("ts") / 1000))

# ── 1. Top 10 songs ───────────────────────────────────────────
top10_songs = df.groupBy("song", "artist").agg(
    F.count("*").alias("play_count"),
    F.countDistinct("userId").alias("unique_listeners"),
    F.round(F.avg("duration"), 1).alias("avg_duration_sec"),
).orderBy(F.desc("play_count")).limit(10)

top10_songs.show(truncate=False)
top10_songs.write.mode("overwrite").parquet("hdfs://namenode:9000/music/batch/top10_songs")

# ── 2. Top 10 artists ─────────────────────────────────────────
top10_artists = df.groupBy("artist").agg(
    F.count("*").alias("play_count"),
    F.countDistinct("userId").alias("unique_listeners"),
    F.countDistinct("song").alias("unique_songs"),
).orderBy(F.desc("play_count")).limit(10)

top10_artists.show(truncate=False)
top10_artists.write.mode("overwrite").parquet("hdfs://namenode:9000/music/batch/top10_artists")

# ── 3. Plays by hour of day ───────────────────────────────────
plays_by_hour = df.withColumn(
    "hour_of_day", F.hour(F.col("event_ts"))
).groupBy("hour_of_day").agg(
    F.count("*").alias("play_count"),
    F.countDistinct("userId").alias("active_users"),
    F.round(F.avg("duration"), 1).alias("avg_duration_sec"),
).orderBy("hour_of_day")

plays_by_hour.show(24, truncate=False)
plays_by_hour.write.mode("overwrite").parquet("hdfs://namenode:9000/music/batch/plays_by_hour")

# ── 4. Free vs Paid ratio ─────────────────────────────────────
total_users = df.select("userId", "level").distinct().count()

level_ratio = df.groupBy("level").agg(
    F.count("*").alias("play_count"),
    F.countDistinct("userId").alias("user_count"),
    F.round(F.avg("duration"), 1).alias("avg_duration_sec"),
    F.round(F.sum("duration") / 3600, 2).alias("total_hours_played"),
).withColumn(
    "user_pct", F.round(F.col("user_count") / total_users * 100, 1)
)

level_ratio.show(truncate=False)
level_ratio.write.mode("overwrite").parquet("hdfs://namenode:9000/music/batch/level_ratio")

# ── 5. Session length distribution ───────────────────────────
session_stats = df.groupBy("userId", "sessionId").agg(
    F.count("*").alias("songs_per_session"),
    F.round(F.sum("duration") / 60, 1).alias("session_minutes"),
    F.min("event_ts").alias("session_start"),
    F.max("event_ts").alias("session_end"),
).withColumn(
    "session_duration_min",
    F.round((F.unix_timestamp("session_end") - F.unix_timestamp("session_start")) / 60, 1)
)

session_dist = session_stats.groupBy("songs_per_session").agg(
    F.count("*").alias("session_count"),
    F.round(F.avg("session_minutes"), 1).alias("avg_session_minutes"),
).orderBy("songs_per_session")

session_dist.show(20, truncate=False)
session_dist.write.mode("overwrite").parquet("hdfs://namenode:9000/music/batch/session_dist")

session_stats.write.mode("overwrite").parquet("hdfs://namenode:9000/music/batch/session_stats")

# ── 6. User retention (new vs returning) ─────────────────────
df_day = df.withColumn("play_date", F.to_date(F.col("event_ts")))

first_play = df_day.groupBy("userId").agg(
    F.min("play_date").alias("first_play_date")
)

user_activity = df_day.join(first_play, on="userId") \
    .withColumn(
        "user_type",
        F.when(F.col("play_date") == F.col("first_play_date"), "new")
         .otherwise("returning")
    )

retention = user_activity.groupBy("play_date", "user_type").agg(
    F.countDistinct("userId").alias("user_count"),
    F.count("*").alias("play_count"),
).orderBy("play_date", "user_type")

retention.show(20, truncate=False)
retention.write.mode("overwrite").parquet("hdfs://namenode:9000/music/batch/retention")

# ── 7. Active users over time (hourly) ───────────────────────
active_users_ts = df.withColumn(
    "hour_bucket",
    F.date_trunc("hour", F.col("event_ts"))
).groupBy("hour_bucket").agg(
    F.countDistinct("userId").alias("active_users"),
    F.count("*").alias("total_plays"),
).orderBy("hour_bucket")

active_users_ts.show(20, truncate=False)
active_users_ts.write.mode("overwrite").parquet("hdfs://namenode:9000/music/batch/active_users_ts")

print("Analytics hoàn tất. Kết quả đã lưu vào HDFS /music/batch/")
spark.stop()
