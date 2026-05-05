"""
02_analytics.py — Batch Analytics: tính các metrics chính.
Đọc /music/batch/clean/ (đã filter + dedup bởi 00_batch_processing.py).
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

HDFS       = "hdfs://namenode:9000"
CLEAN_PATH = f"{HDFS}/music/batch/clean"
BATCH_PATH = f"{HDFS}/music/batch"

spark = SparkSession.builder \
    .appName("Music Analytics") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "4") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(CLEAN_PATH)
print(f"Clean events: {df.count():,}")

# ── 1. Top 10 songs ───────────────────────────────────────────
top10_songs = df.groupBy("song", "artist").agg(
    F.count("*").alias("play_count"),
    F.countDistinct("userId").alias("unique_listeners"),
    F.round(F.avg("duration"), 1).alias("avg_duration_sec"),
).orderBy(F.desc("play_count")).limit(10)
top10_songs.show(truncate=False)
top10_songs.write.mode("overwrite").parquet(f"{BATCH_PATH}/top10_songs")

# ── 2. Top 10 artists ─────────────────────────────────────────
top10_artists = df.groupBy("artist").agg(
    F.count("*").alias("play_count"),
    F.countDistinct("userId").alias("unique_listeners"),
    F.countDistinct("song").alias("unique_songs"),
).orderBy(F.desc("play_count")).limit(10)
top10_artists.show(truncate=False)
top10_artists.write.mode("overwrite").parquet(f"{BATCH_PATH}/top10_artists")

# ── 3. Plays by hour of day ───────────────────────────────────
plays_by_hour = df.withColumn(
    "hour_of_day", F.hour(F.col("event_ts"))
).groupBy("hour_of_day").agg(
    F.count("*").alias("play_count"),
    F.countDistinct("userId").alias("active_users"),
    F.round(F.avg("duration"), 1).alias("avg_duration_sec"),
).orderBy("hour_of_day")
plays_by_hour.show(24, truncate=False)
plays_by_hour.write.mode("overwrite").parquet(f"{BATCH_PATH}/plays_by_hour")

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
level_ratio.write.mode("overwrite").parquet(f"{BATCH_PATH}/level_ratio")

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
session_dist.write.mode("overwrite").parquet(f"{BATCH_PATH}/session_dist")
session_stats.write.mode("overwrite").parquet(f"{BATCH_PATH}/session_stats")

# ── 6. User retention (new vs returning) ─────────────────────
df_day = df.withColumn("play_date", F.to_date(F.col("event_ts")))
first_play = df_day.groupBy("userId").agg(F.min("play_date").alias("first_play_date"))
retention = df_day.join(first_play, on="userId") \
    .withColumn(
        "user_type",
        F.when(F.col("play_date") == F.col("first_play_date"), "new").otherwise("returning")
    ).groupBy("play_date", "user_type").agg(
        F.countDistinct("userId").alias("user_count"),
        F.count("*").alias("play_count"),
    ).orderBy("play_date", "user_type")
retention.show(20, truncate=False)
retention.write.mode("overwrite").parquet(f"{BATCH_PATH}/retention")

# ── 7. Active users over time (hourly) ───────────────────────
active_users_ts = df.withColumn(
    "hour_bucket", F.date_trunc("hour", F.col("event_ts"))
).groupBy("hour_bucket").agg(
    F.countDistinct("userId").alias("active_users"),
    F.count("*").alias("total_plays"),
).orderBy("hour_bucket")
active_users_ts.show(20, truncate=False)
active_users_ts.write.mode("overwrite").parquet(f"{BATCH_PATH}/active_users_ts")

print("Analytics hoàn tất → HDFS /music/batch/")
spark.stop()
