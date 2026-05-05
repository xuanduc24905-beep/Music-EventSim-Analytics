"""
01_eda.py — Batch EDA: phân tích phân phối events âm nhạc.
Đọc /music/batch/clean/ (đã filter + dedup bởi 00_batch_processing.py).
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS       = "hdfs://namenode:9000"
CLEAN_PATH = f"{HDFS}/music/batch/clean"
BATCH_PATH = f"{HDFS}/music/batch"

spark = SparkSession.builder \
    .appName("Music EDA") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "4") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(CLEAN_PATH)
print(f"Clean events: {df.count():,}")
df.printSchema()

# ── 1. Thống kê tổng quan ─────────────────────────────────────
overview = df.agg(
    F.count("*").alias("total_plays"),
    F.countDistinct("userId").alias("unique_users"),
    F.countDistinct("artist").alias("unique_artists"),
    F.countDistinct("song").alias("unique_songs"),
    F.countDistinct("sessionId").alias("total_sessions"),
    F.round(F.avg("duration"), 1).alias("avg_duration_sec"),
    F.round(F.min("duration"), 1).alias("min_duration_sec"),
    F.round(F.max("duration"), 1).alias("max_duration_sec"),
)
overview.show(truncate=False)
overview.write.mode("overwrite").parquet(f"{BATCH_PATH}/overview")

# ── 2. Top songs ──────────────────────────────────────────────
top_songs = df.groupBy("song", "artist").agg(
    F.count("*").alias("play_count"),
    F.countDistinct("userId").alias("unique_listeners"),
    F.round(F.avg("duration"), 1).alias("avg_duration_sec"),
).orderBy(F.desc("play_count"))
top_songs.show(20, truncate=False)
top_songs.write.mode("overwrite").parquet(f"{BATCH_PATH}/top_songs")

# ── 3. Top artists ────────────────────────────────────────────
top_artists = df.groupBy("artist").agg(
    F.count("*").alias("play_count"),
    F.countDistinct("userId").alias("unique_listeners"),
    F.countDistinct("song").alias("unique_songs"),
).orderBy(F.desc("play_count"))
top_artists.show(20, truncate=False)
top_artists.write.mode("overwrite").parquet(f"{BATCH_PATH}/top_artists")

# ── 4. Top locations (by state) ──────────────────────────────
top_locations = df.withColumn(
    "state", F.trim(F.element_at(F.split(F.col("location"), ","), -1))
).groupBy("state").agg(
    F.count("*").alias("play_count"),
    F.countDistinct("userId").alias("unique_users"),
).orderBy(F.desc("play_count")).limit(15)
top_locations.show(15, truncate=False)
top_locations.write.mode("overwrite").parquet(f"{BATCH_PATH}/top_locations")

# ── 5. Plays by hour of day ───────────────────────────────────
plays_by_hour = df.withColumn(
    "hour_of_day", F.hour(F.col("event_ts"))
).groupBy("hour_of_day").agg(
    F.count("*").alias("play_count"),
    F.countDistinct("userId").alias("active_users"),
).orderBy("hour_of_day")
plays_by_hour.show(24, truncate=False)
plays_by_hour.write.mode("overwrite").parquet(f"{BATCH_PATH}/plays_by_hour")

# ── 5. Free vs Paid ratio ─────────────────────────────────────
level_stats = df.groupBy("level").agg(
    F.count("*").alias("play_count"),
    F.countDistinct("userId").alias("user_count"),
    F.round(F.avg("duration"), 1).alias("avg_duration_sec"),
).orderBy(F.desc("play_count"))
level_stats.show(truncate=False)
level_stats.write.mode("overwrite").parquet(f"{BATCH_PATH}/level_stats")

# ── 6. Page type distribution (từ raw để có đủ event types) ───
page_dist = spark.read.parquet(f"{HDFS}/music/raw/") \
    .groupBy("page").agg(
        F.count("*").alias("event_count"),
        F.countDistinct("userId").alias("unique_users"),
    ).orderBy(F.desc("event_count"))
page_dist.show(truncate=False)
page_dist.write.mode("overwrite").parquet(f"{BATCH_PATH}/page_dist")

# ── 7. Gender distribution ────────────────────────────────────
gender_stats = df.groupBy("gender").agg(
    F.count("*").alias("play_count"),
    F.countDistinct("userId").alias("user_count"),
    F.round(F.avg("duration"), 1).alias("avg_duration_sec"),
)
gender_stats.show(truncate=False)
gender_stats.write.mode("overwrite").parquet(f"{BATCH_PATH}/gender_stats")

print("EDA hoàn tất → HDFS /music/batch/")
spark.stop()
