"""
01_eda.py — EDA với Spark: phân tích phân phối events âm nhạc từ HDFS.
Đọc raw parquet từ /music/raw/, tính các thống kê cơ bản, lưu batch views.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Music EDA") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .config("spark.sql.shuffle.partitions", "16") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("hdfs://namenode:9000/music/raw/")
df = df.filter(F.col("page") == "NextSong")
print(f"Total NextSong events: {df.count():,}")
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
overview.write.mode("overwrite").parquet("hdfs://namenode:9000/music/batch/overview")

# ── 2. Top songs ──────────────────────────────────────────────
top_songs = df.groupBy("song", "artist").agg(
    F.count("*").alias("play_count"),
    F.countDistinct("userId").alias("unique_listeners"),
    F.round(F.avg("duration"), 1).alias("avg_duration_sec"),
).orderBy(F.desc("play_count"))

top_songs.show(20, truncate=False)
top_songs.write.mode("overwrite").parquet("hdfs://namenode:9000/music/batch/top_songs")

# ── 3. Top artists ────────────────────────────────────────────
top_artists = df.groupBy("artist").agg(
    F.count("*").alias("play_count"),
    F.countDistinct("userId").alias("unique_listeners"),
    F.countDistinct("song").alias("unique_songs"),
).orderBy(F.desc("play_count"))

top_artists.show(20, truncate=False)
top_artists.write.mode("overwrite").parquet("hdfs://namenode:9000/music/batch/top_artists")

# ── 4. Plays by hour of day ───────────────────────────────────
plays_by_hour = df.withColumn(
    "hour_of_day",
    F.hour(F.to_timestamp(F.col("ts") / 1000))
).groupBy("hour_of_day").agg(
    F.count("*").alias("play_count"),
    F.countDistinct("userId").alias("active_users"),
).orderBy("hour_of_day")

plays_by_hour.show(24, truncate=False)
plays_by_hour.write.mode("overwrite").parquet("hdfs://namenode:9000/music/batch/plays_by_hour")

# ── 5. Free vs Paid ratio ─────────────────────────────────────
level_stats = df.groupBy("level").agg(
    F.count("*").alias("play_count"),
    F.countDistinct("userId").alias("user_count"),
    F.round(F.avg("duration"), 1).alias("avg_duration_sec"),
).orderBy(F.desc("play_count"))

level_stats.show(truncate=False)
level_stats.write.mode("overwrite").parquet("hdfs://namenode:9000/music/batch/level_stats")

# ── 6. Page type distribution ─────────────────────────────────
all_events = spark.read.parquet("hdfs://namenode:9000/music/raw/")
page_dist = all_events.groupBy("page").agg(
    F.count("*").alias("event_count"),
    F.countDistinct("userId").alias("unique_users"),
).orderBy(F.desc("event_count"))

page_dist.show(truncate=False)
page_dist.write.mode("overwrite").parquet("hdfs://namenode:9000/music/batch/page_dist")

# ── 7. Gender distribution ────────────────────────────────────
gender_stats = df.groupBy("gender").agg(
    F.count("*").alias("play_count"),
    F.countDistinct("userId").alias("user_count"),
    F.round(F.avg("duration"), 1).alias("avg_duration_sec"),
)
gender_stats.show(truncate=False)
gender_stats.write.mode("overwrite").parquet("hdfs://namenode:9000/music/batch/gender_stats")

print("EDA hoàn tất. Kết quả đã lưu vào HDFS /music/batch/")
spark.stop()
