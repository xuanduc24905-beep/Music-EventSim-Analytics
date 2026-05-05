"""
05_export.py — Export Serving Layer ra /data/*.parquet cho Streamlit.

Thứ tự:
  1. Export batch views từ HDFS
  2. Tính speed views từ streaming HDFS records
  3. Merge batch + speed → merged views (Serving Layer)
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Music Export") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ══════════════════════════════════════════════════════════════
# 1. BATCH VIEWS — export từ HDFS về /data/
# ══════════════════════════════════════════════════════════════
BATCH_EXPORTS = {
    "hdfs://namenode:9000/music/batch/overview":         "/data/overview.parquet",
    "hdfs://namenode:9000/music/batch/top10_songs":      "/data/top10_songs.parquet",
    "hdfs://namenode:9000/music/batch/top10_artists":    "/data/top10_artists.parquet",
    "hdfs://namenode:9000/music/batch/top_songs":        "/data/top_songs.parquet",
    "hdfs://namenode:9000/music/batch/top_artists":      "/data/top_artists.parquet",
    "hdfs://namenode:9000/music/batch/plays_by_hour":    "/data/plays_by_hour.parquet",
    "hdfs://namenode:9000/music/batch/level_ratio":      "/data/level_ratio.parquet",
    "hdfs://namenode:9000/music/batch/level_stats":      "/data/level_stats.parquet",
    "hdfs://namenode:9000/music/batch/session_dist":     "/data/session_dist.parquet",
    "hdfs://namenode:9000/music/batch/session_stats":    "/data/session_stats.parquet",
    "hdfs://namenode:9000/music/batch/retention":        "/data/retention.parquet",
    "hdfs://namenode:9000/music/batch/active_users_ts":  "/data/active_users_ts.parquet",
    "hdfs://namenode:9000/music/batch/gender_stats":     "/data/gender_stats.parquet",
    "hdfs://namenode:9000/music/batch/page_dist":        "/data/page_dist.parquet",
}

batch_total = 0
try:
    df_raw = spark.read.parquet("hdfs://namenode:9000/music/raw/") \
                  .filter(F.col("page") == "NextSong")
    batch_total = df_raw.count()
    print(f"Batch raw NextSong events: {batch_total:,}")
except Exception as e:
    print(f"[WARN] Không đọc được raw data: {e}")

for hdfs_path, local_path in BATCH_EXPORTS.items():
    try:
        df = spark.read.parquet(hdfs_path)
        df.write.mode("overwrite").parquet(local_path)
        print(f"[BATCH] {hdfs_path.split('/')[-1]}: {df.count()} rows → {local_path}")
    except Exception as e:
        print(f"[WARN]  Bỏ qua {hdfs_path.split('/')[-1]}: {e}")

# ══════════════════════════════════════════════════════════════
# 2. SPEED VIEWS — aggregate streaming records
# ══════════════════════════════════════════════════════════════
speed_total = 0
try:
    df_speed = spark.read.parquet("hdfs://namenode:9000/music/streaming/")
    speed_total = df_speed.count()
    print(f"\n[SPEED] Streaming records: {speed_total:,}")

    # Speed: top songs from stream
    speed_top_songs = df_speed.groupBy("song", "artist").agg(
        F.count("*").alias("stream_plays"),
        F.countDistinct("userId").alias("stream_listeners"),
    ).orderBy(F.desc("stream_plays"))
    speed_top_songs.write.mode("overwrite").parquet("/data/speed_top_songs.parquet")

    # Speed: top artists from stream
    speed_top_artists = df_speed.groupBy("artist").agg(
        F.count("*").alias("stream_plays"),
        F.countDistinct("userId").alias("stream_listeners"),
    ).orderBy(F.desc("stream_plays"))
    speed_top_artists.write.mode("overwrite").parquet("/data/speed_top_artists.parquet")

    # Speed: active users over time (hourly)
    speed_active_ts = df_speed \
        .withColumn("hour_bucket", F.date_trunc("hour", F.col("ingestion_time"))) \
        .groupBy("hour_bucket").agg(
            F.countDistinct("userId").alias("active_users"),
            F.count("*").alias("total_plays"),
        ).orderBy("hour_bucket")
    speed_active_ts.write.mode("overwrite").parquet("/data/speed_active_ts.parquet")

    # Speed: level split from stream
    speed_level = df_speed.groupBy("level").agg(
        F.count("*").alias("stream_plays"),
        F.countDistinct("userId").alias("stream_users"),
    )
    speed_level.write.mode("overwrite").parquet("/data/speed_level.parquet")

    # Speed summary metadata
    latest_ts = df_speed.agg(F.max("ingestion_time").alias("ts")).collect()[0]["ts"]
    spark.createDataFrame([{
        "stream_total":     int(speed_total),
        "batch_total":      int(batch_total),
        "latest_ingestion": str(latest_ts),
    }]).write.mode("overwrite").parquet("/data/speed_summary.parquet")

    print("[SPEED] speed_top_songs, speed_top_artists, speed_active_ts, speed_level, speed_summary OK")

    # ══════════════════════════════════════════════════════════
    # 3. SERVING LAYER — Merge batch + speed
    # ══════════════════════════════════════════════════════════
    # Merge top songs
    try:
        df_batch_songs = spark.read.parquet("/data/top_songs.parquet")
        merged_songs = df_batch_songs.join(
            speed_top_songs, on=["song", "artist"], how="full"
        ).fillna({"play_count": 0, "stream_plays": 0, "unique_listeners": 0, "stream_listeners": 0})

        merged_songs = merged_songs.withColumn(
            "merged_plays", F.col("play_count") + F.col("stream_plays")
        ).orderBy(F.desc("merged_plays"))
        merged_songs.write.mode("overwrite").parquet("/data/merged_top_songs.parquet")
    except Exception as e:
        print(f"[WARN] merged_top_songs: {e}")

    # Merge top artists
    try:
        df_batch_artists = spark.read.parquet("/data/top_artists.parquet")
        merged_artists = df_batch_artists.join(
            speed_top_artists, on="artist", how="full"
        ).fillna({"play_count": 0, "stream_plays": 0, "unique_listeners": 0, "stream_listeners": 0})

        merged_artists = merged_artists.withColumn(
            "merged_plays", F.col("play_count") + F.col("stream_plays")
        ).orderBy(F.desc("merged_plays"))
        merged_artists.write.mode("overwrite").parquet("/data/merged_top_artists.parquet")
    except Exception as e:
        print(f"[WARN] merged_top_artists: {e}")

    # Merge active users timeline
    try:
        df_batch_ts = spark.read.parquet("/data/active_users_ts.parquet")
        merged_ts = df_batch_ts.union(speed_active_ts).groupBy("hour_bucket").agg(
            F.sum("active_users").alias("active_users"),
            F.sum("total_plays").alias("total_plays"),
        ).orderBy("hour_bucket")
        merged_ts.write.mode("overwrite").parquet("/data/merged_active_ts.parquet")
    except Exception as e:
        print(f"[WARN] merged_active_ts: {e}")

    merged_total = batch_total + speed_total
    print(f"\n[SERVING] Merge hoàn tất:")
    print(f"  Batch   : {batch_total:,}")
    print(f"  Speed   : {speed_total:,}")
    print(f"  Merged  : {merged_total:,}")

except Exception as e:
    print(f"\n[WARN] Speed layer trống (chưa stream?): {e}")
    print("       Serving layer dùng batch-only.")
    spark.createDataFrame([{
        "stream_total":     0,
        "batch_total":      int(batch_total),
        "latest_ingestion": "N/A",
    }]).write.mode("overwrite").parquet("/data/speed_summary.parquet")

print("\nExport hoàn tất.")
spark.stop()
