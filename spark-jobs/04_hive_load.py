"""
04_hive_load.py — Serving Layer (Hive):
  - Batch views: play_events, top_songs, top_artists, plays_by_hour, level_stats
  - Speed view : streaming_events (EXTERNAL table → HDFS streaming path)
  - Merged view: merged_events (UNION batch + stream) → merged analytics
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Music Hive Load") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "4") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── Database ───────────────────────────────────────────────────
spark.sql("CREATE DATABASE IF NOT EXISTS music")
spark.sql("USE music")

# ══════════════════════════════════════════════════════════════
# BATCH LAYER — Batch Views
# ══════════════════════════════════════════════════════════════

# ── 1. play_events (batch raw parquet) ───────────────────────
spark.sql("DROP TABLE IF EXISTS music.play_events")
spark.sql("""
    CREATE EXTERNAL TABLE music.play_events (
        artist         STRING,
        song           STRING,
        duration       DOUBLE,
        ts             BIGINT,
        userId         INT,
        sessionId      INT,
        page           STRING,
        level          STRING,
        location       STRING,
        userAgent      STRING,
        gender         STRING,
        firstName      STRING,
        lastName       STRING,
        registration   BIGINT,
        itemInSession  INT,
        status         INT,
        method         STRING
    )
    STORED AS PARQUET
    LOCATION 'hdfs://namenode:9000/music/raw'
""")

batch_total = spark.sql("SELECT COUNT(*) AS cnt FROM music.play_events WHERE page = 'NextSong'").collect()[0]["cnt"]
print(f"Batch play_events (NextSong): {batch_total:,}")

# ── 2. top_songs ──────────────────────────────────────────────
spark.sql("DROP TABLE IF EXISTS music.top_songs")
spark.sql("""
    CREATE TABLE music.top_songs AS
    SELECT
        song,
        artist,
        COUNT(*) AS play_count,
        COUNT(DISTINCT userId) AS unique_listeners,
        ROUND(AVG(duration), 1) AS avg_duration_sec
    FROM music.play_events
    WHERE page = 'NextSong'
    GROUP BY song, artist
    ORDER BY play_count DESC
    LIMIT 100
""")

# ── 3. top_artists ────────────────────────────────────────────
spark.sql("DROP TABLE IF EXISTS music.top_artists")
spark.sql("""
    CREATE TABLE music.top_artists AS
    SELECT
        artist,
        COUNT(*) AS play_count,
        COUNT(DISTINCT userId) AS unique_listeners,
        COUNT(DISTINCT song) AS unique_songs
    FROM music.play_events
    WHERE page = 'NextSong'
    GROUP BY artist
    ORDER BY play_count DESC
    LIMIT 100
""")

# ── 4. plays_by_hour ──────────────────────────────────────────
spark.sql("DROP TABLE IF EXISTS music.plays_by_hour")
spark.sql("""
    CREATE TABLE music.plays_by_hour AS
    SELECT
        HOUR(FROM_UNIXTIME(CAST(ts / 1000 AS BIGINT))) AS hour_of_day,
        COUNT(*) AS play_count,
        COUNT(DISTINCT userId) AS active_users,
        ROUND(AVG(duration), 1) AS avg_duration_sec
    FROM music.play_events
    WHERE page = 'NextSong'
    GROUP BY HOUR(FROM_UNIXTIME(CAST(ts / 1000 AS BIGINT)))
    ORDER BY hour_of_day
""")

# ── 5. level_stats ────────────────────────────────────────────
spark.sql("DROP TABLE IF EXISTS music.level_stats")
spark.sql("""
    CREATE TABLE music.level_stats AS
    SELECT
        level,
        COUNT(*) AS play_count,
        COUNT(DISTINCT userId) AS user_count,
        ROUND(AVG(duration), 1) AS avg_duration_sec,
        ROUND(SUM(duration) / 3600, 2) AS total_hours_played
    FROM music.play_events
    WHERE page = 'NextSong'
    GROUP BY level
""")

print("Batch views OK: play_events, top_songs, top_artists, plays_by_hour, level_stats")

# ══════════════════════════════════════════════════════════════
# SPEED LAYER — Streaming table (External → HDFS speed path)
# ══════════════════════════════════════════════════════════════
try:
    spark.sql("DROP TABLE IF EXISTS music.streaming_events")
    spark.sql("""
        CREATE EXTERNAL TABLE music.streaming_events (
            artist          STRING,
            song            STRING,
            duration        DOUBLE,
            ts              BIGINT,
            userId          INT,
            sessionId       INT,
            page            STRING,
            level           STRING,
            location        STRING,
            userAgent       STRING,
            gender          STRING,
            firstName       STRING,
            lastName        STRING,
            registration    BIGINT,
            itemInSession   INT,
            status          INT,
            method          STRING,
            event_ts        TIMESTAMP,
            kafka_timestamp TIMESTAMP,
            ingestion_time  TIMESTAMP
        )
        STORED AS PARQUET
        LOCATION 'hdfs://namenode:9000/music/streaming'
    """)
    stream_total = spark.sql("SELECT COUNT(*) AS cnt FROM music.streaming_events").collect()[0]["cnt"]
    print(f"Speed layer — streaming_events: {stream_total:,} records")

    # ══════════════════════════════════════════════════════════
    # SERVING LAYER — Merged view (Batch UNION Speed)
    # ══════════════════════════════════════════════════════════
    spark.sql("DROP VIEW IF EXISTS music.merged_events")
    spark.sql("""
        CREATE VIEW music.merged_events AS
        SELECT
            artist, song, duration, ts, userId, sessionId,
            page, level, location, userAgent, gender,
            firstName, lastName, registration, itemInSession,
            status, method, 'batch' AS data_source
        FROM music.play_events
        WHERE page = 'NextSong'
        UNION ALL
        SELECT
            artist, song, duration, ts, userId, sessionId,
            page, level, location, userAgent, gender,
            firstName, lastName, registration, itemInSession,
            status, method, 'stream' AS data_source
        FROM music.streaming_events
    """)

    # Merged top songs (Serving Layer)
    spark.sql("DROP TABLE IF EXISTS music.merged_top_songs")
    spark.sql("""
        CREATE TABLE music.merged_top_songs AS
        SELECT
            song, artist, data_source,
            COUNT(*) AS play_count,
            COUNT(DISTINCT userId) AS unique_listeners
        FROM music.merged_events
        GROUP BY song, artist, data_source
        ORDER BY play_count DESC
        LIMIT 200
    """)

    # Merged top artists (Serving Layer)
    spark.sql("DROP TABLE IF EXISTS music.merged_top_artists")
    spark.sql("""
        CREATE TABLE music.merged_top_artists AS
        SELECT
            artist, data_source,
            COUNT(*) AS play_count,
            COUNT(DISTINCT userId) AS unique_listeners
        FROM music.merged_events
        GROUP BY artist, data_source
        ORDER BY play_count DESC
        LIMIT 200
    """)

    print("Serving layer — merged_events view, merged_top_songs, merged_top_artists OK")
    spark.sql("""
        SELECT data_source, COUNT(*) AS records
        FROM music.merged_events
        GROUP BY data_source
    """).show()

except Exception as e:
    print(f"[WARN] Speed/Serving layer bị bỏ qua (chưa có streaming data): {e}")

# ── Summary ───────────────────────────────────────────────────
print("\n=== Top 10 Songs ===")
spark.sql("SELECT * FROM music.top_songs LIMIT 10").show(truncate=False)

print("\n=== Top 10 Artists ===")
spark.sql("SELECT * FROM music.top_artists LIMIT 10").show(truncate=False)

print("\n=== Level Stats ===")
spark.sql("SELECT * FROM music.level_stats").show(truncate=False)

print("\nHive Serving Layer hoàn tất.")
spark.stop()
