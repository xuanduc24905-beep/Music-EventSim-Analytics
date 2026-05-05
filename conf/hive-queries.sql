-- ================================================================
--  hive-queries.sql — Music Streaming Analytics DDL & queries
--  Serving Layer: batch views + speed view + merged view
-- ================================================================

CREATE DATABASE IF NOT EXISTS music;
USE music;

-- ── Batch: raw play events ────────────────────────────────────
DROP TABLE IF EXISTS music.play_events;
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
    method         STRING,
    event_date     STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/music/raw';

-- ── Batch: top songs ──────────────────────────────────────────
DROP TABLE IF EXISTS music.top_songs;
CREATE TABLE music.top_songs AS
SELECT
    song,
    artist,
    COUNT(*) AS play_count,
    ROUND(AVG(duration), 1) AS avg_duration_sec
FROM music.play_events
WHERE page = 'NextSong'
GROUP BY song, artist
ORDER BY play_count DESC
LIMIT 100;

-- ── Batch: top artists ────────────────────────────────────────
DROP TABLE IF EXISTS music.top_artists;
CREATE TABLE music.top_artists AS
SELECT
    artist,
    COUNT(*) AS play_count,
    COUNT(DISTINCT userId) AS unique_listeners
FROM music.play_events
WHERE page = 'NextSong'
GROUP BY artist
ORDER BY play_count DESC
LIMIT 100;

-- ── Batch: plays by hour ──────────────────────────────────────
DROP TABLE IF EXISTS music.plays_by_hour;
CREATE TABLE music.plays_by_hour AS
SELECT
    HOUR(FROM_UNIXTIME(CAST(ts / 1000 AS BIGINT))) AS hour_of_day,
    COUNT(*) AS play_count,
    COUNT(DISTINCT userId) AS active_users
FROM music.play_events
WHERE page = 'NextSong'
GROUP BY HOUR(FROM_UNIXTIME(CAST(ts / 1000 AS BIGINT)))
ORDER BY hour_of_day;

-- ── Batch: free vs paid ───────────────────────────────────────
DROP TABLE IF EXISTS music.level_stats;
CREATE TABLE music.level_stats AS
SELECT
    level,
    COUNT(*) AS play_count,
    COUNT(DISTINCT userId) AS user_count,
    ROUND(AVG(duration), 1) AS avg_duration_sec
FROM music.play_events
WHERE page = 'NextSong'
GROUP BY level;

-- ── Speed: streaming events (external → HDFS streaming path) ─
DROP TABLE IF EXISTS music.streaming_events;
CREATE EXTERNAL TABLE music.streaming_events (
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
    method         STRING,
    kafka_timestamp TIMESTAMP,
    ingestion_time  TIMESTAMP
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/music/streaming';

-- ── Serving: merged view (batch + speed) ─────────────────────
DROP VIEW IF EXISTS music.merged_events;
CREATE VIEW music.merged_events AS
SELECT
    artist, song, duration, ts, userId, sessionId,
    page, level, location, userAgent, gender,
    firstName, lastName, registration, itemInSession,
    status, method, 'batch' AS data_source
FROM music.play_events
UNION ALL
SELECT
    artist, song, duration, ts, userId, sessionId,
    page, level, location, userAgent, gender,
    firstName, lastName, registration, itemInSession,
    status, method, 'stream' AS data_source
FROM music.streaming_events;
