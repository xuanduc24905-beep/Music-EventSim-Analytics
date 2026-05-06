"""
07_spark_benchmark.py — Apache Spark benchmark (3 tasks).

Reads the same JSON dataset that MapReduce used.
Merges Spark timings with MapReduce timings from /tmp/mr_results.json.
Writes final benchmark results to /data/benchmark_results.parquet.

Tasks:
  1. top_artists   — COUNT plays per artist, ORDER BY DESC
  2. plays_by_hour — COUNT plays per hour of day
  3. user_sessions — COUNT plays per userId
"""
import json
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS        = "hdfs://namenode:9000"
BENCH_INPUT = f"{HDFS}/music/benchmark/data.json"
MR_RESULT   = "/tmp/mr_results.json"
LOCAL_OUT   = "/data/benchmark_results.parquet"

spark = SparkSession.builder \
    .appName("Spark Benchmark") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory",         "4g") \
    .config("spark.executor.cores",          "4") \
    .config("spark.sql.shuffle.partitions",  "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ── Load data ─────────────────────────────────────────────────────
df = spark.read.json(f"{BENCH_INPUT}/").cache()
total = df.count()
print(f"Loaded {total:,} records from HDFS ({BENCH_INPUT})")


def timed_task(description, lazy_df_fn):
    """Materialise the DataFrame and return elapsed seconds."""
    t = time.perf_counter()
    lazy_df_fn().cache().count()
    elapsed = round(time.perf_counter() - t, 3)
    print(f"  Spark [{description}]: {elapsed}s")
    return elapsed


spark_times = {
    "top_artists": timed_task(
        "top_artists",
        lambda: df.groupBy("artist").count().orderBy(F.desc("count")),
    ),
    "plays_by_hour": timed_task(
        "plays_by_hour",
        lambda: df
            .withColumn("hour", F.hour(F.from_unixtime(F.col("ts") / 1000)))
            .groupBy("hour").count().orderBy("hour"),
    ),
    "user_sessions": timed_task(
        "user_sessions",
        lambda: df.groupBy("userId").count().orderBy(F.desc("count")),
    ),
}

# ── Read MapReduce results ────────────────────────────────────────
mr_tasks = []
if os.path.exists(MR_RESULT):
    with open(MR_RESULT) as f:
        mr_data = json.load(f)
    mr_tasks = mr_data.get("tasks", [])
    print(f"Loaded MapReduce results: {len(mr_tasks)} tasks")
else:
    print("[WARN] /tmp/mr_results.json not found — run run_mapreduce.py first")

# ── Build combined DataFrame ──────────────────────────────────────
TASK_LABELS = {
    "top_artists":   "Top Artists",
    "plays_by_hour": "Plays by Hour",
    "user_sessions": "User Sessions",
}

rows = []
for task_key, label in TASK_LABELS.items():
    sp_sec = spark_times.get(task_key, 0.0)
    # Spark row — no Map/Shuffle/Reduce breakdown (DAG model)
    rows.append(("Spark", label, sp_sec, 0.0, 0.0, sp_sec, total))

    mr = next((t for t in mr_tasks if t["task"] == task_key), None)
    if mr:
        rows.append((
            "MapReduce", label,
            float(mr["total_sec"]),
            float(mr["map_sec"]),
            float(mr["shuffle_sec"]),
            float(mr["reduce_sec"]),
            int(total),
        ))

schema = [
    "framework", "task",
    "total_sec", "map_sec", "shuffle_sec", "reduce_sec",
    "num_records",
]
result_df = spark.createDataFrame(rows, schema)
result_df.show(truncate=False)

# ── Write results ─────────────────────────────────────────────────
result_df.coalesce(1).write.mode("overwrite").parquet(LOCAL_OUT)
print(f"✓ Benchmark results saved → {LOCAL_OUT}")

spark.stop()
