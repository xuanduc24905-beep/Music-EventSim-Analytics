#!/usr/bin/env bash
# =============================================================
#  BENCHMARK: Hadoop MapReduce vs Apache Spark
#
#  Tasks (same dataset, same 3 aggregations):
#    1. Top Artists   — count plays per artist
#    2. Plays by Hour — group by hour of day
#    3. User Sessions — count plays per user
#
#  MapReduce: Python simulation with real disk I/O between phases
#             Map → Sort/Shuffle (disk) → Reduce (disk)
#  Spark    : DataFrame API, in-memory DAG execution
# =============================================================
set -e

SPARK_SUBMIT="docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077"

echo "======================================================"
echo " MapReduce vs Apache Spark — Performance Benchmark"
echo "======================================================"

# ── Step 1: Export clean Parquet → JSON in HDFS ───────────────────
echo ""
echo "[1/4] Preparing benchmark data (clean Parquet → JSON in HDFS)..."
$SPARK_SUBMIT /spark-jobs/batch/06_prepare_benchmark.py
echo "      [OK]"

# ── Step 2: MapReduce simulation ──────────────────────────────────
echo ""
echo "[2/4] Running MapReduce simulation..."
echo "      (Map → Shuffle/Sort to disk → Reduce)"
docker cp mapreduce/run_mapreduce.py spark-master:/tmp/run_mapreduce.py
docker exec spark-master python3 /tmp/run_mapreduce.py
echo "      [OK]"

# ── Step 3: Spark benchmark ───────────────────────────────────────
echo ""
echo "[3/4] Running Spark benchmark (3 tasks, DataFrame API)..."
$SPARK_SUBMIT /spark-jobs/batch/07_spark_benchmark.py
echo "      [OK]"

echo ""
echo "======================================================"
echo " BENCHMARK COMPLETE"
echo " Results at: /data/benchmark_results.parquet"
echo " Dashboard : http://localhost:8501  →  System Benchmarks"
echo " Spark UI  : http://localhost:8080"
echo "======================================================"
