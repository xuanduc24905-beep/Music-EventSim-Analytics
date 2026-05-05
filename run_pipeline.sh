#!/usr/bin/env bash
# ============================================================
#  run_pipeline.sh — Chạy batch pipeline (services phải up sẵn)
#  Yêu cầu: chạy start_services.sh trước
# ============================================================
set -e

echo "======================================================"
echo " Batch Pipeline — Music Streaming Analytics"
echo "======================================================"

# Tìm spark-submit path
SPARK="docker exec spark-master bash -c"
SPARK_BIN=$(docker exec spark-master bash -c "which spark-submit 2>/dev/null || find /usr/local -name spark-submit 2>/dev/null | head -1")
SPARK_SUBMIT="${SPARK_BIN} --master spark://spark-master:7077"

# ── Tạo thư mục HDFS nếu chưa có ────────────────────────────
docker exec namenode bash -c "
    hdfs dfs -mkdir -p /music/raw &&
    hdfs dfs -mkdir -p /music/streaming &&
    hdfs dfs -mkdir -p /music/batch
" 2>/dev/null || true

# ── Start Spark Streaming (background) ───────────────────────
echo ""
echo "[1/5] Start Spark Streaming job (Kafka → HDFS, background)..."
docker exec -d spark-master bash -c "
    ${SPARK_BIN} \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    /spark-jobs/03_spark_streaming.py \
    > /tmp/streaming.log 2>&1
"

echo "      Đợi parquet files xuất hiện trong HDFS (tối đa 300s)..."
WAITED=0
until docker exec namenode hdfs dfs -ls /music/raw/ 2>/dev/null | grep -q ".parquet"; do
    sleep 5
    WAITED=$((WAITED + 5))
    printf "      ... ${WAITED}s\r"
    if [ $WAITED -ge 300 ]; then
        echo ""
        echo "[ERROR] Timeout! Kiểm tra: docker exec spark-master cat /tmp/streaming.log"
        exit 1
    fi
done
echo "      [OK] Có data sau ${WAITED}s"

# ── Batch jobs ────────────────────────────────────────────────
echo ""
echo "[2/5] Spark EDA..."
$SPARK "$SPARK_SUBMIT /spark-jobs/01_eda.py"
echo "      [OK]"

echo ""
echo "[3/5] Spark Analytics..."
$SPARK "$SPARK_SUBMIT /spark-jobs/02_analytics.py"
echo "      [OK]"

echo ""
echo "[4/5] Hive Load..."
$SPARK "$SPARK_SUBMIT /spark-jobs/04_hive_load.py"
echo "      [OK]"

echo ""
echo "[5/5] Export → /data/*.parquet..."
$SPARK "$SPARK_SUBMIT /spark-jobs/05_export.py"
echo "      [OK]"

echo ""
echo "======================================================"
echo " PIPELINE HOÀN TẤT!"
echo " Streamlit: http://localhost:8501"
echo "======================================================"
