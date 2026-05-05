#!/usr/bin/env bash
# ============================================================
#  run_pipeline.sh — Lambda Architecture Pipeline
#
#  BATCH LAYER  : độc lập với Kafka, đọc HDFS /music/raw/
#  SPEED LAYER  : Kafka → Spark Streaming → HDFS /music/streaming/
#  SERVING LAYER: merge batch + speed → /data/*.parquet → Streamlit
#
#  Kafka/Streaming chết → batch vẫn chạy bình thường trên data cũ
# ============================================================
set -e

SPARK_SUBMIT="docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077"

echo "======================================================"
echo " Lambda Architecture Pipeline"
echo "======================================================"

# ── Tạo thư mục HDFS ──────────────────────────────────────
docker exec namenode bash -c "
    hdfs dfs -mkdir -p /music/raw
    hdfs dfs -mkdir -p /music/streaming
    hdfs dfs -mkdir -p /music/batch
" 2>/dev/null || true

# ══════════════════════════════════════════════════════════
# SPEED LAYER — Kafka → HDFS (chạy nền, độc lập)
# ══════════════════════════════════════════════════════════
echo ""
echo "[SPEED] Kiểm tra Kafka..."
if docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
    echo "[SPEED] Kafka OK — start streaming job (background)..."
    # Kill job cũ nếu còn chạy
    docker exec spark-master bash -c "pkill -f 03_spark_streaming || true" 2>/dev/null || true
    sleep 2
    docker exec -d spark-master bash -c "
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /spark-jobs/03_spark_streaming.py \
        > /tmp/streaming.log 2>&1
    "
    echo "[SPEED] Streaming job started → /tmp/streaming.log"
else
    echo "[SPEED] Kafka không sẵn sàng — bỏ qua speed layer, batch vẫn chạy bình thường"
fi

# ══════════════════════════════════════════════════════════
# BATCH LAYER — đọc HDFS /music/raw/ (không cần Kafka)
# ══════════════════════════════════════════════════════════
echo ""
echo "[BATCH] Kiểm tra data trong HDFS..."
PARQUET_COUNT=$(docker exec namenode bash -c "hdfs dfs -ls /music/raw/ 2>/dev/null | grep -c '.parquet' || echo 0" 2>/dev/null || echo 0)

if [ "$PARQUET_COUNT" -eq 0 ]; then
    echo "[BATCH] HDFS /music/raw/ chưa có data."
    echo "        Đợi streaming job ghi data lần đầu (tối đa 120s)..."
    WAITED=0
    until docker exec namenode hdfs dfs -ls /music/raw/ 2>/dev/null | grep -q ".parquet"; do
        sleep 5
        WAITED=$((WAITED + 5))
        printf "        ... ${WAITED}s\r"
        if [ $WAITED -ge 120 ]; then
            echo ""
            echo "[BATCH] Không có data sau 120s."
            echo "        Kiểm tra streaming: docker exec spark-master cat /tmp/streaming.log"
            exit 1
        fi
    done
    echo ""
    echo "[BATCH] Data xuất hiện sau ${WAITED}s, tiếp tục..."
else
    echo "[BATCH] Có ${PARQUET_COUNT} parquet files — chạy batch ngay"
fi

echo ""
echo "[1/4] Batch EDA..."
$SPARK_SUBMIT /spark-jobs/01_eda.py
echo "      [OK]"

echo ""
echo "[2/4] Batch Analytics..."
$SPARK_SUBMIT /spark-jobs/02_analytics.py
echo "      [OK]"

echo ""
echo "[3/4] Hive Serving Layer..."
$SPARK_SUBMIT /spark-jobs/04_hive_load.py
echo "      [OK]"

echo ""
echo "[4/4] Export → /data/*.parquet (Streamlit)..."
$SPARK_SUBMIT /spark-jobs/05_export.py
echo "      [OK]"

echo ""
echo "======================================================"
echo " PIPELINE HOÀN TẤT!"
echo " Streamlit  : http://localhost:8501"
echo " Spark UI   : http://localhost:8080"
echo " HDFS UI    : http://localhost:9870"
echo ""
echo " Speed layer: docker exec spark-master cat /tmp/streaming.log"
echo "======================================================"
