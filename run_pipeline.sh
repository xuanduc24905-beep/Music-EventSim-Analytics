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
BATCH="batch"
SPEED="speed"
SERVING="serving"

echo "======================================================"
echo " Lambda Architecture Pipeline"
echo "======================================================"

# ── Tạo thư mục HDFS ──────────────────────────────────────
docker exec namenode bash -c "
    hdfs dfs -mkdir -p /music/raw
    hdfs dfs -mkdir -p /music/streaming
    hdfs dfs -mkdir -p /music/batch/clean
" 2>/dev/null || true

# ══════════════════════════════════════════════════════════
# SPEED LAYER — Kafka → HDFS (chạy nền, độc lập)
# ══════════════════════════════════════════════════════════
echo ""
echo "[SPEED] Kiểm tra Kafka..."
if docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
    echo "[SPEED] Kafka OK — start streaming job (background)..."
    docker exec spark-master bash -c "pkill -f 03_spark_streaming || true" 2>/dev/null || true
    sleep 2
    docker exec -d spark-master bash -c "
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /spark-jobs/speed/03_spark_streaming.py \
        > /tmp/streaming.log 2>&1
    "
    echo "[SPEED] Streaming job started → /tmp/streaming.log"
    KAFKA_OK=true
else
    echo "[SPEED] Kafka không sẵn sàng — bỏ qua speed layer"
    KAFKA_OK=false
fi

# ══════════════════════════════════════════════════════════
# BATCH LAYER — đọc HDFS /music/raw/ (không cần Kafka)
# ══════════════════════════════════════════════════════════
echo ""
echo "[BATCH] Kiểm tra data trong HDFS..."
PARQUET_COUNT=$(docker exec namenode bash -c "hdfs dfs -ls /music/raw/ 2>/dev/null | grep -c '.parquet' || echo 0" 2>/dev/null || echo 0)

if [ "$PARQUET_COUNT" -eq 0 ]; then
    if [ "$KAFKA_OK" = "false" ]; then
        echo ""
        echo "[ERROR] HDFS chưa có data VÀ Kafka không sẵn sàng."
        echo "        Đây là lần chạy đầu tiên — cần Kafka để sinh data."
        echo "        Chạy: bash start_services.sh  rồi thử lại."
        exit 1
    fi
    echo "[BATCH] HDFS /music/raw/ chưa có data."
    echo "        Đợi streaming job ghi data lần đầu (tối đa 180s)..."
    WAITED=0
    until docker exec namenode hdfs dfs -ls /music/raw/ 2>/dev/null | grep -q ".parquet"; do
        sleep 5
        WAITED=$((WAITED + 5))
        printf "        ... ${WAITED}s\r"
        if [ $WAITED -ge 180 ]; then
            echo ""
            echo "[ERROR] Timeout 180s. Xem log: docker exec spark-master cat /tmp/streaming.log"
            exit 1
        fi
    done
    echo ""
    echo "[BATCH] Data xuất hiện sau ${WAITED}s, tiếp tục..."
else
    echo "[BATCH] Có ${PARQUET_COUNT} parquet files — chạy batch ngay"
fi

# ── Dừng streaming job để giải phóng Spark executors cho batch ──
echo ""
echo "[BATCH] Tạm dừng streaming job để giải phóng resources..."
docker exec spark-master bash -c "pkill -f 03_spark_streaming || true" 2>/dev/null || true
sleep 5

echo ""
echo "[1/7] Batch Processing (clean)..."
$SPARK_SUBMIT /spark-jobs/$BATCH/00_batch_processing.py
echo "      [OK]"

echo ""
echo "[2/7] Batch EDA..."
$SPARK_SUBMIT /spark-jobs/$BATCH/01_eda.py
echo "      [OK]"

echo ""
echo "[3/7] Batch Analytics..."
$SPARK_SUBMIT /spark-jobs/$BATCH/02_analytics.py
echo "      [OK]"

echo ""
echo "[4/7] KMeans User Segmentation..."
$SPARK_SUBMIT /spark-jobs/$BATCH/03_kmeans.py
echo "      [OK]"

echo ""
echo "[5/7] Churn Prediction..."
$SPARK_SUBMIT /spark-jobs/$BATCH/04_churn_prediction.py
echo "      [OK]"

echo ""
echo "[6/7] Hive Serving Layer..."
$SPARK_SUBMIT /spark-jobs/$SERVING/04_hive_load.py
echo "      [OK]"

echo ""
echo "[7/7] Export → /data/*.parquet (Streamlit)..."
$SPARK_SUBMIT /spark-jobs/$SERVING/05_export.py
echo "      [OK]"

# ── Khởi động lại streaming job sau khi batch xong ──────────────
if [ "$KAFKA_OK" = "true" ]; then
    echo ""
    echo "[SPEED] Khởi động lại streaming job..."
    docker exec -d spark-master bash -c "
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /spark-jobs/$SPEED/03_spark_streaming.py \
        > /tmp/streaming.log 2>&1
    "
    echo "[SPEED] Streaming job restarted → /tmp/streaming.log"
fi

echo ""
echo "======================================================"
echo " PIPELINE HOÀN TẤT!"
echo " Streamlit  : http://localhost:8501"
echo " Spark UI   : http://localhost:8080"
echo " HDFS UI    : http://localhost:9870"
echo ""
echo " Speed layer: docker exec spark-master cat /tmp/streaming.log"
echo "======================================================"
