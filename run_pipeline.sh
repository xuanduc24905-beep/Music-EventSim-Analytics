#!/usr/bin/env bash
# ============================================================
#  run_pipeline.sh — Lambda Architecture: Music Streaming Analytics
#
#  Cách dùng:
#    Terminal 1: bash run_pipeline.sh          → batch pipeline
#    Terminal 2: bash run_pipeline.sh stream   → chạy streaming job
#
#  Thứ tự đúng:
#    1. Terminal 1: bash run_pipeline.sh       (khởi động services)
#    2. Terminal 2: bash run_pipeline.sh stream (start Kafka → HDFS)
#    3. Đợi ~60s có data
#    4. Terminal 1: bash run_pipeline.sh batch  (chạy EDA → export)
# ============================================================
set -e

SPARK="docker exec spark-master bash -c"
SPARK_SUBMIT="spark-submit --master spark://spark-master:7077"

# ── Chế độ stream: chạy riêng terminal 2 ────────────────────
if [[ "$1" == "stream" ]]; then
    echo "======================================================"
    echo " SPEED LAYER — Spark Streaming (Kafka → HDFS)"
    echo " Ctrl+C để dừng"
    echo "======================================================"
    docker exec spark-master bash -c \
        "$SPARK_SUBMIT \
         --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
         /spark-jobs/03_spark_streaming.py"
    exit 0
fi

# ── Chế độ batch: chạy pipeline sau khi đã có data ──────────
if [[ "$1" == "batch" ]]; then
    echo "======================================================"
    echo " BATCH LAYER — Spark Jobs"
    echo "======================================================"

    echo ""
    echo "[1/4] Spark EDA (01_eda.py)..."
    $SPARK "$SPARK_SUBMIT /spark-jobs/01_eda.py"
    echo "      [OK]"

    echo ""
    echo "[2/4] Spark Analytics (02_analytics.py)..."
    $SPARK "$SPARK_SUBMIT /spark-jobs/02_analytics.py"
    echo "      [OK]"

    echo ""
    echo "[3/4] Hive Load (04_hive_load.py)..."
    $SPARK "$SPARK_SUBMIT /spark-jobs/04_hive_load.py"
    echo "      [OK]"

    echo ""
    echo "[4/4] Export → /data/*.parquet (05_export.py)..."
    $SPARK "$SPARK_SUBMIT /spark-jobs/05_export.py"
    echo "      [OK]"

    echo ""
    echo "======================================================"
    echo " BATCH PIPELINE HOÀN TẤT!"
    echo " Streamlit: http://localhost:8501"
    echo "======================================================"
    exit 0
fi

# ── Mặc định: khởi động toàn bộ services ────────────────────
echo "======================================================"
echo " Lambda Architecture — Music Streaming Analytics"
echo "======================================================"

echo ""
echo "[0/1] Dọn container cũ (tránh lỗi name conflict)..."
docker compose down --remove-orphans 2>/dev/null || true

echo "      Khởi động Docker services..."
docker compose up -d \
    namenode datanode1 datanode2 \
    resourcemanager nodemanager \
    spark-master spark-worker-1 spark-worker-2 \
    postgres hive-metastore hive-server \
    zookeeper kafka \
    streamlit \
    airflow-postgres airflow-webserver airflow-scheduler

echo ""
echo "      Đợi services sẵn sàng (60s)..."
sleep 60

echo ""
echo "      Khởi động EventSim..."
docker compose up -d eventsim

echo ""
echo "======================================================"
echo " Services đã sẵn sàng!"
echo "======================================================"
echo ""
echo " Tiếp theo — mở 2 terminal:"
echo ""
echo "   Terminal 2 (Speed Layer — chạy trước, để chạy liên tục):"
echo "   bash run_pipeline.sh stream"
echo ""
echo "   Terminal 1 (Batch Layer — sau khi stream chạy ~60s):"
echo "   bash run_pipeline.sh batch"
echo ""
echo " UIs:"
echo "   Streamlit   : http://localhost:8501"
echo "   Spark UI    : http://localhost:8080"
echo "   HDFS UI     : http://localhost:9870"
echo "   Airflow     : http://localhost:8083  (admin/admin)"
echo ""
echo " Dừng tất cả: docker compose down"
echo ""
