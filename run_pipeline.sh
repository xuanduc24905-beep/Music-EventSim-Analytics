#!/usr/bin/env bash
# ============================================================
#  run_pipeline.sh — Lambda Architecture: Music Streaming Analytics
#  Chạy tuần tự 1 lệnh duy nhất: bash run_pipeline.sh
# ============================================================
set -e

SPARK="docker exec spark-master bash -c"

echo "======================================================"
echo " Lambda Architecture — Music Streaming Analytics"
echo "======================================================"

# ── Step 0: Dọn container cũ ─────────────────────────────────
echo ""
echo "[0/7] Dọn container cũ (tránh lỗi name conflict)..."
docker compose down --remove-orphans 2>/dev/null || true
docker rm -f \
    namenode datanode1 datanode2 \
    resourcemanager nodemanager \
    spark-master spark-worker-1 spark-worker-2 \
    hive-postgres hive-metastore hive-server \
    zookeeper kafka eventsim streamlit \
    airflow-postgres airflow-webserver airflow-scheduler \
    2>/dev/null || true

# ── Step 1: Khởi động services ───────────────────────────────
echo ""
echo "[1/7] Khởi động Docker services..."
docker compose up -d \
    namenode datanode1 datanode2 \
    resourcemanager nodemanager \
    spark-master spark-worker-1 spark-worker-2 \
    postgres hive-metastore hive-server \
    zookeeper kafka \
    streamlit \
    airflow-postgres airflow-webserver airflow-scheduler

echo "      Đợi HDFS + Kafka sẵn sàng (60s)..."
sleep 60

# Tìm spark-submit path sau khi container đã up
SPARK_BIN=$(docker exec spark-master bash -c "which spark-submit 2>/dev/null || find /usr/local -name spark-submit 2>/dev/null | head -1")
SPARK_SUBMIT="${SPARK_BIN} --master spark://spark-master:7077"
echo "      spark-submit: ${SPARK_BIN}"

# ── Step 2: Khởi động EventSim ───────────────────────────────
echo ""
echo "[2/7] Khởi động EventSim → Kafka..."
docker compose up -d eventsim
echo "      Đợi EventSim gửi events (20s)..."
sleep 20

# ── Step 3: Tạo thư mục HDFS ─────────────────────────────────
echo ""
echo "[3/7] Tạo thư mục HDFS..."
docker exec namenode bash -c "
    hdfs dfs -mkdir -p /music/raw &&
    hdfs dfs -mkdir -p /music/streaming &&
    hdfs dfs -mkdir -p /music/batch
"
echo "      [OK]"

# ── Step 4: Start Spark Streaming job (background) ───────────
echo ""
echo "[4/7] Start Spark Streaming job (Kafka → HDFS, chạy background)..."
docker exec -d spark-master bash -c "
    $SPARK_SUBMIT \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    /spark-jobs/03_spark_streaming.py \
    > /tmp/streaming.log 2>&1
"
echo "      Streaming job đang chạy background..."
echo "      Đợi parquet files xuất hiện trong HDFS (tối đa 120s)..."

# Chờ đến khi có file parquet thật sự trong /music/raw/
WAITED=0
until docker exec namenode hdfs dfs -ls /music/raw/ 2>/dev/null | grep -q ".parquet"; do
    sleep 5
    WAITED=$((WAITED + 5))
    echo "      ... ${WAITED}s — chờ streaming ghi data..."
    if [ $WAITED -ge 300 ]; then
        echo "[ERROR] Timeout! Streaming chưa ghi được data."
        echo "        Kiểm tra logs: docker exec spark-master cat /tmp/streaming.log"
        exit 1
    fi
done
echo "      [OK] Đã có parquet data trong /music/raw/ (sau ${WAITED}s)"

# ── Step 5: Batch Layer ───────────────────────────────────────
echo ""
echo "[5/7] Batch Layer — Spark EDA (01_eda.py)..."
$SPARK "$SPARK_SUBMIT /spark-jobs/01_eda.py"
echo "      [OK]"

echo ""
echo "      Batch Layer — Spark Analytics (02_analytics.py)..."
$SPARK "$SPARK_SUBMIT /spark-jobs/02_analytics.py"
echo "      [OK]"

# ── Step 6: Hive ─────────────────────────────────────────────
echo ""
echo "[6/7] Hive Load + Serving Layer (04_hive_load.py)..."
$SPARK "$SPARK_SUBMIT /spark-jobs/04_hive_load.py"
echo "      [OK]"

# ── Step 7: Export ────────────────────────────────────────────
echo ""
echo "[7/7] Export → /data/*.parquet (05_export.py)..."
$SPARK "$SPARK_SUBMIT /spark-jobs/05_export.py"
echo "      [OK]"

echo ""
echo "======================================================"
echo " PIPELINE HOÀN TẤT!"
echo "======================================================"
echo ""
echo "  Streamlit Dashboard : http://localhost:8501"
echo "  Spark Master UI     : http://localhost:8080"
echo "  HDFS NameNode UI    : http://localhost:9870"
echo "  YARN ResourceMgr    : http://localhost:8088"
echo "  Airflow             : http://localhost:8083  (admin/admin)"
echo "  HiveServer2 UI      : http://localhost:10002"
echo ""
echo "  Streaming job đang chạy ngầm (EventSim → Kafka → HDFS)"
echo "  Xem logs : docker exec spark-master cat /tmp/streaming.log"
echo "  Dừng tất cả: docker compose down"
echo ""
