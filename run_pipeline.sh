#!/usr/bin/env bash
# ============================================================
#  run_pipeline.sh — Lambda Architecture: Music Streaming Analytics
#  EventSim → Kafka → Spark (Batch + Streaming) → Hive → Streamlit
# ============================================================
set -e

SPARK="docker exec spark-master bash -c"
SPARK_SUBMIT="spark-submit --master spark://spark-master:7077"

echo "======================================================"
echo " Lambda Architecture — Music Streaming Analytics"
echo "======================================================"

# ── Step 0: Dọn container cũ + Khởi động services ───────────
echo ""
echo "[0/6] Dọn container cũ (tránh lỗi name conflict)..."
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

echo "      Đợi HDFS + Kafka sẵn sàng (60s)..."
sleep 60

# ── Step 1: Khởi động EventSim ───────────────────────────────
echo ""
echo "[1/6] Khởi động EventSim (music event generator → Kafka)..."
docker compose up -d eventsim
echo "      [OK] EventSim đang generate events → topic: music-events"
echo "      Đợi EventSim gửi events (30s)..."
sleep 30

# ── Step 2: Dump batch data từ Kafka vào HDFS ────────────────
echo ""
echo "[2/6] Khởi động Spark Streaming (Kafka → HDFS /music/raw via batch dump)..."
echo "      Gom dữ liệu từ Kafka 60s rồi lưu HDFS /music/raw/..."

docker exec namenode bash -c "hdfs dfs -mkdir -p /music/raw /music/streaming /music/batch"

# Chạy streaming job ngầm (background) để ghi vào HDFS liên tục
docker exec -d spark-master bash -c \
  "$SPARK_SUBMIT \
   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
   /spark-jobs/03_spark_streaming.py" \
  || echo "[WARN] Streaming job already running or failed — tiếp tục..."

echo "      Streaming job chạy background. Đợi 60s để có đủ data..."
sleep 60

# ── Step 3: Spark EDA ─────────────────────────────────────────
echo ""
echo "[3/6] Spark EDA (01_eda.py)..."
$SPARK "$SPARK_SUBMIT /spark-jobs/01_eda.py"
echo "      [OK] EDA stats → HDFS /music/batch/"

# ── Step 4: Analytics ─────────────────────────────────────────
echo ""
echo "[4/6] Spark Analytics (02_analytics.py)..."
$SPARK "$SPARK_SUBMIT /spark-jobs/02_analytics.py"
echo "      [OK] top songs, artists, retention → HDFS /music/batch/"

# ── Step 5: Hive ──────────────────────────────────────────────
echo ""
echo "[5/6] Hive Load + Serving Layer (04_hive_load.py)..."
$SPARK "$SPARK_SUBMIT /spark-jobs/04_hive_load.py"
echo "      [OK] Hive tables + merged views đã tạo"

# ── Step 6: Export ────────────────────────────────────────────
echo ""
echo "[6/6] Export Serving Layer → /data/*.parquet (05_export.py)..."
$SPARK "$SPARK_SUBMIT /spark-jobs/05_export.py"
echo "      [OK] /data/*.parquet sẵn sàng cho Streamlit"

echo ""
echo "======================================================"
echo " PIPELINE HOÀN TẤT!"
echo "======================================================"
echo ""
echo " Streamlit Dashboard : http://localhost:8501"
echo " Spark Master UI     : http://localhost:8080"
echo " HDFS NameNode UI    : http://localhost:9870"
echo " YARN ResourceMgr    : http://localhost:8088"
echo " Airflow             : http://localhost:8083  (admin/admin)"
echo " HiveServer2 UI      : http://localhost:10002"
echo ""
echo " EventSim logs       : docker logs -f eventsim"
echo " Streaming logs      : docker exec spark-master tail -f /tmp/streaming.log"
echo ""
echo " Để dừng tất cả: docker compose down"
echo ""
