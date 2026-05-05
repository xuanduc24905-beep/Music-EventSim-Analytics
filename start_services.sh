#!/usr/bin/env bash
# ============================================================
#  start_services.sh — Khởi động toàn bộ Docker services
#  Chạy 1 lần trước khi demo. Đợi tất cả healthy rồi thôi.
# ============================================================
set -e

echo "======================================================"
echo " Khởi động services — Music Streaming Analytics"
echo "======================================================"

echo ""
echo "[1/2] Dọn container cũ..."
docker compose down --remove-orphans 2>/dev/null || true
docker rm -f \
    namenode datanode1 datanode2 \
    resourcemanager nodemanager \
    spark-master spark-worker-1 spark-worker-2 \
    hive-postgres hive-metastore hive-server \
    zookeeper kafka eventsim streamlit \
    airflow-postgres airflow-webserver airflow-scheduler \
    2>/dev/null || true

echo ""
echo "[2/2] Start containers..."
docker compose up -d \
    namenode datanode1 datanode2 \
    resourcemanager nodemanager \
    spark-master spark-worker-1 spark-worker-2 \
    postgres hive-metastore hive-server \
    zookeeper kafka \
    streamlit \
    airflow-postgres airflow-webserver airflow-scheduler

echo ""
echo "      Đợi HDFS namenode..."
until docker exec namenode curl -sf http://localhost:9870 > /dev/null 2>&1; do
    printf "."; sleep 5
done
echo " [OK]"

echo "      Đợi Kafka..."
until docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do
    printf "."; sleep 5
done
echo " [OK]"

echo "      Đợi Spark master..."
until docker exec spark-master curl -sf http://localhost:8080 > /dev/null 2>&1; do
    printf "."; sleep 3
done
echo " [OK]"

echo "      Khởi động EventSim..."
docker compose up -d eventsim

echo ""
echo "======================================================"
echo " Services sẵn sàng!"
echo "======================================================"
echo ""
echo "  Streamlit   : http://localhost:8501"
echo "  Spark UI    : http://localhost:8080"
echo "  HDFS UI     : http://localhost:9870"
echo "  Airflow     : http://localhost:8083  (admin/admin)"
echo ""
echo "  Tiếp theo: bash run_pipeline.sh"
echo ""
