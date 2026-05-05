# Music Streaming Analytics — Lambda Architecture

> **Big Data pipeline** giả lập hệ thống analytics của một music streaming service (Spotify-like), xây dựng trên kiến trúc Lambda với EventSim làm data source thay vì file CSV tĩnh.

---

## Architecture Overview

```
EventSim container
(generate JSON events → push vào Kafka liên tục)
        │
        ▼
Kafka topic: music-events
        │
        ├─────────────────────────────────────┐
        ▼                                     ▼
  BATCH LAYER                           SPEED LAYER
  HDFS /music/raw/                 03_spark_streaming.py
        │                          (Kafka → HDFS append)
  01_eda.py                               │
  02_analytics.py                   HDFS /music/streaming/
  (top artists, peak hours,               │
   free vs paid, retention)               │
        │                                 │
  04_hive_load.py                         │
  (Hive tables)                           │
        │                                 │
  05_export.py                            │
  (HDFS → /data/*.parquet)                │
        │                                 │
        └──────────────┬──────────────────┘
                       ▼
               SERVING LAYER
         Hive Metastore (PostgreSQL)
         merged_events VIEW (batch UNION stream)
                       │
                       ▼
          Streamlit Dashboard :8501
```

---

## Stack

| Component | Technology |
|---|---|
| Event Simulator | Python (EventSim — custom) |
| Message Queue | Apache Kafka 7.4.0 + Zookeeper |
| Distributed Storage | Hadoop HDFS 3.2.1 (1 namenode + 2 datanodes) |
| Resource Manager | YARN (resourcemanager + nodemanager) |
| Batch Processing | Apache Spark 3.5.0 (1 master + 2 workers) |
| Stream Processing | Spark Structured Streaming |
| Metadata Store | Apache Hive 4.0.0 + PostgreSQL 15 |
| Orchestration | Apache Airflow 2.8.1 |
| Dashboard | Streamlit 1.32.0 + Plotly |

---

## Project Structure

```
Music-EventSim-Analytics/
├── docker-compose.yml          # 14 services, toàn bộ stack
├── run_pipeline.sh             # One-command pipeline runner
├── conf/
│   ├── hive-site.xml           # Hive metastore config (PostgreSQL backend)
│   ├── hive-queries.sql        # DDL cho music domain
│   └── postgresql-42.7.3.jar  # JDBC driver
├── eventsim/
│   ├── Dockerfile
│   └── simulator.py            # Python event generator → Kafka
├── spark-jobs/
│   ├── 01_eda.py               # EDA: overview stats, top songs/artists
│   ├── 02_analytics.py         # Analytics: peak hours, retention, session dist
│   ├── 03_spark_streaming.py   # Kafka → HDFS (batch raw + speed layer)
│   ├── 04_hive_load.py         # Hive tables + merged serving view
│   └── 05_export.py            # HDFS → /data/*.parquet cho Streamlit
├── dags/
│   └── music_batch_dag.py      # Airflow DAG (@hourly)
└── streamlit-app/
    ├── app.py                  # Dashboard (5 charts + live feed)
    ├── requirements.txt
    └── Dockerfile
```

---

## Event Schema

EventSim generate JSON events theo schema sau, chỉ `page = "NextSong"` được xử lý cho music analytics:

```json
{
  "artist":        "Taylor Swift",
  "song":          "Anti-Hero",
  "duration":      197.5,
  "ts":            1746432000000,
  "userId":        42,
  "sessionId":     8821,
  "page":          "NextSong",
  "level":         "paid",
  "location":      "New York, NY",
  "userAgent":     "Mozilla/5.0 ...",
  "gender":        "F",
  "firstName":     "Jane",
  "lastName":      "Doe",
  "registration":  1700000000000,
  "itemInSession": 3,
  "status":        200,
  "method":        "PUT"
}
```

---

## Quick Start

### Prerequisites

- Docker Desktop với ít nhất **16GB RAM** allocated
- Docker Compose v2+

### Run

```bash
git clone https://github.com/xuanduc24905-beep/Music-EventSim-Analytics.git
cd Music-EventSim-Analytics
bash run_pipeline.sh
```

Script tự động:
1. **Dọn container cũ** (`docker compose down`) để tránh lỗi name conflict
2. Khởi động toàn bộ 14 Docker services
3. Start EventSim → push events vào Kafka
4. Chạy Spark Streaming (Kafka → HDFS) trong background
5. Đợi 60s để tích đủ data
6. Chạy batch pipeline: EDA → Analytics → Hive → Export
7. Streamlit dashboard sẵn sàng

### Stop

```bash
docker compose down
```

Thêm `--volumes` nếu muốn xóa luôn data HDFS/Postgres:

```bash
docker compose down --volumes
```

### Access UIs

| Service | URL | Credentials |
|---|---|---|
| Streamlit Dashboard | http://localhost:8501 | — |
| Spark Master | http://localhost:8080 | — |
| HDFS NameNode | http://localhost:9870 | — |
| YARN ResourceManager | http://localhost:8088 | — |
| Airflow | http://localhost:8083 | admin / admin |
| HiveServer2 Web | http://localhost:10002 | — |

---

## Lambda Architecture — Chi tiết

### Batch Layer

Xử lý toàn bộ historical data trong `/music/raw/`, đảm bảo tính **chính xác tuyệt đối**:

| Spark Job | Input | Output (HDFS /music/batch/) |
|---|---|---|
| `01_eda.py` | `/music/raw/` | overview, top_songs, top_artists, plays_by_hour, level_stats, gender_stats |
| `02_analytics.py` | `/music/raw/` | top10_songs, top10_artists, plays_by_hour, level_ratio, session_dist, retention, active_users_ts |
| `04_hive_load.py` | HDFS batch paths | Hive tables + merged_events VIEW |
| `05_export.py` | HDFS batch paths | `/data/*.parquet` cho Streamlit |

### Speed Layer

Xử lý data mới nhất **chưa được batch pipeline đọc**, giảm độ trễ xuống vài giây:

- `03_spark_streaming.py` đọc Kafka → filter `NextSong` → ghi vào `/music/streaming/` mỗi **2 giây**
- Đồng thời ghi raw events vào `/music/raw/` để batch layer tích lũy dữ liệu

### Serving Layer

Hive `merged_events` VIEW = `UNION ALL` của batch table và streaming table:

```sql
SELECT ..., 'batch' AS data_source FROM music.play_events
UNION ALL
SELECT ..., 'stream' AS data_source FROM music.streaming_events
```

`05_export.py` merge kết quả:
```
merged_plays = batch_play_count + stream_play_count
```

---

## Streamlit Dashboard

5 charts chính + live feed:

| Chart | Mô tả |
|---|---|
| **Top 10 Songs** | Horizontal bar — bài được nghe nhiều nhất (merged) |
| **Top 10 Artists** | Horizontal bar — nghệ sĩ phổ biến nhất (merged) |
| **Plays by Hour** | Line chart — peak listening hours trong ngày |
| **Free vs Paid** | Donut pie — tỷ lệ user tier |
| **Session Distribution** | Histogram — số bài nghe mỗi session |
| **Active Users Over Time** | Area chart — timeline user active |
| **User Retention** | Stacked bar — new vs returning users theo ngày |
| **Live Feed** | Card list — events mới nhất từ Kafka stream |

Auto-refresh **30 giây**. Sidebar hiển thị trạng thái 3 Lambda layers (Batch / Speed / Serving).

---

## Airflow DAG

DAG `music_batch_pipeline` chạy **@hourly**, tự động:

```
wait_for_hdfs_data
        │
        ▼
   spark_eda (01_eda.py)
        │
        ▼
spark_analytics (02_analytics.py)
        │
        ▼
  hive_load (04_hive_load.py)
        │
        ▼
export_parquet (05_export.py)
```

---

## EventSim

Thay vì dùng image `seatgeek/eventsim` (khó pull), project dùng Python simulator tự viết:

- **1000 users** với profile cố định (gender, level free/paid, location)
- **40 artists** × catalog bài hát riêng mỗi artist
- **Page weights**: 75% NextSong, 8% Home, 4% Login, ...
- Session reset ngẫu nhiên (~5% mỗi event)
- Tốc độ mặc định: **10 events/giây** (cấu hình qua env `EVENTS_PER_SECOND`)

```bash
# Tùy chỉnh tốc độ và số user
EVENTS_PER_SECOND=50 NUM_USERS=5000 docker compose up eventsim
```

---

## Configuration

### docker-compose.yml — Resource

| Service | Memory | Cores |
|---|---|---|
| spark-worker-1 | 24GB | 10 |
| spark-worker-2 | 24GB | 10 |

Máy ít RAM có thể giảm `SPARK_WORKER_MEMORY` xuống `8G`.

### Streaming trigger

Mặc định 2 giây. Tăng lên nếu cần giảm tải:

```python
# 03_spark_streaming.py
.trigger(processingTime="10 seconds")
```

---

## Troubleshooting

**Lỗi container name conflict khi chạy `docker compose up`:**
```
Error: Conflict. The container name "/namenode" is already in use
```
```bash
# Cách 1 — dùng run_pipeline.sh (đã tích hợp sẵn bước dọn container)
bash run_pipeline.sh

# Cách 2 — dọn thủ công rồi start lại
docker compose down --remove-orphans
docker compose up -d

# Cách 3 — nếu container đến từ project khác
docker rm -f namenode datanode1 datanode2 resourcemanager nodemanager \
  spark-master spark-worker-1 spark-worker-2 \
  hive-postgres hive-metastore hive-server \
  zookeeper kafka eventsim streamlit \
  airflow-postgres airflow-webserver airflow-scheduler
docker compose up -d
```

---

**EventSim không connect được Kafka:**
```bash
docker logs eventsim
# Kafka cần ~30s để sẵn sàng sau khi start, eventsim có retry logic tự động
```

**Spark job báo lỗi `/music/raw/ not found`:**
```bash
# Streaming chưa ghi đủ data, chờ thêm hoặc kiểm tra
docker exec namenode hdfs dfs -ls /music/raw/
```

**Hive metastore lỗi connection:**
```bash
# Đợi postgres healthy trước
docker compose ps hive-postgres
```

**Xem logs streaming job:**
```bash
docker exec spark-master tail -f /tmp/streaming.log
```

---

## License

MIT
