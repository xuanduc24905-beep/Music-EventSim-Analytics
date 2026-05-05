# Music Streaming Analytics — Lambda Architecture

> End-to-end **Big Data pipeline** mô phỏng hệ thống analytics của một nền tảng music streaming (Spotify-like).  
> Dữ liệu được sinh real-time bởi **EventSim**, xử lý song song qua kiến trúc **Lambda** (Batch + Speed + Serving), và trực quan hoá trên **BI Dashboard** phục vụ Marketing & Product teams.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA INGESTION                              │
│                                                                     │
│   EventSim  ──►  Kafka (music-events)  ──►  HDFS /music/raw/       │
│   (1000 users · ~10 events/s)                                       │
└────────────────────────────┬────────────────────────────────────────┘
                             │
              ┌──────────────┴──────────────┐
              ▼                             ▼
   ┌──────────────────┐         ┌──────────────────────┐
   │   BATCH LAYER    │         │    SPEED LAYER       │
   │                  │         │                      │
   │ 00_batch_proc.py │         │ 03_spark_streaming.py│
   │ (clean + dedup)  │         │ Kafka → HDFS/stream  │
   │        ↓         │         │  trigger: 2s         │
   │ 01_eda.py        │         └──────────┬───────────┘
   │ 02_analytics.py  │                    │
   │ 03_kmeans.py     │         ┌──────────▼───────────┐
   │ 04_churn_pred.py │         │  HDFS /music/        │
   │                  │         │  streaming/          │
   │ HDFS /music/     │         └──────────────────────┘
   │ batch/           │                    │
   └────────┬─────────┘                    │
            │                              │
            └──────────────┬───────────────┘
                           ▼
              ┌────────────────────────┐
              │     SERVING LAYER      │
              │                        │
              │  04_hive_load.py       │
              │  (Hive + merged VIEW)  │
              │         ↓              │
              │  05_export.py          │
              │  (HDFS → /data/*.pq)   │
              └───────────┬────────────┘
                          ▼
              ┌────────────────────────┐
              │   Streamlit Dashboard  │
              │   :8501  (dark BI UI)  │
              └────────────────────────┘
```

---

## Tech Stack

| Layer | Technology | Version |
|---|---|---|
| Event Simulator | Python (custom EventSim) | — |
| Message Queue | Apache Kafka + Zookeeper | 7.4.0 |
| Distributed Storage | Hadoop HDFS (1 NN + 2 DN) | 3.2.1 |
| Resource Manager | YARN | 3.2.1 |
| Batch / Stream Processing | Apache Spark | 4.x |
| ML / Analytics | Spark MLlib (KMeans, Random Forest) | — |
| Metadata Store | Apache Hive + PostgreSQL | 4.0.0 / 15 |
| Orchestration | Apache Airflow | 2.8.1 |
| Dashboard | Streamlit + Plotly | 1.35.0 |
| Containerisation | Docker Compose | v2 |

---

## Project Structure

```
Music-EventSim-Analytics/
│
├── docker-compose.yml            # 14+ services
├── run_pipeline.sh               # One-command pipeline runner
├── start_services.sh             # One-time infra startup
│
├── eventsim/
│   ├── Dockerfile
│   └── simulator.py              # Event generator → Kafka
│
├── spark-jobs/
│   ├── batch/
│   │   ├── 00_batch_processing.py  # Raw ingestion → clean (filter + dedup)
│   │   ├── 01_eda.py               # EDA: overview, top songs/artists, locations
│   │   ├── 02_analytics.py         # Analytics: retention, session dist, active users
│   │   ├── 03_kmeans.py            # ML: KMeans user segmentation (4 clusters)
│   │   └── 04_churn_prediction.py  # ML: Random Forest churn prediction
│   │
│   ├── speed/
│   │   └── 03_spark_streaming.py   # Kafka → HDFS /music/streaming/ (2s trigger)
│   │
│   └── serving/
│       ├── 04_hive_load.py         # Hive tables + merged_events VIEW
│       └── 05_export.py            # HDFS → /data/*.parquet for Streamlit
│
├── spark/
│   └── Dockerfile                  # Spark image + numpy/MLlib dependencies
│
├── streamlit-app/
│   ├── app.py                      # Single-page BI dashboard
│   ├── requirements.txt
│   └── Dockerfile
│
├── conf/
│   ├── hive-site.xml
│   └── hive-queries.sql
│
└── dags/
    └── music_batch_dag.py          # Airflow DAG (@hourly)
```

---

## Quick Start

### Requirements

- Docker Desktop ≥ v24 với **Docker Compose v2**
- RAM: **≥ 16 GB** allocated cho Docker (khuyến nghị 32–48 GB)
- WSL2 (Windows): xem [WSL Memory Config](#wsl2-memory-config)

### 1. Clone & Start Services

```bash
git clone https://github.com/XuanDuc/Music-EventSim-Analytics.git
cd Music-EventSim-Analytics

# Khởi động toàn bộ infrastructure (chỉ cần chạy 1 lần)
bash start_services.sh
```

`start_services.sh` thực hiện:
- Khởi động 14 Docker containers
- Chờ HDFS, Kafka, Spark sẵn sàng (health checks)
- Start EventSim → push events liên tục vào Kafka
- Start Spark Streaming job (Kafka → HDFS) trong background

### 2. Run Analytics Pipeline

```bash
bash run_pipeline.sh
```

Pipeline thực hiện **7 bước** tuần tự:

| Bước | Job | Mô tả |
|---|---|---|
| 1/7 | `00_batch_processing.py` | Đọc `/music/raw/` → filter NextSong → dedup → `/music/batch/clean/` |
| 2/7 | `01_eda.py` | EDA: overview stats, top songs/artists, peak hours, locations |
| 3/7 | `02_analytics.py` | Analytics: retention, session distribution, active users timeline |
| 4/7 | `03_kmeans.py` | KMeans clustering → 4 user segments |
| 5/7 | `04_churn_prediction.py` | Random Forest → churn probability per user |
| 6/7 | `04_hive_load.py` | Load Hive tables + `merged_events` VIEW (batch ∪ stream) |
| 7/7 | `05_export.py` | Export `/data/*.parquet` cho Streamlit |

> Streaming job tự động **dừng** trước batch để giải phóng Spark executors, và **khởi động lại** sau khi pipeline hoàn tất.

### 3. Access Dashboard

| Service | URL | Credentials |
|---|---|---|
| **Streamlit Dashboard** | http://localhost:8501 | — |
| Spark Master UI | http://localhost:8080 | — |
| HDFS NameNode UI | http://localhost:9870 | — |
| YARN ResourceManager | http://localhost:8088 | — |
| Airflow | http://localhost:8083 | admin / admin |
| HiveServer2 | http://localhost:10002 | — |

### Stop

```bash
docker compose down                 # Giữ volumes (data HDFS/Postgres)
docker compose down --volumes       # Xoá toàn bộ data
```

---

## Lambda Architecture — Chi tiết

### Batch Layer

Xử lý **toàn bộ historical data** trong HDFS, đảm bảo tính chính xác tuyệt đối.

**Data flow:**
```
/music/raw/  →  00_batch_processing  →  /music/batch/clean/
                                               │
                    ┌──────────────────────────┤
                    ▼                          ▼
              01_eda.py                  02_analytics.py
         (EDA & distributions)       (business metrics)
                    │                          │
                    └──────────┬───────────────┘
                               ▼
                    /music/batch/{overview, top_songs,
                     top_artists, plays_by_hour,
                     retention, session_dist,
                     level_ratio, gender_stats,
                     top_locations, user_segments,
                     churn_predictions, ...}
```

**`00_batch_processing.py`** — Single source of truth cho toàn bộ batch layer:
- Đọc raw events với schema tường minh
- Filter `page == "NextSong"`
- Dedup theo `(userId, sessionId, ts)`
- Cast `event_ts` từ epoch milliseconds

### Speed Layer

Giảm độ trễ xuống **dưới 10 giây** cho data mới nhất.

`03_spark_streaming.py`:
- Subscribe Kafka topic `music-events`
- Parse JSON → filter NextSong
- Ghi append vào `HDFS /music/streaming/` mỗi **2 giây**
- Streamlit đọc trực tiếp qua WebHDFS API (không cần batch)

### Serving Layer

Merge batch + speed thành unified view:

```sql
-- Hive: merged_events VIEW
SELECT ..., 'batch' AS data_source FROM music.play_events
UNION ALL
SELECT ..., 'stream' AS data_source FROM music.streaming_events
```

`05_export.py` tính thêm:
```
merged_plays = batch_play_count + stream_play_count
```

---

## ML Pipeline

### KMeans User Segmentation (`03_kmeans.py`)

Features: `total_plays`, `unique_artists`, `unique_songs`, `total_sessions`, `avg_duration`, `active_days`, `level`

Pipeline: `StringIndexer → VectorAssembler → StandardScaler → KMeans(k=4)`

Output segments:

| Segment | Đặc điểm |
|---|---|
| **Power Users** | Plays cao, nhiều artists, nhiều ngày active |
| **Regular Listeners** | Engagement trung bình, ổn định |
| **Casual Listeners** | Ít plays, ngắn session |
| **At-Risk Users** | Engagement thấp, dấu hiệu rời bỏ |

### Churn Prediction (`04_churn_prediction.py`)

Label: engagement-based — bottom quartile `plays_per_day` × `active_days` = churned (1)

Features: `total_plays`, `unique_artists`, `unique_songs`, `total_sessions`, `avg_duration`, `active_days`, `days_span`, `plays_per_day`, `level`

Model: `RandomForestClassifier(numTrees=100, maxDepth=6)`

Output: `churn_probability` [0–1] + `risk_level` (High / Medium / Low)

---

## Dashboard

Single-page dark BI dashboard (Spotify aesthetic), không có tabs — tất cả trên một màn hình scroll:

```
┌────────────────────────────────────────────────────┐
│  Header: Title + Live badge + Last sync            │
├──────────┬──────────┬──────────┬──────────────────┤
│  Total   │   MAU    │  Churn   │   Top Artist     │  ← KPI Cards
│  Streams │          │  Rate    │                  │
├──────────────────────────────┬─────────────────────┤
│  Hourly Traffic Peaks        │  Free vs Paid       │  ← Row 2
│  (Batch vs Real-time area)   │  (Donut chart)      │
├────────────────────┬─────────┴─────────────────────┤
│  Customer          │  Churn Gauge (Indicator)      │  ← Row 3
│  Segmentation      │  + At-Risk Premium Users list │
│  (KMeans Scatter)  │                               │
├────────────────────┴──────────────┬────────────────┤
│  Top Locations (H-Bar)            │ Gender Split   │  ← Row 4
└───────────────────────────────────┴────────────────┘
│  Live Ticker — bài đang nghe real-time (3s refresh)│
└────────────────────────────────────────────────────┘
```

**Live Ticker** dùng `@st.experimental_fragment(run_every=3)` — chỉ refresh phần live feed, không reload cả trang.

---

## Event Schema

EventSim sinh JSON events theo schema:

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

Chỉ events có `page == "NextSong"` được đưa vào analytics pipeline.

**EventSim config:**
- 1,000 users với profile cố định (gender, level, location)
- 40 artists × catalog bài hát riêng
- Page weights: 75% NextSong, 8% Home, 4% Login, ...
- Session reset ngẫu nhiên ~5%/event
- Default: **10 events/giây**

```bash
# Tuỳ chỉnh tốc độ
EVENTS_PER_SECOND=50 docker compose up eventsim -d
```

---

## Airflow DAG

DAG `music_batch_pipeline` chạy **@hourly**, tự động trigger toàn bộ batch pipeline:

```
wait_for_hdfs_data
        │
        ▼
batch_processing (00)
        │
        ▼
spark_eda (01)  ──►  spark_analytics (02)
                              │
                    ┌─────────┴─────────┐
                    ▼                   ▼
             kmeans (03)       churn_prediction (04)
                    └─────────┬─────────┘
                              ▼
                       hive_load (04_serving)
                              │
                              ▼
                      export_parquet (05)
```

---

## WSL2 Memory Config

Trên Windows, WSL2 mặc định chỉ lấy 50% RAM hoặc 8GB. Tạo/chỉnh file `C:\Users\<username>\.wslconfig`:

```ini
[wsl2]
memory=48GB
processors=16
swap=8GB
```

Sau đó restart WSL:
```powershell
wsl --shutdown
```

Khuyến nghị cho máy 64GB: `memory=52GB` (giữ ~12GB cho Windows).

---

## Resource Configuration

| Service | Memory | Cores |
|---|---|---|
| spark-worker-1 | 24GB | 10 |
| spark-worker-2 | 24GB | 10 |

Máy RAM thấp hơn — chỉnh trong `docker-compose.yml`:
```yaml
SPARK_WORKER_MEMORY: 8g
SPARK_WORKER_CORES: 4
```

---

## Troubleshooting

**Spark job báo "Initial job has not accepted any resources":**
```bash
# Streaming job đang chiếm executors — kill và chạy lại pipeline
docker exec spark-master bash -c "pkill -f 03_spark_streaming || true"
bash run_pipeline.sh
```

**HDFS chưa có data:**
```bash
docker exec namenode hdfs dfs -ls /music/raw/
docker logs eventsim | tail -20
```

**MLlib lỗi `No module named 'numpy'`:**
```bash
# Rebuild Spark image
docker compose up -d --build --no-deps spark-master spark-worker-1 spark-worker-2
```

**Container name conflict:**
```bash
docker compose down --remove-orphans
docker compose up -d
```

**Xem log streaming:**
```bash
docker exec spark-master tail -f /tmp/streaming.log
```

**Hive metastore lỗi connection:**
```bash
# Postgres cần healthy trước
docker compose ps hive-postgres
```

---

## License

MIT
