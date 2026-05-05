import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pyarrow.parquet as pq
import requests
import io
import os
import time

st.set_page_config(
    page_title="Music Streaming Analytics",
    page_icon="🎵",
    layout="wide",
    initial_sidebar_state="expanded",
)

WEBHDFS = "http://namenode:9870/webhdfs/v1"

DATA_DIR = "/app/data"


# ── Load helpers ───────────────────────────────────────────────
@st.cache_data(ttl=30)
def load_parquet(filename: str) -> pd.DataFrame:
    for prefix in [DATA_DIR + "/", "data/", "../data/"]:
        p = prefix + filename
        if os.path.exists(p):
            try:
                return pd.read_parquet(p)
            except Exception:
                continue
    return pd.DataFrame()


@st.cache_data(ttl=5)
def load_streaming_live() -> pd.DataFrame:
    try:
        path = "/music/streaming"
        url  = f"{WEBHDFS}{path}?op=LISTSTATUS"
        resp = requests.get(url, timeout=3)
        resp.raise_for_status()
        files = resp.json()["FileStatuses"]["FileStatus"]
        parquet_files = [f for f in files if ".parquet" in f["pathSuffix"]]
        if not parquet_files:
            return pd.DataFrame()
        dfs = []
        for f in sorted(parquet_files, key=lambda x: x["modificationTime"], reverse=True)[:6]:
            file_url = f"{WEBHDFS}{path}/{f['pathSuffix']}?op=OPEN"
            r = requests.get(file_url, allow_redirects=True, timeout=5)
            dfs.append(pq.read_table(io.BytesIO(r.content)).to_pandas())
        return pd.concat(dfs, ignore_index=True)
    except Exception:
        return pd.DataFrame()


# ── Load datasets ─────────────────────────────────────────────
df_overview       = load_parquet("overview.parquet")
df_top10_songs    = load_parquet("top10_songs.parquet")
df_top10_artists  = load_parquet("top10_artists.parquet")
df_plays_by_hour  = load_parquet("plays_by_hour.parquet")
df_level_ratio    = load_parquet("level_ratio.parquet")
df_level_stats    = load_parquet("level_stats.parquet")
df_session_dist   = load_parquet("session_dist.parquet")
df_retention      = load_parquet("retention.parquet")
df_active_users   = load_parquet("active_users_ts.parquet")
df_gender         = load_parquet("gender_stats.parquet")
df_speed_summary  = load_parquet("speed_summary.parquet")

# Merged (Serving Layer = Batch + Speed)
df_merged_songs   = load_parquet("merged_top_songs.parquet")
df_merged_artists = load_parquet("merged_top_artists.parquet")
df_merged_active  = load_parquet("merged_active_ts.parquet")

# Speed-only views
df_speed_songs    = load_parquet("speed_top_songs.parquet")
df_speed_artists  = load_parquet("speed_top_artists.parquet")
df_speed_level    = load_parquet("speed_level.parquet")
df_speed_active   = load_parquet("speed_active_ts.parquet")

# Live WebHDFS feed
df_live = load_streaming_live()

# ── Layer status ──────────────────────────────────────────────
speed_total      = 0
batch_total      = 0
latest_ingestion = "N/A"
if not df_speed_summary.empty:
    r = df_speed_summary.iloc[0]
    speed_total      = int(r.get("stream_total", 0))
    batch_total      = int(r.get("batch_total",  0))
    latest_ingestion = str(r.get("latest_ingestion", "N/A"))

is_merged = not df_merged_songs.empty

# ── Pick best available source ────────────────────────────────
df_songs   = df_merged_songs   if not df_merged_songs.empty   else df_top10_songs
df_artists = df_merged_artists if not df_merged_artists.empty else df_top10_artists
df_active  = df_merged_active  if not df_merged_active.empty  else df_active_users
df_level   = df_level_ratio    if not df_level_ratio.empty    else df_level_stats

# ── Sidebar ────────────────────────────────────────────────────
with st.sidebar:
    st.title("🎵 Music Analytics")
    st.caption("Lambda Architecture · EventSim")
    st.divider()

    st.subheader("⚡ Lambda Layer Status")

    batch_ok   = batch_total > 0 or not df_top10_songs.empty
    speed_ok   = speed_total > 0
    serving_ok = is_merged

    st.write(f"{'✅' if batch_ok   else '❌'} **Batch Layer**")
    st.caption(f"  {batch_total:,} events · HDFS + Hive")

    st.write(f"{'✅' if speed_ok   else '🟡'} **Speed Layer**")
    st.caption(f"  {speed_total:,} stream events · Kafka → Spark")

    st.write(f"{'✅' if serving_ok else '🟡'} **Serving Layer**")
    if serving_ok:
        st.caption(f"  Merged {batch_total + speed_total:,} · Batch + Speed")
    else:
        st.caption("  Batch-only (chưa có stream)")

    st.divider()
    st.caption(f"Latest stream: {latest_ingestion[:19] if latest_ingestion != 'N/A' else 'N/A'}")

    auto_refresh = st.checkbox("Auto-refresh 30s", value=True)

# ── Auto-refresh ──────────────────────────────────────────────
if auto_refresh:
    time.sleep(0)
    st.markdown(
        '<meta http-equiv="refresh" content="30">',
        unsafe_allow_html=True,
    )

# ── Header ────────────────────────────────────────────────────
col_h1, col_h2 = st.columns([4, 1])
with col_h1:
    st.title("🎵 Music Streaming Analytics")
    st.caption("Lambda Architecture · EventSim → Kafka → Spark → HDFS → Hive → Streamlit")
with col_h2:
    if speed_ok:
        st.success("🟢 STREAM ACTIVE")
    elif not df_live.empty:
        st.success("🟢 LIVE FEED")
    else:
        st.warning("🟡 Batch-only")

st.divider()

# ── KPI Cards ─────────────────────────────────────────────────
k1, k2, k3, k4, k5 = st.columns(5)

total_plays   = batch_total + speed_total
unique_artists = 0
unique_songs   = 0
avg_duration   = 0.0
if not df_overview.empty:
    row = df_overview.iloc[0]
    unique_artists = int(row.get("unique_artists", 0))
    unique_songs   = int(row.get("unique_songs",   0))
    avg_duration   = float(row.get("avg_duration_sec", 0))

k1.metric("Total Plays (Batch + Speed)", f"{total_plays:,}",
          delta=f"+{speed_total:,} stream" if speed_total > 0 else None)
k2.metric("Unique Artists",  f"{unique_artists:,}")
k3.metric("Unique Songs",    f"{unique_songs:,}")
k4.metric("Avg Duration",    f"{avg_duration:.0f}s")
k5.metric("Live Feed",       f"+{len(df_live):,}" if not df_live.empty else f"+{speed_total:,}")

st.write("")

# ══════════════════════════════════════════════════════════════
# Row 1: Top 10 Songs  |  Top 10 Artists
# ══════════════════════════════════════════════════════════════
row1_c1, row1_c2 = st.columns(2)

with row1_c1:
    label = "Top 10 Songs" + (" ✦ Merged" if is_merged else " (Batch)")
    st.subheader(label)

    src = df_songs
    if src.empty:
        src = df_top10_songs

    if not src.empty:
        plays_col = next((c for c in ["merged_plays", "play_count", "stream_plays"] if c in src.columns), None)
        if plays_col:
            src_top = src.nlargest(10, plays_col).copy()
            src_top["label"] = src_top["song"].str[:30] + " — " + src_top["artist"].str[:20]
            fig = px.bar(
                src_top.sort_values(plays_col),
                x=plays_col, y="label",
                orientation="h",
                color=plays_col,
                color_continuous_scale="Viridis",
                labels={plays_col: "Plays", "label": ""},
            )
            fig.update_layout(coloraxis_showscale=False, height=380, margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Chưa có dữ liệu songs.")

with row1_c2:
    label2 = "Top 10 Artists" + (" ✦ Merged" if is_merged else " (Batch)")
    st.subheader(label2)

    src2 = df_artists
    if src2.empty:
        src2 = df_top10_artists

    if not src2.empty:
        plays_col2 = next((c for c in ["merged_plays", "play_count", "stream_plays"] if c in src2.columns), None)
        if plays_col2:
            src_top2 = src2.nlargest(10, plays_col2).copy()
            fig2 = px.bar(
                src_top2.sort_values(plays_col2),
                x=plays_col2, y="artist",
                orientation="h",
                color=plays_col2,
                color_continuous_scale="Plasma",
                labels={plays_col2: "Plays", "artist": ""},
            )
            fig2.update_layout(coloraxis_showscale=False, height=380, margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("Chưa có dữ liệu artists.")

st.divider()

# ══════════════════════════════════════════════════════════════
# Row 2: Plays by Hour  |  Free vs Paid
# ══════════════════════════════════════════════════════════════
row2_c1, row2_c2 = st.columns(2)

with row2_c1:
    st.subheader("Plays by Hour of Day")
    st.caption("Peak listening hours — Batch Layer")

    if not df_plays_by_hour.empty and "hour_of_day" in df_plays_by_hour.columns:
        fig_hour = px.line(
            df_plays_by_hour.sort_values("hour_of_day"),
            x="hour_of_day",
            y="play_count",
            markers=True,
            labels={"hour_of_day": "Hour (0–23)", "play_count": "Plays"},
            color_discrete_sequence=["#1db954"],
        )
        if "active_users" in df_plays_by_hour.columns:
            fig_hour.add_scatter(
                x=df_plays_by_hour.sort_values("hour_of_day")["hour_of_day"],
                y=df_plays_by_hour.sort_values("hour_of_day")["active_users"],
                mode="lines+markers",
                name="Active Users",
                yaxis="y2",
                line=dict(color="#ff6b6b", dash="dot"),
            )
            fig_hour.update_layout(
                yaxis2=dict(overlaying="y", side="right", title="Active Users"),
            )
        fig_hour.update_layout(xaxis=dict(dtick=1), height=350)
        st.plotly_chart(fig_hour, use_container_width=True)
    else:
        st.info("Chưa có dữ liệu theo giờ.")

with row2_c2:
    st.subheader("Free vs Paid Distribution")
    st.caption("User tier breakdown — Batch Layer")

    src_level = df_level
    if not src_level.empty and "level" in src_level.columns:
        count_col = next((c for c in ["user_count", "play_count"] if c in src_level.columns), None)
        if count_col:
            fig_pie = px.pie(
                src_level,
                names="level",
                values=count_col,
                color="level",
                color_discrete_map={"free": "#adb5bd", "paid": "#1db954"},
                hole=0.45,
            )
            fig_pie.update_traces(textposition="inside", textinfo="percent+label+value")
            fig_pie.update_layout(height=350)
            st.plotly_chart(fig_pie, use_container_width=True)
    else:
        st.info("Chưa có dữ liệu free/paid.")

st.divider()

# ══════════════════════════════════════════════════════════════
# Row 3: Session Length Distribution  |  Active Users Over Time
# ══════════════════════════════════════════════════════════════
row3_c1, row3_c2 = st.columns(2)

with row3_c1:
    st.subheader("Session Length Distribution")
    st.caption("Songs per session — Batch Layer")

    if not df_session_dist.empty and "songs_per_session" in df_session_dist.columns:
        src_sd = df_session_dist[df_session_dist["songs_per_session"] <= 30].copy()
        fig_hist = px.bar(
            src_sd,
            x="songs_per_session",
            y="session_count",
            color="session_count",
            color_continuous_scale="Blues",
            labels={"songs_per_session": "Songs per Session", "session_count": "Sessions"},
        )
        fig_hist.update_layout(coloraxis_showscale=False, height=350)
        st.plotly_chart(fig_hist, use_container_width=True)
    else:
        st.info("Chưa có dữ liệu session distribution.")

with row3_c2:
    label_active = "Active Users Over Time" + (" ✦ Merged" if is_merged else " (Batch)")
    st.subheader(label_active)
    st.caption("Hourly active users · " + ("Batch + Speed" if is_merged else "Batch only"))

    src_active = df_active
    if src_active.empty:
        src_active = df_active_users

    if not src_active.empty:
        time_col = next((c for c in ["hour_bucket", "hour_of_day"] if c in src_active.columns), None)
        if time_col:
            src_sorted = src_active.sort_values(time_col)
            fig_area = px.area(
                src_sorted,
                x=time_col,
                y="active_users",
                labels={time_col: "Time", "active_users": "Active Users"},
                color_discrete_sequence=["#1db954"],
            )
            fig_area.update_layout(height=350)
            st.plotly_chart(fig_area, use_container_width=True)
    else:
        st.info("Chưa có dữ liệu active users timeline.")

st.divider()

# ══════════════════════════════════════════════════════════════
# Row 4: User Retention  |  Live Stream Feed
# ══════════════════════════════════════════════════════════════
row4_c1, row4_c2 = st.columns(2)

with row4_c1:
    st.subheader("User Retention — New vs Returning")
    st.caption("Daily cohort analysis · Batch Layer")

    if not df_retention.empty:
        try:
            df_ret_pivot = df_retention.pivot_table(
                index="play_date", columns="user_type", values="user_count", aggfunc="sum"
            ).reset_index().fillna(0)
            df_ret_plot = df_retention.sort_values("play_date")
            fig_ret = px.bar(
                df_ret_plot,
                x="play_date",
                y="user_count",
                color="user_type",
                barmode="stack",
                color_discrete_map={"new": "#1db954", "returning": "#0d6efd"},
                labels={"play_date": "Date", "user_count": "Users", "user_type": "Type"},
            )
            fig_ret.update_layout(height=350)
            st.plotly_chart(fig_ret, use_container_width=True)
        except Exception:
            st.info("Không thể hiển thị retention chart.")
    else:
        st.info("Chưa có dữ liệu retention.")

with row4_c2:
    st.subheader("⚡ Live Stream Feed")
    st.caption("EventSim → Kafka → Spark Streaming → HDFS")

    if not df_live.empty:
        latest = (
            df_live.sort_values("ingestion_time", ascending=False).head(8)
            if "ingestion_time" in df_live.columns else df_live.head(8)
        )
        for _, row in latest.iterrows():
            artist = str(row.get("artist", "Unknown"))
            song   = str(row.get("song",   "Unknown"))
            label  = f"{artist[:25]} — {song[:30]}"
            with st.container(border=True):
                st.write(f"🎵 **{label}**")
                st.caption(
                    f"User {row.get('userId','?')} · "
                    f"{str(row.get('level','?')).upper()} · "
                    f"{row.get('location','?')[:25]} · "
                    f":green[**LIVE**]"
                )
    else:
        st.info("Đang chờ dữ liệu streaming từ Kafka...")
        st.caption("Chạy: `docker exec spark-master spark-submit --master spark://spark-master:7077 /spark-jobs/03_spark_streaming.py`")

st.divider()

# ── Lambda Architecture diagram ───────────────────────────────
with st.expander("📐 Lambda Architecture — Data Flow", expanded=False):
    st.markdown("""
    ```
    EventSim container
    (generate JSON events, push → Kafka topic: music-events)
              │
    ┌─────────┴─────────────────────────────────────┐
    ▼                                               ▼
    BATCH LAYER                              SPEED LAYER
    HDFS /music/raw/                   03_spark_streaming.py
    │                            (Kafka → HDFS /music/streaming/)
    01_eda.py (stats)                              │
    │                                              │
    02_analytics.py                                │
    (top songs, peak hours,                        │
    free/paid, retention)                          │
    │                                              │
    04_hive_load.py                                │
    (Hive tables)                                  │
    │                                              │
    05_export.py                                   │
    (HDFS → /data/*.parquet)                       │
    │                                              │
    └──────────────────┬────────────────────────────┘
                       ▼
               SERVING LAYER
         Hive Metastore (PostgreSQL)
         HDFS Parquet → /data/*.parquet
                       │
                       ▼
            Streamlit Dashboard (:8501)
    ```
    """)

# ── Data table ────────────────────────────────────────────────
with st.expander("Top 50 Songs — Detail", expanded=False):
    src = df_songs if not df_songs.empty else df_top10_songs
    if not src.empty:
        plays_col = next((c for c in ["merged_plays", "play_count"] if c in src.columns), None)
        if plays_col:
            st.dataframe(src.nlargest(50, plays_col), use_container_width=True)
    else:
        st.info("Không có dữ liệu.")

st.caption(
    f"Lambda Architecture · HDFS + Spark + Kafka + Hive + Streamlit · "
    f"Batch {batch_total:,} | Speed {speed_total:,} | "
    f"Total {batch_total + speed_total:,}"
)
