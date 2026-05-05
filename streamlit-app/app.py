import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pyarrow.parquet as pq
import requests
import io
import os
import time
from datetime import datetime

# ══════════════════════════════════════════════════════════════
# CONFIGURATION & STYLING
# ══════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="Music Analytics | Lambda Dashboard",
    page_icon="🎵",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for Premium Look
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }
    
    .stMetric {
        background: rgba(255, 255, 255, 0.05);
        padding: 15px;
        border-radius: 12px;
        border: 1px solid rgba(255, 255, 255, 0.1);
        transition: all 0.3s ease;
    }
    .stMetric:hover {
        transform: translateY(-5px);
        border-color: rgba(255, 255, 255, 0.2);
        box-shadow: 0 10px 20px rgba(0,0,0,0.2);
    }
    
    .layer-card {
        padding: 20px;
        border-radius: 16px;
        margin-bottom: 20px;
        color: white;
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
    }
    .batch-layer { background: linear-gradient(135deg, #e74c3c 0%, #c0392b 100%); }
    .speed-layer { background: linear-gradient(135deg, #f1c40f 0%, #f39c12 100%); }
    .serving-layer { background: linear-gradient(135deg, #3498db 0%, #2980b9 100%); }
    
    .live-event-card {
        background: rgba(255, 255, 255, 0.03);
        border-left: 4px solid #f1c40f;
        padding: 12px;
        margin-bottom: 8px;
        border-radius: 0 8px 8px 0;
        animation: fadeIn 0.5s ease;
    }
    
    @keyframes fadeIn {
        from { opacity: 0; transform: translateX(-10px); }
        to { opacity: 1; transform: translateX(0); }
    }
    
    .status-badge {
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 600;
        text-transform: uppercase;
    }
    .badge-online { background: #2ecc7122; color: #2ecc71; border: 1px solid #2ecc71; }
    .badge-offline { background: #e74c3c22; color: #e74c3c; border: 1px solid #e74c3c; }
    
    /* Hide Streamlit Header/Footer */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    </style>
""", unsafe_allow_html=True)

# Path Constants
DATA_DIR = "/app/data"
WEBHDFS  = "http://namenode:9870/webhdfs/v1"

# ══════════════════════════════════════════════════════════════
# DATA LAYER (HELPER FUNCTIONS)
# ══════════════════════════════════════════════════════════════

@st.cache_data(ttl=60)
def load_parquet(filename: str) -> pd.DataFrame:
    """Load parquet from local volume with multiple fallback paths"""
    for prefix in [DATA_DIR + "/", "data/", "../data/", "./data/"]:
        path = os.path.join(prefix, filename)
        if os.path.exists(path):
            try:
                return pd.read_parquet(path)
            except Exception as e:
                print(f"Error reading {path}: {e}")
                pass
    return pd.DataFrame()

@st.cache_data(ttl=5)
def fetch_live_stream() -> pd.DataFrame:
    """Fetch real-time events directly from HDFS via WebHDFS"""
    try:
        url = f"{WEBHDFS}/music/streaming?op=LISTSTATUS"
        resp = requests.get(url, timeout=3)
        resp.raise_for_status()
        
        files = [f for f in resp.json()["FileStatuses"]["FileStatus"] 
                 if ".parquet" in f["pathSuffix"]]
        
        if not files:
            return pd.DataFrame()
            
        # Get last 3 files for fresh stream
        dfs = []
        for f in sorted(files, key=lambda x: x["modificationTime"], reverse=True)[:3]:
            file_url = f"{WEBHDFS}/music/streaming/{f['pathSuffix']}?op=OPEN"
            r = requests.get(file_url, allow_redirects=True, timeout=5)
            dfs.append(pq.read_table(io.BytesIO(r.content)).to_pandas())
        
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    except Exception:
        return pd.DataFrame()

# Load All Data
# Batch
df_overview = load_parquet("overview.parquet")
df_batch_songs = load_parquet("top10_songs.parquet")
df_batch_artists = load_parquet("top10_artists.parquet")
df_plays_hour = load_parquet("plays_by_hour.parquet")
df_level_ratio = load_parquet("level_ratio.parquet")
df_retention = load_parquet("retention.parquet")

# Speed
df_speed_songs = load_parquet("speed_top_songs.parquet")
df_speed_active = load_parquet("speed_active_ts.parquet")
df_speed_summary = load_parquet("speed_summary.parquet")
df_live_feed = fetch_live_stream()

# Serving
df_merged_songs = load_parquet("merged_top_songs.parquet")
df_merged_artists = load_parquet("merged_top_artists.parquet")
df_merged_active = load_parquet("merged_active_ts.parquet")

# Process Metrics
speed_total = batch_total = 0
last_ingest = "Never"
if not df_speed_summary.empty:
    speed_total = int(df_speed_summary.iloc[0].get("stream_total", 0))
    batch_total = int(df_speed_summary.iloc[0].get("batch_total", 0))
    last_ingest = str(df_speed_summary.iloc[0].get("latest_ingestion", "N/A"))[:19]

# ══════════════════════════════════════════════════════════════
# SIDEBAR & SYSTEM STATUS
# ══════════════════════════════════════════════════════════════

with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/000000/music-node.png", width=80)
    st.title("Music Analytics")
    st.markdown("### Lambda Architecture")
    st.divider()

    # Status Badges
    st.markdown("#### System Health")
    
    batch_status = "online" if not df_batch_songs.empty else "offline"
    speed_status = "online" if speed_total > 0 else "offline"
    serving_status = "online" if not df_merged_songs.empty else "offline"
    
    st.markdown(f"🔴 **Batch Layer**: <span class='status-badge badge-{batch_status}'>{batch_status}</span>", unsafe_allow_html=True)
    st.markdown(f"🟡 **Speed Layer**: <span class='status-badge badge-{speed_status}'>{speed_status}</span>", unsafe_allow_html=True)
    st.markdown(f"🔵 **Serving Layer**: <span class='status-badge badge-{serving_status}'>{serving_status}</span>", unsafe_allow_html=True)
    
    st.divider()
    
    st.markdown(f"**Last Sync**: `{last_ingest}`")
    
    if st.toggle("Auto-Refresh (30s)", value=True):
        st.markdown('<meta http-equiv="refresh" content="30">', unsafe_allow_html=True)
        st.caption("Auto-refreshing every 30 seconds...")

    st.divider()
    st.info("Built with Spark, Kafka, HDFS & Hive")

# ══════════════════════════════════════════════════════════════
# MAIN DASHBOARD - TOP KPI
# ══════════════════════════════════════════════════════════════

st.title("🎵 Real-time Music Analytics")
st.markdown("#### Holistic View of the Music Streaming Ecosystem")

k1, k2, k3, k4 = st.columns(4)

with k1:
    st.metric("Total Plays", f"{batch_total + speed_total:,}", delta=f"+{speed_total:,} Live")
with k2:
    st.metric("Batch Data", f"{batch_total:,}", help="Historical data in HDFS")
with k3:
    st.metric("Stream Flow", f"{speed_total:,}", help="Processed by Spark Streaming")
with k4:
    unique_artists = df_overview.iloc[0].get("unique_artists", 0) if not df_overview.empty else 0
    st.metric("Artists Tracked", f"{int(unique_artists):,}")

st.divider()

# ══════════════════════════════════════════════════════════════
# THE 3 LAYERS OF LAMBDA
# ══════════════════════════════════════════════════════════════

# Create 3 clear sections using tabs for deep-dives
tab_serving, tab_batch, tab_speed = st.tabs([
    "🔵 SERVING LAYER (Unified View)", 
    "🔴 BATCH LAYER (Historical)", 
    "🟡 SPEED LAYER (Real-time)"
])

# ─── SERVING LAYER ─────────────────────────────────────────────
with tab_serving:
    st.markdown("""
        <div class='layer-card serving-layer'>
            <h3>🔵 Serving Layer</h3>
            <p>The Serving Layer merges high-accuracy historical data with low-latency streaming data to provide a comprehensive, up-to-the-second view.</p>
        </div>
    """, unsafe_allow_html=True)
    
    c1, c2 = st.columns([2, 1])
    
    with c1:
        st.subheader("Global Leaderboard (Merged)")
        if not df_merged_songs.empty:
            # Stacked Bar: Batch vs Speed
            top_songs = df_merged_songs.nlargest(10, 'merged_plays').copy()
            top_songs['label'] = top_songs['song'] + " - " + top_songs['artist']
            
            fig = go.Figure()
            fig.add_trace(go.Bar(
                y=top_songs['label'], x=top_songs['play_count'],
                name='Batch (History)', orientation='h',
                marker=dict(color='#e74c3c')
            ))
            fig.add_trace(go.Bar(
                y=top_songs['label'], x=top_songs.get('stream_plays', 0),
                name='Speed (Live)', orientation='h',
                marker=dict(color='#f1c40f')
            ))
            
            fig.update_layout(
                barmode='stack', 
                height=450, 
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font=dict(color='white'),
                margin=dict(l=0, r=0, t=20, b=0),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Waiting for Batch and Speed layers to converge...")

    with c2:
        st.subheader("Active Users Trend")
        if not df_merged_active.empty:
            df_ma = df_merged_active.sort_values('hour_bucket')
            fig_ma = px.area(df_ma, x='hour_bucket', y='active_users', 
                            color_discrete_sequence=['#3498db'])
            fig_ma.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0))
            st.plotly_chart(fig_ma, use_container_width=True)
        
        st.markdown("---")
        st.write("📊 **Quick Stats**")
        st.write(f"• Ingestion Rate: **{speed_total/60:.1f} events/min**")
        st.write(f"• Batch Refresh: **Hourly**")

# ─── BATCH LAYER ───────────────────────────────────────────────
with tab_batch:
    st.markdown("""
        <div class='layer-card batch-layer'>
            <h3>🔴 Batch Layer</h3>
            <p>Processes the master dataset in HDFS. Provides high-precision analytics like retention and long-term trends.</p>
        </div>
    """, unsafe_allow_html=True)
    
    b1, b2, b3 = st.columns(3)
    
    with b1:
        st.subheader("Daily Retention")
        if not df_retention.empty:
            fig_ret = px.bar(df_retention, x='play_date', y='user_count', color='user_type',
                             color_discrete_map={'new': '#e74c3c', 'returning': '#922b21'})
            fig_ret.update_layout(height=300, barmode='group')
            st.plotly_chart(fig_ret, use_container_width=True)
    
    with b2:
        st.subheader("Listening Peak Hours")
        if not df_plays_hour.empty:
            fig_h = px.line(df_plays_hour.sort_values('hour_of_day'), 
                           x='hour_of_day', y='play_count', markers=True,
                           color_discrete_sequence=['#e74c3c'])
            fig_h.update_layout(height=300)
            st.plotly_chart(fig_h, use_container_width=True)
            
    with b3:
        st.subheader("Tier Breakdown")
        if not df_level_ratio.empty:
            fig_pie = px.pie(df_level_ratio, names='level', values='play_count', 
                            hole=0.5, color_discrete_sequence=['#e74c3c', '#c0392b'])
            fig_pie.update_layout(height=300)
            st.plotly_chart(fig_pie, use_container_width=True)

# ─── SPEED LAYER ───────────────────────────────────────────────
with tab_speed:
    st.markdown("""
        <div class='layer-card speed-layer'>
            <h3>🟡 Speed Layer</h3>
            <p>Near real-time processing of the Kafka stream. Delivers immediate insights with sub-second latency.</p>
        </div>
    """, unsafe_allow_html=True)
    
    s1, s2 = st.columns([1, 1.5])
    
    with s1:
        st.subheader("⚡ Live Stream Feed")
        if not df_live_feed.empty:
            # Show top 8 latest events
            latest = df_live_feed.head(8)
            for _, row in latest.iterrows():
                st.markdown(f"""
                    <div class='live-event-card'>
                        <b>{row.get('artist', 'Unknown')}</b> — {row.get('song', 'Unknown')}<br>
                        <small>User {row.get('userId', 'N/A')} • {row.get('location', 'Global')}</small>
                    </div>
                """, unsafe_allow_html=True)
        else:
            st.warning("Listening for Kafka events...")
            st.image("https://media.giphy.com/media/v1.Y2lkPTc5MGI3NjExM3Y5bmR5bmR5bmR5bmR5bmR5bmR5bmR5bmR5bmR5bmR5JmVwPXYxX2ludGVybmFsX2dpZl9ieV9pZCZjdD1n/3o7TKMGpxVfNlqS2qY/giphy.gif", width=200)

    with s2:
        st.subheader("Real-time Active Users")
        if not df_speed_active.empty:
            fig_speed_act = px.area(df_speed_active.sort_values('hour_bucket'), 
                                   x='hour_bucket', y='active_users',
                                   color_discrete_sequence=['#f1c40f'])
            fig_speed_act.update_layout(height=350)
            st.plotly_chart(fig_speed_act, use_container_width=True)
        else:
            st.info("Streaming analytics data is being computed...")

# ══════════════════════════════════════════════════════════════
# FOOTER
# ══════════════════════════════════════════════════════════════
st.divider()
st.caption(f"© 2026 Music Streaming Analytics Pipeline | Serving {batch_total + speed_total:,} events total.")
