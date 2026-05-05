import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pyarrow.parquet as pq
import requests, io, os, time
from datetime import datetime

# ══════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="Music Streaming Analytics",
    page_icon="🎵",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700;800&display=swap');

html, body, [class*="css"], .stApp {
    font-family: 'Inter', sans-serif;
    background-color: #0d0d0d !important;
    color: #ffffff;
}

section[data-testid="stSidebar"] { background: #111111 !important; }

.block-container { padding: 1.5rem 2rem !important; max-width: 100% !important; }

/* KPI Cards */
.kpi-card {
    background: #161616;
    border: 1px solid #282828;
    border-radius: 14px;
    padding: 22px 24px;
    position: relative;
    overflow: hidden;
    transition: border-color 0.2s;
    min-height: 110px;
}
.kpi-card:hover { border-color: #1ed760; }
.kpi-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, #1ed760, #00d4aa);
}
.kpi-label  { color: #b3b3b3; font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 8px; }
.kpi-value  { color: #ffffff; font-size: 32px; font-weight: 800; line-height: 1; }
.kpi-delta  { font-size: 12px; margin-top: 8px; }
.kpi-icon   { position: absolute; top: 20px; right: 20px; font-size: 26px; opacity: 0.7; }
.delta-up   { color: #1ed760; }
.delta-down { color: #e74c3c; }
.delta-neutral { color: #b3b3b3; }

/* Section headers */
.section-title {
    font-size: 15px;
    font-weight: 700;
    color: #ffffff;
    margin-bottom: 4px;
}
.section-sub {
    font-size: 11px;
    color: #7f8c8d;
    margin-bottom: 16px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}

/* Chart containers */
.chart-card {
    background: #161616;
    border: 1px solid #1e1e1e;
    border-radius: 14px;
    padding: 20px;
}

/* At-risk row */
.risk-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 10px 14px;
    margin-bottom: 6px;
    background: #1a1a1a;
    border-radius: 8px;
    border-left: 3px solid #e74c3c;
}
.risk-name  { font-weight: 600; font-size: 13px; color: #fff; }
.risk-sub   { font-size: 11px; color: #7f8c8d; }
.badge-high   { background: #e74c3c22; color: #e74c3c; border: 1px solid #e74c3c44; padding: 2px 10px; border-radius: 20px; font-size: 11px; font-weight: 700; }
.badge-medium { background: #f39c1222; color: #f39c12; border: 1px solid #f39c1244; padding: 2px 10px; border-radius: 20px; font-size: 11px; font-weight: 700; }
.badge-low    { background: #1ed76022; color: #1ed760; border: 1px solid #1ed76044; padding: 2px 10px; border-radius: 20px; font-size: 11px; font-weight: 700; }

/* Live ticker */
.live-dot { display:inline-block; width:8px; height:8px; background:#1ed760; border-radius:50%; animation:pulse 1.5s infinite; margin-right:6px; }
@keyframes pulse { 0%,100%{opacity:1;} 50%{opacity:0.3;} }
.ticker-item {
    display: inline-flex; align-items: center; gap: 8px;
    background: #1a1a1a; border: 1px solid #282828; border-radius: 20px;
    padding: 4px 14px; margin-right: 8px; font-size: 12px; color: #b3b3b3;
}

/* Hide Streamlit chrome */
#MainMenu, footer, header { visibility: hidden; }
.stDeployButton { display: none; }
div[data-testid="stDecoration"] { display: none; }
</style>
""", unsafe_allow_html=True)

# ── DARK PLOTLY TEMPLATE ──────────────────────────────────────
DARK = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color="#b3b3b3", family="Inter"),
    xaxis=dict(gridcolor="#1e1e1e", linecolor="#282828", tickcolor="#555"),
    yaxis=dict(gridcolor="#1e1e1e", linecolor="#282828", tickcolor="#555"),
    margin=dict(l=0, r=0, t=30, b=0),
)
COLORS = {
    "green":  "#1ed760",
    "teal":   "#00d4aa",
    "blue":   "#00b4d8",
    "red":    "#e74c3c",
    "orange": "#f39c12",
    "purple": "#9b59b6",
    "gray":   "#7f8c8d",
}
SEG_COLORS = {
    "Power Users":       COLORS["green"],
    "Regular Listeners": COLORS["blue"],
    "Casual Listeners":  COLORS["gray"],
    "At-Risk Users":     COLORS["red"],
}

# ══════════════════════════════════════════════════════════════
# DATA LOADING
# ══════════════════════════════════════════════════════════════
DATA_DIR = "/app/data"
WEBHDFS  = "http://namenode:9870/webhdfs/v1"

@st.cache_data(ttl=60)
def load_parquet(filename):
    for prefix in [DATA_DIR + "/", "data/", "../data/"]:
        path = os.path.join(prefix, filename)
        if os.path.exists(path):
            try:    return pd.read_parquet(path)
            except: pass
    return pd.DataFrame()

@st.cache_data(ttl=5)
def fetch_live_stream():
    try:
        resp  = requests.get(f"{WEBHDFS}/music/streaming?op=LISTSTATUS", timeout=3)
        files = [f for f in resp.json()["FileStatuses"]["FileStatus"] if ".parquet" in f["pathSuffix"]]
        if not files: return pd.DataFrame()
        dfs = []
        for f in sorted(files, key=lambda x: x["modificationTime"], reverse=True)[:3]:
            r = requests.get(f"{WEBHDFS}/music/streaming/{f['pathSuffix']}?op=OPEN", allow_redirects=True, timeout=5)
            dfs.append(pq.read_table(io.BytesIO(r.content)).to_pandas())
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    except: return pd.DataFrame()

# Load
df_overview          = load_parquet("overview.parquet")
df_batch_songs       = load_parquet("top10_songs.parquet")
df_batch_artists     = load_parquet("top10_artists.parquet")
df_plays_hour        = load_parquet("plays_by_hour.parquet")
df_level_ratio       = load_parquet("level_ratio.parquet")
df_retention         = load_parquet("retention.parquet")
df_gender_stats      = load_parquet("gender_stats.parquet")
df_top_locations     = load_parquet("top_locations.parquet")
df_speed_active      = load_parquet("speed_active_ts.parquet")
df_speed_summary     = load_parquet("speed_summary.parquet")
df_merged_active     = load_parquet("merged_active_ts.parquet")
df_segment_profiles  = load_parquet("segment_profiles.parquet")
df_user_segments     = load_parquet("user_segments.parquet")
df_churn_predictions = load_parquet("churn_predictions.parquet")
df_churn_summary     = load_parquet("churn_summary.parquet")

# Compute KPIs
speed_total = batch_total = 0
last_ingest = "—"
if not df_speed_summary.empty:
    r = df_speed_summary.iloc[0]
    speed_total = int(r.get("stream_total", 0))
    batch_total = int(r.get("batch_total",  0))
    last_ingest = str(r.get("latest_ingestion", "—"))[:16]

total_streams   = batch_total + speed_total
unique_users    = int(df_overview.iloc[0].get("unique_users",  0)) if not df_overview.empty else 0
unique_artists  = int(df_overview.iloc[0].get("unique_artists",0)) if not df_overview.empty else 0
top_artist      = df_batch_artists.iloc[0]["artist"] if not df_batch_artists.empty else "—"

avg_churn = 0.0
high_risk = medium_risk = total_ml_users = 0
auc_roc   = 0.0
if not df_churn_summary.empty:
    cr = df_churn_summary.iloc[0]
    avg_churn      = float(cr.get("avg_churn_pct", 0))
    high_risk      = int(cr.get("high_risk",       0))
    medium_risk    = int(cr.get("medium_risk",     0))
    total_ml_users = int(cr.get("total_users",     0))
    auc_roc        = float(cr.get("auc_roc",       0))

# ══════════════════════════════════════════════════════════════
# HEADER
# ══════════════════════════════════════════════════════════════
h1, h2 = st.columns([3, 1])
with h1:
    st.markdown("## Music Streaming Analytics")
    st.markdown("<p style='color:#7f8c8d;margin-top:-12px;font-size:13px;'>Real-time Business Intelligence Dashboard</p>", unsafe_allow_html=True)
with h2:
    st.markdown(f"""
    <div style='text-align:right;padding-top:8px;'>
        <span style='color:#7f8c8d;font-size:12px;'>Last sync: {last_ingest}</span><br>
        <span style='color:#1ed760;font-size:12px;'>● LIVE</span>
        <span style='color:#7f8c8d;font-size:12px;'> · Kafka + Spark Streaming</span>
    </div>""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════
# ROW 1 — KPI CARDS
# ══════════════════════════════════════════════════════════════
c1, c2, c3, c4 = st.columns(4)

def kpi_card(col, label, value, delta_html, icon):
    col.markdown(f"""
    <div class='kpi-card'>
        <div class='kpi-icon'>{icon}</div>
        <div class='kpi-label'>{label}</div>
        <div class='kpi-value'>{value}</div>
        <div class='kpi-delta'>{delta_html}</div>
    </div>""", unsafe_allow_html=True)

streams_fmt = f"{total_streams/1e6:.1f}M" if total_streams >= 1e6 else f"{total_streams:,}"
mau_fmt     = f"{unique_users/1e3:.1f}K"  if unique_users  >= 1000  else str(unique_users)
churn_fmt   = f"{avg_churn:.1f}%"
top_art_fmt = top_artist[:14] + "…" if len(top_artist) > 14 else top_artist

kpi_card(c1, "Total Streams",    streams_fmt,
         f"<span class='delta-up'>+{speed_total:,} live</span>", "📈")
kpi_card(c2, "Monthly Active Users", mau_fmt,
         f"<span class='delta-neutral'>{unique_artists:,} artists tracked</span>", "👥")
kpi_card(c3, "Churn Rate",       churn_fmt,
         f"<span class='delta-down'>{high_risk:,} high-risk users</span>" if high_risk else "<span class='delta-neutral'>Run pipeline for ML</span>", "📉")
kpi_card(c4, "Top Artist",       top_art_fmt,
         f"<span class='delta-neutral'>Batch · Historical</span>", "🎵")

st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════
# ROW 2 — HOURLY TRAFFIC + FREE/PAID DONUT
# ══════════════════════════════════════════════════════════════
left, right = st.columns([2, 1], gap="medium")

with left:
    st.markdown("<div class='section-title'>Hourly Traffic Peaks</div>", unsafe_allow_html=True)
    st.markdown("<div class='section-sub'>Server load comparison: Real-time vs Batch processing</div>", unsafe_allow_html=True)

    batch_h = (df_plays_hour[["hour_of_day","play_count"]].rename(columns={"hour_of_day":"hour","play_count":"batch"})
               if not df_plays_hour.empty
               else pd.DataFrame({"hour": range(24), "batch": [0]*24}))

    if not df_speed_active.empty:
        tmp = df_speed_active.copy()
        tmp["hour"] = pd.to_datetime(tmp["hour_bucket"]).dt.hour
        speed_h = tmp.groupby("hour")["active_users"].sum().reset_index().rename(columns={"active_users":"realtime"})
    else:
        speed_h = pd.DataFrame({"hour": range(24), "realtime": [0]*24})

    combined = batch_h.merge(speed_h, on="hour", how="outer").fillna(0).sort_values("hour")
    hours_fmt = [f"{int(h):02d}:00" for h in combined["hour"]]

    fig_traffic = go.Figure()
    fig_traffic.add_trace(go.Scatter(
        x=hours_fmt, y=combined["batch"], name="Batch Processing",
        fill="tozeroy", line=dict(color=COLORS["teal"], width=2),
        fillcolor="rgba(0,212,170,0.12)",
    ))
    fig_traffic.add_trace(go.Scatter(
        x=hours_fmt, y=combined["realtime"], name="Real-time Traffic",
        fill="tozeroy", line=dict(color=COLORS["green"], width=2),
        fillcolor="rgba(30,215,96,0.12)",
    ))
    fig_traffic.update_layout(
        **DARK, height=280,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                    font=dict(size=11), bgcolor="rgba(0,0,0,0)"),
        margin=dict(l=0, r=0, t=40, b=0),
    )
    st.plotly_chart(fig_traffic, use_container_width=True)

with right:
    st.markdown("<div class='section-title'>User Distribution</div>", unsafe_allow_html=True)
    st.markdown("<div class='section-sub'>Free vs Premium conversion tracking</div>", unsafe_allow_html=True)

    if not df_level_ratio.empty:
        fig_donut = px.pie(
            df_level_ratio, names="level", values="user_count",
            hole=0.6,
            color="level",
            color_discrete_map={"free": COLORS["blue"], "paid": COLORS["green"]},
        )
        fig_donut.update_traces(textinfo="none", hovertemplate="%{label}: %{percent}")
        fig_donut.update_layout(
            **DARK, height=250,
            showlegend=True,
            legend=dict(orientation="h", yanchor="top", y=-0.05, xanchor="center", x=0.5,
                        font=dict(size=11)),
            margin=dict(l=10, r=10, t=10, b=40),
        )
        # Center annotation
        free_pct = df_level_ratio[df_level_ratio["level"]=="free"]["user_pct"].values
        paid_pct = df_level_ratio[df_level_ratio["level"]=="paid"]["user_pct"].values
        fig_donut.add_annotation(
            text=f"<b>{paid_pct[0]:.0f}%</b><br><span style='font-size:10px'>Paid</span>" if len(paid_pct) else "",
            x=0.5, y=0.5, showarrow=False, font=dict(size=16, color="white"),
        )
        st.plotly_chart(fig_donut, use_container_width=True)
        if len(free_pct) and len(paid_pct):
            st.markdown(f"""
            <div style='text-align:center;font-size:12px;color:#b3b3b3;margin-top:-10px;'>
                <span style='color:{COLORS["blue"]};'>● Free Users: {free_pct[0]:.0f}%</span>&nbsp;&nbsp;
                <span style='color:{COLORS["green"]};'>● Premium Users: {paid_pct[0]:.0f}%</span>
            </div>""", unsafe_allow_html=True)
    else:
        st.info("Run pipeline to load data")

st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════
# ROW 3 — KMEANS SCATTER + CHURN GAUGE + AT-RISK USERS
# ══════════════════════════════════════════════════════════════
col_a, col_b = st.columns(2, gap="medium")

with col_a:
    st.markdown("<div class='section-title'>Customer Segmentation (K-Means Clustering)</div>", unsafe_allow_html=True)
    st.markdown("<div class='section-sub'>Engagement Score vs Retention Rate</div>", unsafe_allow_html=True)

    if not df_user_segments.empty:
        fig_scatter = px.scatter(
            df_user_segments.sample(min(2000, len(df_user_segments)), random_state=42),
            x="total_plays", y="active_days",
            color="segment_label",
            color_discrete_map=SEG_COLORS,
            labels={"total_plays": "Engagement Score", "active_days": "Retention Rate (days)"},
            hover_data={"total_plays": True, "active_days": True, "level": True},
            opacity=0.75, size_max=8,
        )
        fig_scatter.update_traces(marker=dict(size=6))
        fig_scatter.update_layout(
            **DARK, height=320,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                        font=dict(size=11), bgcolor="rgba(0,0,0,0)", title=""),
        )
        st.plotly_chart(fig_scatter, use_container_width=True)
    else:
        st.markdown("""
        <div style='height:320px;display:flex;align-items:center;justify-content:center;
                    background:#161616;border-radius:12px;color:#555;font-size:13px;'>
            Run pipeline to generate KMeans segments
        </div>""", unsafe_allow_html=True)

with col_b:
    st.markdown("<div class='section-title'>Retention & Churn Analysis</div>", unsafe_allow_html=True)
    st.markdown("<div class='section-sub'>Predictive risk assessment</div>", unsafe_allow_html=True)

    g_col, _ = st.columns([1, 0.01])
    with g_col:
        risk_color = COLORS["red"] if avg_churn >= 50 else COLORS["orange"] if avg_churn >= 30 else COLORS["green"]
        risk_label = "High" if avg_churn >= 50 else "Medium" if avg_churn >= 30 else "Low"

        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=avg_churn,
            title={"text": "Current Churn Prediction", "font": {"color": "#7f8c8d", "size": 12}},
            number={"suffix": "%", "font": {"color": "white", "size": 40}},
            gauge={
                "axis": {"range": [0, 100], "tickcolor": "#555", "tickfont": {"color":"#555","size":10}},
                "bar":  {"color": risk_color, "thickness": 0.25},
                "bgcolor": "#1a1a1a",
                "borderwidth": 0,
                "steps": [
                    {"range": [0,  33], "color": "rgba(30,215,96,0.1)"},
                    {"range": [33, 66], "color": "rgba(243,156,18,0.1)"},
                    {"range": [66,100], "color": "rgba(231,76,60,0.1)"},
                ],
            },
        ))
        fig_gauge.update_layout(
            **DARK, height=190,
            margin=dict(t=40, b=0, l=30, r=30),
        )
        st.plotly_chart(fig_gauge, use_container_width=True)
        st.markdown(f"<p style='text-align:center;color:{risk_color};font-size:12px;margin-top:-10px;'>Risk Level: {risk_label}</p>", unsafe_allow_html=True)

    # At-risk users list
    st.markdown("<div style='font-size:13px;font-weight:700;color:#fff;margin:8px 0 6px;'>At-Risk Premium Users</div>", unsafe_allow_html=True)
    if not df_churn_predictions.empty:
        top_risk = df_churn_predictions[df_churn_predictions["risk_level"].isin(["High","Medium"])] \
            .sort_values("churn_probability", ascending=False).head(5)
        for _, row in top_risk.iterrows():
            lvl   = str(row.get("level", "free")).capitalize()
            prob  = float(row.get("churn_probability", 0))
            risk  = str(row.get("risk_level", "Medium"))
            uid   = row.get("userId", "—")
            fname = row.get("firstName", f"User {uid}")
            lname = row.get("lastName", "")
            badge_cls = "badge-high" if risk == "High" else "badge-medium"
            st.markdown(f"""
            <div class='risk-row'>
                <div>
                    <div class='risk-name'>{fname} {lname}</div>
                    <div class='risk-sub'>{lvl} Plan · User #{uid}</div>
                </div>
                <div style='display:flex;align-items:center;gap:8px;'>
                    <span style='color:#b3b3b3;font-size:12px;'>{prob:.0%}</span>
                    <span class='{badge_cls}'>{risk}</span>
                </div>
            </div>""", unsafe_allow_html=True)
    else:
        st.markdown("<p style='color:#555;font-size:12px;'>Run pipeline for predictions</p>", unsafe_allow_html=True)

st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════
# ROW 4 — TOP LOCATIONS + GENDER DISTRIBUTION
# ══════════════════════════════════════════════════════════════
fl, fg = st.columns([1.5, 1], gap="medium")

with fl:
    st.markdown("<div class='section-title'>Top Locations</div>", unsafe_allow_html=True)
    st.markdown("<div class='section-sub'>MAU by region</div>", unsafe_allow_html=True)

    if not df_top_locations.empty:
        top10 = df_top_locations.nlargest(10, "play_count")
        fig_loc = px.bar(
            top10.sort_values("play_count"),
            x="play_count", y="state", orientation="h",
            color="play_count",
            color_continuous_scale=[[0, "#005f2e"], [1, COLORS["green"]]],
            labels={"play_count": "", "state": ""},
        )
        fig_loc.update_coloraxes(showscale=False)
        fig_loc.update_layout(**DARK, height=280, margin=dict(l=0, r=0, t=10, b=0))
        fig_loc.update_traces(marker_line_width=0)
        st.plotly_chart(fig_loc, use_container_width=True)
    else:
        st.markdown("""
        <div style='height:280px;display:flex;align-items:center;justify-content:center;
                    background:#161616;border-radius:12px;color:#555;font-size:13px;'>
            Run pipeline to load location data
        </div>""", unsafe_allow_html=True)

with fg:
    st.markdown("<div class='section-title'>Gender Distribution</div>", unsafe_allow_html=True)
    st.markdown("<div class='section-sub'>User demographics</div>", unsafe_allow_html=True)

    if not df_gender_stats.empty:
        fig_gender = px.pie(
            df_gender_stats, names="gender", values="play_count",
            color_discrete_sequence=[COLORS["blue"], "#e91e8c", COLORS["purple"]],
        )
        fig_gender.update_traces(textinfo="percent", textfont_size=12,
                                  hovertemplate="%{label}: %{percent}")
        fig_gender.update_layout(
            **DARK, height=280,
            legend=dict(orientation="v", yanchor="middle", y=0.5,
                        xanchor="left", x=1.0, font=dict(size=11)),
            margin=dict(l=0, r=80, t=10, b=10),
        )
        st.plotly_chart(fig_gender, use_container_width=True)
    else:
        st.info("Run pipeline to load data")

# ══════════════════════════════════════════════════════════════
# LIVE TICKER (fragment — refreshes every 3s)
# ══════════════════════════════════════════════════════════════
@st.experimental_fragment(run_every=3)
def live_ticker():
    df_live = fetch_live_stream()
    if df_live.empty: return
    latest = df_live.head(6)
    items  = "".join([
        f"<span class='ticker-item'>"
        f"<span class='live-dot'></span>"
        f"<b>{r.get('artist','?')}</b> — {str(r.get('song','?'))[:25]}"
        f"</span>"
        for _, r in latest.iterrows()
    ])
    st.markdown(f"""
    <div style='background:#111;border-top:1px solid #1e1e1e;padding:10px 0;margin-top:8px;overflow:hidden;'>
        <span style='color:#1ed760;font-size:11px;font-weight:700;margin-right:12px;'>● LIVE NOW</span>
        {items}
    </div>""", unsafe_allow_html=True)

live_ticker()

# ══════════════════════════════════════════════════════════════
# FOOTER
# ══════════════════════════════════════════════════════════════
st.markdown(f"""
<div style='text-align:center;padding:20px 0 10px;color:#555;font-size:11px;border-top:1px solid #1e1e1e;margin-top:12px;'>
    Music Streaming Analytics · Lambda Architecture · Spark · Kafka · HDFS · Hive<br>
    {total_streams:,} total events · AUC-ROC {auc_roc:.3f} · {datetime.now().strftime('%Y-%m-%d')}
</div>""", unsafe_allow_html=True)
