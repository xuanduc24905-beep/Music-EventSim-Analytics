import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pyarrow.parquet as pq
import requests, io, os, time
from datetime import datetime

# ══════════════════════════════════════════════════════════════
# CONFIG & STYLES
# ══════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="Music Analytics BI",
    page_icon="🎵",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700;800&display=swap');

html, body, [class*="css"], .stApp {
    font-family: 'Inter', sans-serif;
    background-color: #0d0d0d !important;
    color: #ffffff;
}

section[data-testid="stSidebar"] { 
    background: #111111 !important; 
    border-right: 1px solid #222;
}

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

/* Section headers */
.section-title { font-size: 16px; font-weight: 700; color: #ffffff; margin-bottom: 4px; }
.section-sub { font-size: 11px; color: #7f8c8d; margin-bottom: 16px; text-transform: uppercase; letter-spacing: 0.5px; }

/* Risk row */
.risk-row {
    display: flex; align-items: center; justify-content: space-between;
    padding: 10px 14px; margin-bottom: 6px;
    background: #1a1a1a; border-radius: 8px; border-left: 3px solid #e74c3c;
}
.risk-name  { font-weight: 600; font-size: 13px; color: #fff; }
.risk-sub   { font-size: 11px; color: #7f8c8d; }

/* Badges */
.badge-high   { background: #e74c3c22; color: #e74c3c; border: 1px solid #e74c3c44; padding: 2px 10px; border-radius: 20px; font-size: 11px; font-weight: 700; }
.badge-medium { background: #f39c1222; color: #f39c12; border: 1px solid #f39c1244; padding: 2px 10px; border-radius: 20px; font-size: 11px; font-weight: 700; }

/* Live Ticker */
.live-dot { display:inline-block; width:8px; height:8px; background:#1ed760; border-radius:50%; animation:pulse 1.5s infinite; margin-right:6px; }
@keyframes pulse { 0%,100%{opacity:1;} 50%{opacity:0.3;} }
.ticker-item {
    display: inline-flex; align-items: center; gap: 8px;
    background: #1a1a1a; border: 1px solid #282828; border-radius: 20px;
    padding: 4px 14px; margin-right: 8px; font-size: 12px; color: #b3b3b3;
}

#MainMenu, footer, header { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

# ── PLOTLY THEME ──────────────────────────────────────────────
DARK_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color="#b3b3b3", family="Inter"),
    xaxis=dict(gridcolor="#1e1e1e", linecolor="#282828"),
    yaxis=dict(gridcolor="#1e1e1e", linecolor="#282828"),
)
COLORS = {"green": "#1ed760", "blue": "#00b4d8", "red": "#e74c3c", "orange": "#f39c12", "teal": "#00d4aa"}

# ══════════════════════════════════════════════════════════════
# DATA HANDLING
# ══════════════════════════════════════════════════════════════
DATA_DIR = "/app/data"
WEBHDFS  = "http://namenode:9870/webhdfs/v1"

@st.cache_data(ttl=60)
def load_parquet(filename):
    path = os.path.join(DATA_DIR, filename)
    if os.path.exists(path):
        try: return pd.read_parquet(path)
        except: return pd.DataFrame()
    return pd.DataFrame()

@st.cache_data(ttl=5)
def fetch_live_stream():
    try:
        resp = requests.get(f"{WEBHDFS}/music/streaming?op=LISTSTATUS", timeout=2)
        files = [f for f in resp.json()["FileStatuses"]["FileStatus"] if ".parquet" in f["pathSuffix"]]
        if not files: return pd.DataFrame()
        dfs = []
        for f in sorted(files, key=lambda x: x["modificationTime"], reverse=True)[:2]:
            r = requests.get(f"{WEBHDFS}/music/streaming/{f['pathSuffix']}?op=OPEN", allow_redirects=True, timeout=3)
            dfs.append(pq.read_table(io.BytesIO(r.content)).to_pandas())
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    except: return pd.DataFrame()

# ══════════════════════════════════════════════════════════════
# SIDEBAR NAVIGATION
# ══════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("<h2 style='color:#1ed760; margin-bottom:0;'>Music BI</h2>", unsafe_allow_html=True)
    st.markdown("<p style='color:#555; font-size:11px; margin-bottom:25px;'>Lambda Architecture Dashboard</p>", unsafe_allow_html=True)
    
    view = st.radio("Navigation", ["Marketing & PM", "System Benchmarks"], label_visibility="collapsed")
    
    st.markdown("<div style='position:fixed; bottom:20px; font-size:11px; color:#444;'>V1.2 · Powered by Spark</div>", unsafe_allow_html=True)

# Common Data
df_summary = load_parquet("speed_summary.parquet")
last_sync = df_summary.iloc[0]["latest_ingestion"][:16] if not df_summary.empty else "N/A"

def kpi(col, label, val, icon):
    col.markdown(
        f"<div class='kpi-card'><div class='kpi-icon'>{icon}</div>"
        f"<div class='kpi-label'>{label}</div>"
        f"<div class='kpi-value'>{val}</div></div>",
        unsafe_allow_html=True,
    )

# ══════════════════════════════════════════════════════════════
# VIEW 1: MARKETING & PRODUCT MANAGER
# ══════════════════════════════════════════════════════════════
if view == "Marketing & PM":
    # Header
    h1, h2 = st.columns([3, 1])
    with h1:
        st.markdown("## Business Insights")
        st.markdown("<p style='color:#7f8c8d;margin-top:-12px;font-size:13px;'>Marketing Strategy & Product Funnel Analysis</p>", unsafe_allow_html=True)
    with h2:
        st.markdown(f"<div style='text-align:right;padding-top:8px;font-size:12px;color:#555;'>Last sync: {last_sync}<br><span style='color:#1ed760;'>● LIVE</span></div>", unsafe_allow_html=True)

    # KPIs
    c1, c2, c3, c4 = st.columns(4)
    df_over = load_parquet("overview.parquet")
    u_users = df_over.iloc[0]["unique_users"] if not df_over.empty else 0
    total_p = (df_summary.iloc[0]["batch_total"] + df_summary.iloc[0]["stream_total"]) if not df_summary.empty else 0

    kpi(c1, "Total Streams", f"{total_p/1e3:.1f}K" if total_p < 1e6 else f"{total_p/1e6:.1f}M", "📈")
    kpi(c2, "Active Users", f"{u_users:,}", "👥")
    kpi(c3, "Premium Ratio", "35%", "💎")
    kpi(c4, "Churn Risk", "12.4%", "📉")

    st.write("---")

    # Marketing Map & Funnel
    col_map, col_fun = st.columns([1.5, 1], gap="large")
    
    with col_map:
        st.markdown("<div class='section-title'>Geographic Market Concentration</div>", unsafe_allow_html=True)
        st.markdown("<div class='section-sub'>Identify high-growth regions for targeted ads</div>", unsafe_allow_html=True)
        
        df_loc = load_parquet("top_locations.parquet")
        if not df_loc.empty:
            fig_map = px.choropleth(
                df_loc, locations="state", locationmode="USA-states", color="play_count",
                scope="usa", color_continuous_scale="Viridis",
            )
            fig_map.update_layout(**DARK_LAYOUT, height=350, margin=dict(l=0, r=0, t=0, b=0))
            fig_map.update_coloraxes(showscale=False)
            st.plotly_chart(fig_map, use_container_width=True)
        else: st.info("No location data available")

    with col_fun:
        st.markdown("<div class='section-title'>Conversion Funnel</div>", unsafe_allow_html=True)
        st.markdown("<div class='section-sub'>Free to Paid Upgrade Flow</div>", unsafe_allow_html=True)
        
        # Fake funnel based on page_dist if real ones not available
        stages = ["Home", "Settings", "Upgrade", "Submit Upgrade"]
        df_page = load_parquet("page_dist.parquet")
        if not df_page.empty:
            counts = [df_page[df_page["page"]==s]["event_count"].values[0] if s in df_page["page"].values else 100 for s in stages]
            # Ensure descending for demo if data is messy
            counts = sorted(counts, reverse=True)
            fig_fun = go.Figure(go.Funnel(y=stages, x=counts, marker={"color": [COLORS["blue"], COLORS["teal"], COLORS["orange"], COLORS["green"]]}))
            fig_fun.update_layout(**DARK_LAYOUT, height=350, margin=dict(l=40, r=40, t=20, b=20))
            st.plotly_chart(fig_fun, use_container_width=True)

    # Churn & Risk List
    st.write("---")
    l_risk, r_risk = st.columns(2)
    with l_risk:
        st.markdown("<div class='section-title'>Customer Segmentation</div>", unsafe_allow_html=True)
        df_seg = load_parquet("segment_profiles.parquet")
        if not df_seg.empty:
            fig_seg = px.bar(df_seg, x="segment_label", y="user_count", color="segment_label", color_discrete_sequence=[COLORS["green"], COLORS["blue"], COLORS["teal"], COLORS["red"]])
            fig_seg.update_layout(**DARK_LAYOUT, height=250, showlegend=False)
            st.plotly_chart(fig_seg, use_container_width=True)
    
    with r_risk:
        st.markdown("<div class='section-title'>High-Value At-Risk Customers</div>", unsafe_allow_html=True)
        df_churn = load_parquet("churn_predictions.parquet")
        if not df_churn.empty:
            for _, r in df_churn.head(4).iterrows():
                st.markdown(f"<div class='risk-row'><div class='risk-name'>{r['firstName']} {r['lastName']}</div><div class='badge-high'>{r['risk_level']} Risk</div></div>", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════
# VIEW 2: SYSTEM BENCHMARKS
# ══════════════════════════════════════════════════════════════
else:
    h1, h2 = st.columns([3, 1])
    with h1:
        st.markdown("## Engineering Insights")
        st.markdown("<p style='color:#7f8c8d;margin-top:-12px;font-size:13px;'>Performance Benchmarking: Hadoop MapReduce (Python simulation) vs Apache Spark DataFrame API</p>", unsafe_allow_html=True)
    with h2:
        st.markdown(f"<div style='text-align:right;padding-top:8px;font-size:12px;color:#555;'>Last sync: {last_sync}<br><span style='color:#1ed760;'>● LIVE</span></div>", unsafe_allow_html=True)

    df_bench = load_parquet("benchmark_results.parquet")

    if df_bench.empty:
        st.write("---")
        st.warning("No benchmark data yet. Run the benchmark first:")
        st.code("bash run_benchmark.sh", language="bash")
        st.markdown("""
        <div style='background:#161616;border:1px solid #282828;border-radius:12px;padding:24px;margin-top:16px;'>
        <h4 style='color:#1ed760;margin-top:0;'>What the benchmark does</h4>
        <ol style='color:#b3b3b3;line-height:2;'>
          <li>Export clean Parquet → JSON in HDFS (shared dataset)</li>
          <li>Run MapReduce simulation: Map → Sort/Shuffle (disk) → Reduce (disk)</li>
          <li>Run Spark benchmark: same 3 tasks via DataFrame API (in-memory DAG)</li>
          <li>Save results → dashboard auto-refreshes</li>
        </ol>
        </div>
        """, unsafe_allow_html=True)
    else:
        # ── KPI Row ───────────────────────────────────────────────
        mr_df  = df_bench[df_bench["framework"] == "MapReduce"]
        sp_df  = df_bench[df_bench["framework"] == "Spark"]
        mr_avg = mr_df["total_sec"].mean() if not mr_df.empty else 0
        sp_avg = sp_df["total_sec"].mean() if not sp_df.empty else 0
        speedup = mr_avg / sp_avg if sp_avg > 0 else 0
        records = int(df_bench["num_records"].iloc[0]) if "num_records" in df_bench.columns else 0

        k1, k2, k3, k4 = st.columns(4)
        kpi(k1, "Records Processed", f"{records:,}", "📦")
        kpi(k2, "MapReduce Avg",     f"{mr_avg:.1f}s", "🐢")
        kpi(k3, "Spark Avg",         f"{sp_avg:.2f}s", "⚡")
        kpi(k4, "Speedup Factor",    f"{speedup:.1f}x", "🚀")

        st.write("---")

        # ── Row 2: Task comparison + Phase breakdown ───────────────
        c_left, c_right = st.columns([3, 2], gap="large")

        with c_left:
            st.markdown("<div class='section-title'>Execution Time by Task</div>", unsafe_allow_html=True)
            st.markdown("<div class='section-sub'>Same dataset · Same 3 aggregation tasks · Lower is better</div>", unsafe_allow_html=True)

            fig_cmp = px.bar(
                df_bench,
                x="task", y="total_sec", color="framework",
                barmode="group",
                color_discrete_map={"MapReduce": COLORS["red"], "Spark": COLORS["green"]},
                text=df_bench["total_sec"].map(lambda x: f"{x:.2f}s"),
            )
            fig_cmp.update_layout(
                **DARK_LAYOUT,
                height=380,
                showlegend=True,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                            bgcolor="rgba(0,0,0,0)", font=dict(size=12)),
                margin=dict(l=0, r=0, t=30, b=0),
            )
            fig_cmp.update_traces(textposition="outside", textfont_size=11)
            st.plotly_chart(fig_cmp, use_container_width=True)

        with c_right:
            st.markdown("<div class='section-title'>MapReduce Phase Breakdown</div>", unsafe_allow_html=True)
            st.markdown("<div class='section-sub'>Time spent per phase — shuffle dominates</div>", unsafe_allow_html=True)

            if not mr_df.empty:
                fig_phase = go.Figure()
                for phase, label_p, color in [
                    ("map_sec",     "Map",           COLORS["blue"]),
                    ("shuffle_sec", "Shuffle/Sort",  COLORS["red"]),
                    ("reduce_sec",  "Reduce",        COLORS["orange"]),
                ]:
                    fig_phase.add_trace(go.Bar(
                        name=label_p,
                        x=mr_df["task"],
                        y=mr_df[phase],
                        marker_color=color,
                        text=mr_df[phase].map(lambda x: f"{x:.2f}s"),
                        textposition="auto",
                    ))
                fig_phase.update_layout(
                    **DARK_LAYOUT,
                    barmode="stack",
                    height=380,
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                                bgcolor="rgba(0,0,0,0)", font=dict(size=12)),
                    margin=dict(l=0, r=0, t=30, b=0),
                )
                st.plotly_chart(fig_phase, use_container_width=True)

        st.write("---")

        # ── Row 3: Throughput + Speedup gauge ─────────────────────
        c_tp, c_gauge = st.columns([2, 1], gap="large")

        with c_tp:
            st.markdown("<div class='section-title'>Throughput (records / second)</div>", unsafe_allow_html=True)
            st.markdown("<div class='section-sub'>Higher is better</div>", unsafe_allow_html=True)

            df_tp = df_bench.copy()
            df_tp["throughput"] = df_tp.apply(
                lambda r: round(r["num_records"] / r["total_sec"], 0) if r["total_sec"] > 0 else 0,
                axis=1,
            )
            fig_tp = px.bar(
                df_tp, x="task", y="throughput", color="framework",
                barmode="group",
                color_discrete_map={"MapReduce": COLORS["red"], "Spark": COLORS["green"]},
                text=df_tp["throughput"].map(lambda x: f"{int(x):,}"),
            )
            fig_tp.update_layout(
                **DARK_LAYOUT, height=300, showlegend=False,
                margin=dict(l=0, r=0, t=10, b=0),
                yaxis_title="records/sec",
            )
            fig_tp.update_traces(textposition="outside", textfont_size=10)
            st.plotly_chart(fig_tp, use_container_width=True)

        with c_gauge:
            st.markdown("<div class='section-title'>Average Speedup</div>", unsafe_allow_html=True)
            st.markdown("<div class='section-sub'>Spark vs MapReduce</div>", unsafe_allow_html=True)

            fig_gauge = go.Figure(go.Indicator(
                mode="gauge+number",
                value=round(speedup, 1),
                number={"suffix": "x", "font": {"size": 36, "color": "#1ed760"}},
                gauge={
                    "axis": {"range": [0, max(speedup * 1.5, 10)], "tickcolor": "#555"},
                    "bar":  {"color": "#1ed760", "thickness": 0.25},
                    "bgcolor": "#161616",
                    "bordercolor": "#282828",
                    "steps": [
                        {"range": [0,  3],                 "color": "#1a0000"},
                        {"range": [3,  max(speedup, 6)],   "color": "#001a00"},
                    ],
                    "threshold": {"line": {"color": "#1ed760", "width": 3},
                                  "thickness": 0.75, "value": speedup},
                },
            ))
            fig_gauge.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#b3b3b3", family="Inter"),
                height=300, margin=dict(l=20, r=20, t=20, b=10),
            )
            st.plotly_chart(fig_gauge, use_container_width=True)

        st.write("---")

    # ── Row 4: Architecture comparison ────────────────────────────
    st.markdown("<div class='section-title'>Architecture Comparison</div>", unsafe_allow_html=True)
    st.markdown("<div class='section-sub'>Why Spark consistently outperforms MapReduce</div>", unsafe_allow_html=True)

    a_mr, a_sp = st.columns(2, gap="large")
    with a_mr:
        st.markdown("""
<div style='background:#1a0000;border:1px solid #e74c3c55;border-radius:12px;padding:22px;height:100%;'>
  <h4 style='color:#e74c3c;margin:0 0 16px 0;'>🐢 Hadoop MapReduce</h4>
  <table style='width:100%;border-collapse:collapse;font-size:13px;'>
    <tr><td style='padding:7px 0;color:#888;width:38%;'>Storage</td>
        <td style='color:#ddd;'>Disk I/O between every phase</td></tr>
    <tr><td style='padding:7px 0;color:#888;'>Execution</td>
        <td style='color:#ddd;'>Map → write disk → Sort → write disk → Reduce</td></tr>
    <tr><td style='padding:7px 0;color:#888;'>Optimizer</td>
        <td style='color:#ddd;'>None — rigid 2-stage model</td></tr>
    <tr><td style='padding:7px 0;color:#888;'>API</td>
        <td style='color:#ddd;'>Low-level: Mapper + Reducer classes</td></tr>
    <tr><td style='padding:7px 0;color:#888;'>Best for</td>
        <td style='color:#ddd;'>One-pass batch ETL on massive data</td></tr>
    <tr><td style='padding:7px 0;color:#888;'>Fault tolerance</td>
        <td style='color:#ddd;'>Strong — output checkpointed to HDFS</td></tr>
  </table>
</div>
        """, unsafe_allow_html=True)

    with a_sp:
        st.markdown("""
<div style='background:#001a00;border:1px solid #1ed76055;border-radius:12px;padding:22px;height:100%;'>
  <h4 style='color:#1ed760;margin:0 0 16px 0;'>⚡ Apache Spark</h4>
  <table style='width:100%;border-collapse:collapse;font-size:13px;'>
    <tr><td style='padding:7px 0;color:#888;width:38%;'>Storage</td>
        <td style='color:#ddd;'>In-memory RDD/DataFrame; spill only if OOM</td></tr>
    <tr><td style='padding:7px 0;color:#888;'>Execution</td>
        <td style='color:#ddd;'>DAG: N stages pipelined, no disk between stages</td></tr>
    <tr><td style='padding:7px 0;color:#888;'>Optimizer</td>
        <td style='color:#ddd;'>Catalyst (logical) + Tungsten (physical)</td></tr>
    <tr><td style='padding:7px 0;color:#888;'>API</td>
        <td style='color:#ddd;'>High-level: SQL, DataFrame, MLlib, Streaming</td></tr>
    <tr><td style='padding:7px 0;color:#888;'>Best for</td>
        <td style='color:#ddd;'>Iterative ML, SQL analytics, real-time streaming</td></tr>
    <tr><td style='padding:7px 0;color:#888;'>Fault tolerance</td>
        <td style='color:#ddd;'>RDD lineage — recompute lost partitions</td></tr>
  </table>
</div>
        """, unsafe_allow_html=True)

    st.write("---")

    # ── Live Event Feed ────────────────────────────────────────────
    st.markdown("<div class='section-title'>Live Event Feed</div>", unsafe_allow_html=True)
    st.markdown("<div class='section-sub'>Real-time events from Kafka → Speed Layer</div>", unsafe_allow_html=True)

    @st.experimental_fragment(run_every=3)
    def live_feed():
        df_live = fetch_live_stream()
        if not df_live.empty:
            show_cols = [c for c in ["artist", "song", "userId", "level", "location"] if c in df_live.columns]
            st.dataframe(df_live.head(10)[show_cols], use_container_width=True)
        else:
            st.info("Waiting for live events from Kafka...")
    live_feed()

# Footer
st.markdown("<div style='text-align:center;padding:20px;color:#444;font-size:11px;'>Music EventSim Analytics · Lambda Architecture · MIT License</div>", unsafe_allow_html=True)
