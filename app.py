"""
dashboard/app.py
─────────────────
Step 29/30 — Final Project: End-to-end pipeline dashboard

Run with:
  pip install streamlit plotly
  streamlit run dashboard/app.py

The dashboard reads directly from:
  data/warehouse/violations.db  (SQLite — loaded by loader.py)
  data/processed/violations.csv (fallback if no DB)
  data/processed/zone_aggregates.csv

And provides:
  • Live KPI cards
  • Interactive charts (Plotly)
  • Pipeline health monitor
  • Data quality report viewer
  • Anomaly detection results
  • Raw violation table with filters
  • Airflow-style run log
"""

import sqlite3
import json
import time
import random
import logging
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

try:
    import streamlit as st
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    STREAMLIT = True
except ImportError:
    STREAMLIT = False
    print("Streamlit not installed. Run: pip install streamlit plotly")

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────────────────────────────────────────────

DB_PATH  = "data/warehouse/violations.db"
CSV_PATH = "data/processed/violations.csv"
AGG_PATH = "data/processed/zone_aggregates.csv"

@st.cache_data(ttl=60)  # refresh every 60 seconds
def load_data() -> pd.DataFrame:
    """Load violations data from SQLite or CSV fallback."""
    if Path(DB_PATH).exists():
        with sqlite3.connect(DB_PATH) as conn:
            try:
                df = pd.read_sql("SELECT * FROM fact_violation LIMIT 100000", conn)
                return df
            except Exception:
                pass

    if Path(CSV_PATH).exists():
        return pd.read_csv(CSV_PATH, nrows=100_000)

    # Generate synthetic data if nothing exists
    return _generate_demo_data()


@st.cache_data(ttl=60)
def load_zone_aggregates() -> pd.DataFrame:
    if Path(AGG_PATH).exists():
        return pd.read_csv(AGG_PATH)
    df = load_data()
    return _compute_zone_agg(df)


@st.cache_data(ttl=300)
def load_dq_report() -> dict:
    report_path = Path("data/reports/dq_report.md")
    if report_path.exists():
        return {"text": report_path.read_text(), "exists": True}
    return {"text": "Run main.py to generate DQ report.", "exists": False}


def _generate_demo_data(n=50_000) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    TYPES  = ["Speeding","Red-Light","DUI","Illegal Park","Reckless","Mobile Use","No Seatbelt"]
    ZONES  = ["Downtown","Highway","School Zone","Residential","Industrial"]
    VEHCLS = ["Car","Truck","Motorcycle","Bus","Van"]

    vtype = rng.choice(TYPES, n)
    zone  = rng.choice(ZONES, n)
    speed = np.where(vtype=="Speeding", rng.integers(5,80,n), rng.integers(0,25,n)).astype(float)
    sev_map = {"DUI":"High","Reckless":"High","Red-Light":"High","Mobile Use":"Medium"}
    severity = [sev_map.get(t, "High" if (t=="Speeding" and s>30) else
                              ("Medium" if (t=="Speeding" and s>15) else "Low"))
                for t, s in zip(vtype, speed)]
    FINES = {"Downtown":{"High":500,"Medium":250,"Low":100},
             "Highway":{"High":600,"Medium":300,"Low":150},
             "School Zone":{"High":800,"Medium":400,"Low":200},
             "Residential":{"High":350,"Medium":175,"Low":75},
             "Industrial":{"High":400,"Medium":200,"Low":100}}
    fine = [FINES[z][s] for z, s in zip(zone, severity)]
    hours = rng.integers(0, 24, n)
    return pd.DataFrame({
        "violation_id":   [f"TRF-{i:06d}" for i in range(n)],
        "violation_type": vtype, "zone": zone,
        "speed_kmh": speed, "fine_amount": fine,
        "severity": severity, "hour": hours,
        "vehicle_type": rng.choice(VEHCLS, n),
        "officer_id": [f"OFF-{rng.integers(1,50):03d}" for _ in range(n)],
        "is_peak": [(h in range(7,10) or h in range(17,20)) for h in hours],
        "day_of_week": rng.choice(["Mon","Tue","Wed","Thu","Fri","Sat","Sun"], n),
    })


def _compute_zone_agg(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include=["category"]).columns:
        df[col] = df[col].astype(str)
    return (df.groupby(["zone","violation_type"], observed=True)
              .agg(total_violations=("violation_type","size"),
                   avg_fine=("fine_amount","mean"),
                   total_revenue=("fine_amount","sum"))
              .round(2).reset_index())


# ─────────────────────────────────────────────────────────────────────────────
# STREAMLIT APP
# ─────────────────────────────────────────────────────────────────────────────

def run_dashboard():
    st.set_page_config(
        page_title="Traffic Violations Pipeline",
        page_icon="🚦",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # ── Custom CSS ────────────────────────────────────────────────────────────
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500&family=Syne:wght@600;700&display=swap');
    .main { background: #0a0d14; }
    .block-container { padding-top: 1rem; }
    .metric-card { background: #161c2e; border: 1px solid #1e2840;
                   border-radius: 8px; padding: 1rem; }
    .pipeline-badge { display:inline-block; padding:2px 10px; border-radius:3px;
                      font-size:11px; font-family:monospace; border:1px solid; }
    h1,h2,h3 { font-family: 'Syne', sans-serif; }
    </style>
    """, unsafe_allow_html=True)

    # ── Sidebar ───────────────────────────────────────────────────────────────
    with st.sidebar:
        st.title("🚦 Traffic Pipeline")
        st.caption("Data Engineering — All 30 Steps")
        st.divider()

        page = st.radio("Navigate", [
            "📊 Dashboard",
            "🏗️ Pipeline Monitor",
            "✅ Data Quality",
            "⚡ Streaming Monitor",
            "☁️ Cloud Status",
            "🔬 Anomaly Detection",
            "📋 Raw Data",
        ])

        st.divider()
        st.caption(f"Last refresh: {datetime.now().strftime('%H:%M:%S')}")
        if st.button("🔄 Refresh data"):
            st.cache_data.clear()
            st.rerun()

    df       = load_data()
    zone_agg = load_zone_aggregates()

    # coerce types
    for col in df.select_dtypes(include=["category"]).columns:
        df[col] = df[col].astype(str)
    if "fine_amount" in df.columns:
        df["fine_amount"] = pd.to_numeric(df["fine_amount"], errors="coerce")
    if "speed_kmh" in df.columns:
        df["speed_kmh"]   = pd.to_numeric(df["speed_kmh"],   errors="coerce")

    # ── Page: Dashboard ──────────────────────────────────────────────────────
    if page == "📊 Dashboard":
        st.title("Traffic Violations Analytics Dashboard")
        st.caption("Step 29/30 — End-to-end pipeline output visualization")

        # KPI cards
        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("Total violations", f"{len(df):,}")
        col2.metric("Total revenue",    f"${df['fine_amount'].sum():,.0f}")
        col3.metric("Avg fine",         f"${df['fine_amount'].mean():.0f}")
        col4.metric("High severity",
                    f"{(df['severity']=='High').sum():,}",
                    f"{(df['severity']=='High').mean()*100:.1f}%")
        col5.metric("Officers active",
                    df["officer_id"].nunique() if "officer_id" in df.columns else "N/A")

        st.divider()

        # Row 1: Violations by type + Hourly trend
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Violations by type")
            type_counts = df["violation_type"].value_counts().reset_index()
            type_counts.columns = ["type","count"]
            fig = px.bar(type_counts, x="count", y="type", orientation="h",
                         color="count", color_continuous_scale="Blues",
                         template="plotly_dark")
            fig.update_layout(showlegend=False, yaxis_title="", xaxis_title="Count",
                              margin=dict(l=0,r=0,t=10,b=0), height=300)
            st.plotly_chart(fig, use_container_width=True)

        with c2:
            st.subheader("Hourly violation trend")
            if "hour" in df.columns:
                hourly = df.groupby("hour").agg(
                    count=("violation_type","size"),
                    avg_fine=("fine_amount","mean")
                ).reset_index()
                fig = make_subplots(specs=[[{"secondary_y": True}]])
                fig.add_trace(go.Bar(x=hourly["hour"], y=hourly["count"],
                                     name="Violations", marker_color="#00e5ff",
                                     opacity=0.7), secondary_y=False)
                fig.add_trace(go.Scatter(x=hourly["hour"], y=hourly["avg_fine"],
                                         name="Avg Fine $", line=dict(color="#ff6b35", width=2)),
                              secondary_y=True)
                fig.update_layout(template="plotly_dark", height=300,
                                  margin=dict(l=0,r=0,t=10,b=0))
                st.plotly_chart(fig, use_container_width=True)

        # Row 2: Severity donut + Zone revenue
        c3, c4 = st.columns(2)
        with c3:
            st.subheader("Severity distribution")
            sev = df["severity"].value_counts()
            fig = px.pie(values=sev.values, names=sev.index,
                         color_discrete_sequence=["#ff6b35","#f59e0b","#10b981"],
                         hole=0.6, template="plotly_dark")
            fig.update_layout(height=280, margin=dict(l=0,r=0,t=10,b=0))
            st.plotly_chart(fig, use_container_width=True)

        with c4:
            st.subheader("Revenue by zone")
            zone_rev = df.groupby("zone")["fine_amount"].sum().reset_index()
            zone_rev.columns = ["zone","revenue"]
            fig = px.bar(zone_rev.sort_values("revenue", ascending=True),
                         x="revenue", y="zone", orientation="h",
                         color="revenue", color_continuous_scale="Purples",
                         template="plotly_dark")
            fig.update_layout(showlegend=False, height=280,
                              margin=dict(l=0,r=0,t=10,b=0))
            st.plotly_chart(fig, use_container_width=True)

        # Row 3: Heatmap hour × day
        st.subheader("Violation heatmap — hour × day of week")
        if "hour" in df.columns and "day_of_week" in df.columns:
            days_order = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
            heat = df.groupby(["day_of_week","hour"]).size().reset_index(name="count")
            heat_pivot = heat.pivot(index="day_of_week", columns="hour", values="count").fillna(0)
            heat_pivot = heat_pivot.reindex([d for d in days_order if d in heat_pivot.index])
            fig = px.imshow(heat_pivot, aspect="auto",
                            color_continuous_scale="Blues",
                            template="plotly_dark",
                            labels={"x":"Hour","y":"Day","color":"Violations"})
            fig.update_layout(height=260, margin=dict(l=0,r=0,t=10,b=0))
            st.plotly_chart(fig, use_container_width=True)

    # ── Page: Pipeline Monitor ────────────────────────────────────────────────
    elif page == "🏗️ Pipeline Monitor":
        st.title("Pipeline Architecture Monitor")
        st.caption("Steps 1–30 execution status")

        stages = [
            ("01", "Linux Setup",          "data/logs",                         "Steps 1-3"),
            ("02", "Python ETL",           "data/processed/violations.csv",     "Steps 4-6"),
            ("03", "SQL Warehouse",        "data/warehouse/violations.db",      "Steps 8-11"),
            ("04", "HDFS Upload",          "data/hdfs/namenode/fsimage.json",   "Steps 11-12"),
            ("05", "Spark Processing",     "data/spark_output",                 "Steps 13-16"),
            ("06", "Kafka Streaming",      "data/hdfs",                         "Steps 17-20"),
            ("07", "Airflow Orchestration","dags/traffic_pipeline_dag.py",      "Steps 21-22"),
            ("08", "Cloud Upload",         "data/cloud_sim",                    "Steps 23-25"),
            ("09", "BigQuery Analytics",   "data/cloud_sim/s3",                 "Step 26"),
            ("10", "Delta Lake",           "data/delta_lake",                   "Step 27"),
            ("11", "Data Quality",         "data/reports/dq_report.md",         "Step 28"),
            ("12", "Dashboard",            "dashboard/app.py",                  "Steps 29-30"),
        ]

        cols = st.columns(4)
        for i, (num, name, check_path, steps) in enumerate(stages):
            exists = Path(check_path).exists()
            status = "✅ DONE" if exists else "⏳ PENDING"
            color  = "green"  if exists else "orange"
            with cols[i % 4]:
                st.markdown(f"""
                <div style="background:#161c2e;border:1px solid #1e2840;border-radius:8px;
                            padding:.75rem;margin-bottom:.5rem;">
                  <div style="font-size:10px;color:#64748b">{steps}</div>
                  <div style="font-family:monospace;font-size:12px;color:#e2e8f0;margin:.2rem 0">
                    {num}. {name}</div>
                  <div style="font-size:11px;color:{'#10b981' if exists else '#f59e0b'}">{status}</div>
                </div>
                """, unsafe_allow_html=True)

        st.divider()
        st.subheader("Run full pipeline")
        col_a, col_b, col_c = st.columns(3)
        with col_a:
            if st.button("▶ Run main.py"):
                with st.spinner("Running pipeline..."):
                    import subprocess
                    result = subprocess.run(["python", "main.py"],
                                           capture_output=True, text=True, timeout=120)
                    st.code(result.stdout[-3000:] if result.stdout else result.stderr)
        with col_b:
            if st.button("▶ Run Kafka sim"):
                with st.spinner("Running Kafka simulation..."):
                    import sys
                    sys.path.insert(0, ".")
                    from pipeline.kafka_streaming import run_kafka_simulation
                    r = run_kafka_simulation(n_events=200)
                    st.json(r)
        with col_c:
            if st.button("▶ Run cloud upload"):
                with st.spinner("Running cloud pipeline..."):
                    import sys
                    sys.path.insert(0, ".")
                    from pipeline.cloud import upload_to_cloud
                    r = upload_to_cloud()
                    st.json(r)

        # Pipeline log
        st.subheader("Latest pipeline log")
        log_files = sorted(Path("data/logs").glob("*.log")) if Path("data/logs").exists() else []
        if log_files:
            log_text = log_files[-1].read_text()
            st.code(log_text[-4000:], language="bash")
        else:
            st.info("No log files yet. Run the pipeline first.")

    # ── Page: Data Quality ────────────────────────────────────────────────────
    elif page == "✅ Data Quality":
        st.title("Data Quality Dashboard")
        st.caption("Step 28 — Validation checks and anomaly detection")

        import sys
        sys.path.insert(0, ".")
        from pipeline.quality import run_dq_checks, detect_anomalies

        with st.spinner("Running DQ checks..."):
            dq = run_dq_checks(df)
            anomalies = detect_anomalies(df, col="fine_amount")

        # Score gauge
        score = dq["score"]
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("DQ Score", f"{score:.1f}%",
                    delta="PASS" if dq["passed"] else "FAIL")
        col2.metric("Checks passed", f"{dq['n_pass']}/{dq['n_pass']+dq['n_fail']}")
        col3.metric("Anomalies found", len(anomalies))
        col4.metric("Total rows", f"{len(df):,}")

        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=score,
            domain={"x":[0,1],"y":[0,1]},
            gauge={
                "axis": {"range":[0,100]},
                "bar":  {"color": "#10b981" if score >= 90 else "#f59e0b"},
                "steps": [{"range":[0,60],"color":"#1a0a0a"},
                           {"range":[60,90],"color":"#1a1200"},
                           {"range":[90,100],"color":"#0a1a0a"}],
                "threshold": {"line":{"color":"white","width":2},"value":90},
            },
            title={"text": "Data Quality Score (%)"},
        ))
        fig.update_layout(template="plotly_dark", height=260)
        st.plotly_chart(fig, use_container_width=True)

        # Check table
        st.subheader("Individual check results")
        check_df = pd.DataFrame(dq["checks"])
        if not check_df.empty:
            def color_status(val):
                if val == "PASS":  return "color: #10b981"
                if val == "FAIL":  return "color: #ff6b35"
                return "color: #f59e0b"
            st.dataframe(check_df.style.applymap(color_status, subset=["status"]),
                         use_container_width=True)

        # Anomalies
        if len(anomalies):
            st.subheader(f"Anomalies detected ({len(anomalies)} records)")
            disp_cols = [c for c in ["violation_id","zone","violation_type",
                                      "fine_amount","anomaly_reason"] if c in anomalies.columns]
            st.dataframe(anomalies[disp_cols].head(50), use_container_width=True)
        else:
            st.success("No anomalies detected in fine_amount column.")

        # DQ report markdown
        dq_report = load_dq_report()
        with st.expander("View full DQ report"):
            st.markdown(dq_report["text"])

    # ── Page: Streaming Monitor ───────────────────────────────────────────────
    elif page == "⚡ Streaming Monitor":
        st.title("Kafka Streaming Monitor")
        st.caption("Steps 17-20 — Real-time violation event pipeline")

        import sys
        sys.path.insert(0, ".")
        from pipeline.kafka_streaming import (
            SimulatedKafkaBroker, TOPIC_VIOLATIONS,
            generate_violation_event, run_kafka_simulation
        )

        col1, col2, col3 = st.columns(3)
        n_events = col1.slider("Events to simulate", 50, 1000, 200, 50)
        rate     = col2.slider("Events per second",   10,  200, 50,  10)

        if col3.button("▶ Run Kafka simulation"):
            with st.spinner(f"Simulating {n_events} events..."):
                result = run_kafka_simulation(n_events=n_events, rate_per_sec=rate)
            st.success(f"Produced: {result['produced']} | Consumed: {result['consumed']}")

            broker = SimulatedKafkaBroker()
            stats  = broker.topic_stats(TOPIC_VIOLATIONS)
            if stats:
                st.subheader("Partition distribution")
                part_df = pd.DataFrame([
                    {"partition": k, "messages": v}
                    for k, v in stats.get("partitions", {}).items()
                ])
                if not part_df.empty:
                    fig = px.bar(part_df, x="partition", y="messages",
                                 template="plotly_dark",
                                 color="messages", color_continuous_scale="Teal")
                    fig.update_layout(height=300)
                    st.plotly_chart(fig, use_container_width=True)

            st.subheader("Sample events")
            if result.get("sample_records"):
                st.dataframe(pd.DataFrame(result["sample_records"]),
                             use_container_width=True)

        st.divider()
        st.subheader("Structured Streaming job (Step 20)")
        st.info("Deploy this on a Spark cluster with Kafka. Requires: pyspark, delta-spark, kafka connector jar.")
        from pipeline.kafka_streaming import STRUCTURED_STREAMING_CODE
        st.code(STRUCTURED_STREAMING_CODE, language="python")

    # ── Page: Cloud Status ────────────────────────────────────────────────────
    elif page == "☁️ Cloud Status":
        st.title("Cloud Infrastructure Status")
        st.caption("Steps 23-27 — AWS, GCP, Delta Lake")

        import sys
        sys.path.insert(0, ".")
        from pipeline.cloud import CLOUD_COMPARISON, DeltaLakeSimulator

        st.subheader("Cloud provider comparison (Step 23)")
        for category, providers in CLOUD_COMPARISON.items():
            with st.expander(f"📦 {category.replace('_',' ').title()}"):
                comp_df = pd.DataFrame(providers).T
                st.dataframe(comp_df, use_container_width=True)

        st.divider()
        st.subheader("S3 / GCS uploaded files (Step 24)")
        sim_s3 = Path("data/cloud_sim/s3")
        if sim_s3.exists():
            files = [str(p.relative_to(sim_s3)) for p in sim_s3.rglob("*") if p.is_file()]
            if files:
                st.dataframe(pd.DataFrame({"s3_key": files,
                                           "uri": [f"s3://traffic-pipeline-bucket/{f}" for f in files]}),
                             use_container_width=True)
            else:
                st.info("No files uploaded yet. Run cloud upload from Pipeline Monitor.")
        else:
            st.info("Run cloud upload first.")

        st.divider()
        st.subheader("Delta Lake transaction history (Step 27)")
        delta_path = Path("data/delta_lake/violations")
        if delta_path.exists():
            delta = DeltaLakeSimulator(str(delta_path))
            history = delta.history()
            if history:
                hist_df = pd.DataFrame([{
                    "version":   h["version"],
                    "operation": h["operation"],
                    "timestamp": h["timestamp"][:19],
                    "rows":      h.get("operationParameters",{}).get("numRows","—"),
                } for h in history])
                st.dataframe(hist_df, use_container_width=True)

                if st.button("🔙 Time travel to version 0"):
                    v0 = delta.read(version=0)
                    st.write(f"Version 0: {len(v0):,} rows")
                    st.dataframe(v0.head(10), use_container_width=True)
            else:
                st.info("No Delta transactions yet.")
        else:
            st.info("Run cloud upload to initialise Delta Lake.")

    # ── Page: Anomaly Detection ───────────────────────────────────────────────
    elif page == "🔬 Anomaly Detection":
        st.title("Anomaly Detection")
        st.caption("Step 28 — Z-score + IQR statistical outlier detection")

        import sys
        sys.path.insert(0, ".")
        from pipeline.quality import detect_anomalies, detect_temporal_spikes

        col1, col2 = st.columns(2)
        target_col = col1.selectbox("Target column", ["fine_amount","speed_kmh"])
        method     = col2.selectbox("Method", ["combined","zscore","iqr"])
        z_thresh   = st.slider("Z-score threshold", 1.5, 5.0, 3.0, 0.5)

        with st.spinner("Detecting anomalies..."):
            anomalies = detect_anomalies(df, col=target_col, method=method,
                                         z_threshold=z_thresh)
            spikes    = detect_temporal_spikes(df)

        st.metric("Anomalies found", len(anomalies),
                  f"{len(anomalies)/len(df)*100:.2f}% of dataset")

        # Scatter: value distribution with anomalies highlighted
        plot_df = df[[target_col]].copy().dropna()
        plot_df["is_anomaly"] = plot_df.index.isin(anomalies.index)
        plot_df["idx"] = range(len(plot_df))
        fig = px.scatter(plot_df, x="idx", y=target_col,
                         color="is_anomaly",
                         color_discrete_map={False:"#00e5ff", True:"#ff6b35"},
                         template="plotly_dark",
                         labels={"idx":"Record index", target_col:target_col},
                         title=f"Anomaly scatter — {target_col}")
        fig.update_traces(marker_size=3)
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)

        if len(spikes):
            st.subheader("Temporal spikes (hourly)")
            st.dataframe(spikes, use_container_width=True)

        if len(anomalies):
            st.subheader("Anomalous records")
            disp = [c for c in ["violation_id","zone","violation_type",
                                  target_col,"severity","anomaly_reason"] if c in anomalies.columns]
            st.dataframe(anomalies[disp].head(100), use_container_width=True)

    # ── Page: Raw Data ────────────────────────────────────────────────────────
    elif page == "📋 Raw Data":
        st.title("Violations Explorer")
        st.caption("Filtered view of processed violations dataset")

        c1, c2, c3 = st.columns(3)
        zones     = ["All"] + sorted(df["zone"].dropna().unique().tolist())
        types     = ["All"] + sorted(df["violation_type"].dropna().unique().tolist())
        severities= ["All"] + ["High","Medium","Low"]

        sel_zone = c1.selectbox("Zone", zones)
        sel_type = c2.selectbox("Violation type", types)
        sel_sev  = c3.selectbox("Severity", severities)

        filt = df.copy()
        if sel_zone != "All":      filt = filt[filt["zone"]==sel_zone]
        if sel_type != "All":      filt = filt[filt["violation_type"]==sel_type]
        if sel_sev  != "All":      filt = filt[filt["severity"]==sel_sev]

        st.caption(f"Showing {len(filt):,} of {len(df):,} records")

        disp_cols = [c for c in ["violation_id","violation_type","zone","speed_kmh",
                                   "fine_amount","severity","hour","vehicle_type","officer_id"]
                     if c in filt.columns]
        st.dataframe(filt[disp_cols].head(500), use_container_width=True)

        # Download
        csv = filt.to_csv(index=False).encode()
        st.download_button("⬇ Download filtered CSV", csv,
                           f"violations_{sel_zone}_{sel_type}.csv", "text/csv")


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if STREAMLIT:
        run_dashboard()
    else:
        print("Install Streamlit: pip install streamlit plotly")
        print("Then run:         streamlit run dashboard/app.py")
