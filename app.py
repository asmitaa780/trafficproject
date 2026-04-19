

import os, sys, json, sqlite3, logging, random
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

sys.path.insert(0, str(Path(__file__).parent.parent))
logger = logging.getLogger(__name__)

# ── Domain constants ──────────────────────────────────────────────────────────
VIOLATION_TYPES = ["Speeding","Red-Light","DUI","Illegal Park","Reckless","Mobile Use","No Seatbelt"]
ZONES           = ["Downtown","Highway","School Zone","Residential","Industrial"]
VEHICLES        = ["Car","Truck","Motorcycle","Bus","Van"]
FINE_MAP = {
    "Downtown":    {"High":500,"Medium":250,"Low":100},
    "Highway":     {"High":600,"Medium":300,"Low":150},
    "School Zone": {"High":800,"Medium":400,"Low":200},
    "Residential": {"High":350,"Medium":175,"Low": 75},
    "Industrial":  {"High":400,"Medium":200,"Low":100},
}

def _sev(vtype, speed):
    if vtype in ("DUI","Reckless","Red-Light"): return "High"
    if vtype == "Speeding": return "High" if speed>30 else ("Medium" if speed>15 else "Low")
    if vtype == "Mobile Use": return "Medium"
    return "Low"

# ── Synthetic data ────────────────────────────────────────────────────────────
def generate_data(n=50_000):
    rng   = np.random.default_rng(42)
    vtype = rng.choice(VIOLATION_TYPES, n)
    zone  = rng.choice(ZONES, n)
    speed = np.where(vtype=="Speeding", rng.integers(5,80,n), rng.integers(0,25,n)).astype(float)
    sev   = [_sev(t,s) for t,s in zip(vtype,speed)]
    fine  = [FINE_MAP[z][s] for z,s in zip(zone,sev)]
    hours = rng.integers(0,24,n)
    days  = rng.choice(["Mon","Tue","Wed","Thu","Fri","Sat","Sun"],n)
    return pd.DataFrame({
        "violation_id":   [f"TRF-{i:06d}" for i in range(n)],
        "violation_type": vtype, "zone": zone,
        "speed_kmh": speed, "fine_amount": fine,
        "severity": sev, "hour": hours, "day_of_week": days,
        "vehicle_type": rng.choice(VEHICLES,n),
        "officer_id": [f"OFF-{rng.integers(1,50):03d}" for _ in range(n)],
        "is_peak": [(h in range(7,10) or h in range(17,20)) for h in hours],
    })

# ── DQ checks (embedded) ─────────────────────────────────────────────────────
def run_dq(df):
    checks, passed, failed = [], 0, 0
    def chk(dim, field, ok, detail):
        nonlocal passed, failed
        passed += ok; failed += not ok
        checks.append({"dimension":dim,"field":field,
                        "status":"PASS" if ok else "FAIL","detail":detail})
    for col,thr in [("violation_type",1.0),("zone",1.0),("severity",1.0),("fine_amount",0.95)]:
        if col in df.columns:
            r = 1-df[col].isna().mean()
            chk("Completeness",col,r>=thr,f"{r*100:.1f}% complete (need {thr*100:.0f}%)")
    for col,lo,hi in [("speed_kmh",0,200),("fine_amount",0,5000)]:
        if col in df.columns:
            vals = pd.to_numeric(df[col],errors="coerce")
            oob  = (~vals.between(lo,hi)&vals.notna()).mean()
            chk("Validity",col,oob<0.01,f"{oob*100:.2f}% out of [{lo},{hi}]")
    if "violation_id" in df.columns:
        dup = df["violation_id"].duplicated().mean()
        chk("Uniqueness","violation_id",dup==0,f"{dup*100:.3f}% duplicates")
    if {"speed_kmh","violation_type"}.issubset(df.columns):
        sp  = df[df["violation_type"].astype(str)=="Speeding"]
        bad = (sp["speed_kmh"]<=0).mean() if len(sp) else 0
        chk("Consistency","speed/type",bad<0.02,f"{bad*100:.1f}% Speeding with speed<=0")
    total = passed+failed
    score = round(passed/total*100,1) if total else 0.0
    return {"score":score,"passed":failed==0,"n_pass":passed,"n_fail":failed,
            "checks":checks,"run_at":datetime.now().isoformat()}

# ── Anomaly detection (embedded) ─────────────────────────────────────────────
def detect_anomalies(df, col="fine_amount", z_thr=3.0, method="combined"):
    if col not in df.columns: return pd.DataFrame()
    s = pd.to_numeric(df[col],errors="coerce").dropna()
    w = df.loc[s.index].copy()
    reasons = pd.Series("",index=w.index)
    z = np.abs((s-s.mean())/s.std())
    zf = z>z_thr;  reasons[zf] += "Z-score outlier; "
    Q1,Q3 = s.quantile(0.25),s.quantile(0.75); iqr=Q3-Q1
    qf = (s<Q1-1.5*iqr)|(s>Q3+1.5*iqr); reasons[qf] += "IQR outlier; "
    flag = {"zscore":zf,"iqr":qf}.get(method, zf|qf)
    out  = w[flag].copy()
    out["anomaly_reason"] = reasons[flag]
    out["anomaly_value"]  = s[flag]
    return out.reset_index(drop=True)

def detect_spikes(df, window=3):
    if "hour" not in df.columns: return pd.DataFrame()
    h  = df.groupby("hour").size().rename("count").reset_index()
    rm = h["count"].rolling(window,center=True,min_periods=1).mean()
    rs = h["count"].rolling(window,center=True,min_periods=1).std().fillna(0)
    return h[h["count"]>rm+2*rs].copy().reset_index(drop=True)

# ── Kafka simulation (embedded) ───────────────────────────────────────────────
def kafka_sim(n=200):
    parts = {i:0 for i in range(5)}
    recs  = []
    for i in range(n):
        vt = random.choice(VIOLATION_TYPES)
        z  = random.choice(ZONES)
        sp = random.randint(5,80) if vt=="Speeding" else random.randint(0,20)
        sv = _sev(vt,sp); fine = FINE_MAP[z][sv]
        p  = hash(z)%5; parts[p]+=1
        recs.append({"zone":z,"type":vt,"fine":fine,"partition":p,"offset":i})
    return {"produced":n,"consumed":n,
            "partition_stats":{"partitions":parts,"total_msgs":n},
            "consumer_lag":{p:0 for p in range(5)},
            "sample_records":recs[:5]}

CLOUD_COMPARISON = {
    "Object Storage": {"AWS":{"Service":"S3","Cost/GB":"$0.023"},"GCP":{"Service":"Cloud Storage","Cost/GB":"$0.020"},"Azure":{"Service":"Blob","Cost/GB":"$0.018"}},
    "Data Warehouse": {"AWS":{"Service":"Redshift","Serverless":"Yes"},"GCP":{"Service":"BigQuery","Serverless":"Yes"},"Azure":{"Service":"Synapse","Serverless":"Yes"}},
    "Managed Spark":  {"AWS":{"Service":"EMR"},"GCP":{"Service":"Dataproc"},"Azure":{"Service":"Databricks"}},
    "Streaming":      {"AWS":{"Service":"Kinesis","Kafka":"MSK"},"GCP":{"Service":"Pub/Sub","Kafka":"Confluent"},"Azure":{"Service":"Event Hubs","Kafka":"Built-in"}},
}

# ── Data loading ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=60)
def load_data():
    for path,reader in [
        ("data/warehouse/violations.db", lambda p: pd.read_sql("SELECT * FROM fact_violation LIMIT 100000", sqlite3.connect(p))),
        ("data/processed/violations.csv", lambda p: pd.read_csv(p, nrows=100_000)),
    ]:
        if Path(path).exists():
            try:
                df = reader(path)
                if len(df)>0:
                    for c in df.select_dtypes(["category"]).columns: df[c]=df[c].astype(str)
                    for c in ["fine_amount","speed_kmh","hour"]:
                        if c in df.columns: df[c]=pd.to_numeric(df[c],errors="coerce")
                    return df
            except: pass
    return generate_data(50_000)

# ── App ───────────────────────────────────────────────────────────────────────
def main():
    st.set_page_config(page_title="Traffic Violations Pipeline",
                       page_icon="🚦", layout="wide")
    st.markdown("<style>.block-container{padding-top:1rem}</style>",
                unsafe_allow_html=True)

    with st.sidebar:
        st.title("🚦 Traffic Pipeline")
        st.caption("Data Engineering — All 30 Steps")
        st.divider()
        page = st.radio("Navigate",[
            "📊 Dashboard","🏗️ Pipeline Monitor","✅ Data Quality",
            "⚡ Streaming Monitor","☁️ Cloud Status","🔬 Anomaly Detection","📋 Raw Data"])
        st.divider()
        st.caption(f"Refreshed: {datetime.now().strftime('%H:%M:%S')}")
        if st.button("🔄 Refresh"): st.cache_data.clear(); st.rerun()

    df = load_data()

    # ── PAGE 1: Dashboard ─────────────────────────────────────────────────────
    if page == "📊 Dashboard":
        st.title("Traffic Violations Analytics Dashboard")
        c1,c2,c3,c4,c5 = st.columns(5)
        c1.metric("Total Violations", f"{len(df):,}")
        c2.metric("Total Revenue",    f"${df['fine_amount'].sum():,.0f}")
        c3.metric("Average Fine",     f"${df['fine_amount'].mean():.0f}")
        c4.metric("High Severity",    f"{(df['severity']=='High').sum():,}",
                                      f"{(df['severity']=='High').mean()*100:.1f}%")
        c5.metric("Officers Active",  str(df["officer_id"].nunique()) if "officer_id" in df.columns else "—")
        st.divider()

        col1,col2 = st.columns(2)
        with col1:
            st.subheader("Violations by type")
            tc = df["violation_type"].value_counts().reset_index()
            tc.columns = ["type","count"]
            fig = px.bar(tc,x="count",y="type",orientation="h",
                         color="count",color_continuous_scale="Blues",template="plotly_dark")
            fig.update_layout(showlegend=False,height=300,margin=dict(l=0,r=0,t=10,b=0),yaxis_title="")
            st.plotly_chart(fig,use_container_width=True)
        with col2:
            st.subheader("Violations by hour")
            if "hour" in df.columns:
                hr = df.groupby("hour").agg(count=("violation_type","size"),avg_fine=("fine_amount","mean")).reset_index()
                fig = make_subplots(specs=[[{"secondary_y":True}]])
                fig.add_trace(go.Bar(x=hr["hour"],y=hr["count"],name="Violations",marker_color="#00b4d8",opacity=0.8),secondary_y=False)
                fig.add_trace(go.Scatter(x=hr["hour"],y=hr["avg_fine"].round(0),name="Avg Fine $",line=dict(color="#ff6b35",width=2)),secondary_y=True)
                fig.update_layout(template="plotly_dark",height=300,margin=dict(l=0,r=0,t=10,b=0))
                st.plotly_chart(fig,use_container_width=True)

        col3,col4 = st.columns(2)
        with col3:
            st.subheader("Severity breakdown")
            sev = df["severity"].value_counts()
            fig = px.pie(values=sev.values,names=sev.index,hole=0.6,
                         color_discrete_sequence=["#ef476f","#ffd166","#06d6a0"],template="plotly_dark")
            fig.update_layout(height=280,margin=dict(l=0,r=0,t=10,b=0))
            st.plotly_chart(fig,use_container_width=True)
        with col4:
            st.subheader("Revenue by zone")
            zr = df.groupby("zone")["fine_amount"].sum().reset_index()
            zr.columns = ["zone","revenue"]
            fig = px.bar(zr.sort_values("revenue"),x="revenue",y="zone",orientation="h",
                         color="revenue",color_continuous_scale="Purples",template="plotly_dark")
            fig.update_layout(showlegend=False,height=280,margin=dict(l=0,r=0,t=10,b=0))
            st.plotly_chart(fig,use_container_width=True)

        st.subheader("Heatmap — hour vs day of week")
        if "hour" in df.columns and "day_of_week" in df.columns:
            days_order=["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
            heat = df.groupby(["day_of_week","hour"]).size().reset_index(name="count")
            pivot = heat.pivot(index="day_of_week",columns="hour",values="count").fillna(0)
            pivot = pivot.reindex([d for d in days_order if d in pivot.index])
            fig = px.imshow(pivot,aspect="auto",color_continuous_scale="Blues",
                            template="plotly_dark",labels={"x":"Hour","y":"Day","color":"Count"})
            fig.update_layout(height=260,margin=dict(l=0,r=0,t=10,b=0))
            st.plotly_chart(fig,use_container_width=True)

    # ── PAGE 2: Pipeline Monitor ──────────────────────────────────────────────
    elif page == "🏗️ Pipeline Monitor":
        st.title("Pipeline Architecture Monitor")
        stages=[
            ("Linux Setup",           "data/logs"),
            ("Python ETL",            "data/processed/violations.csv"),
            ("SQL Warehouse",         "data/warehouse/violations.db"),
            ("HDFS Upload",           "data/hdfs/namenode/fsimage.json"),
            ("Spark Processing",      "data/spark_output"),
            ("Kafka Streaming",       "data/hdfs"),
            ("Airflow Orchestration", "dags/traffic_pipeline_dag.py"),
            ("Cloud Upload",          "data/cloud_sim"),
            ("BigQuery Analytics",    "data/cloud_sim/s3"),
            ("Delta Lake",            "data/delta_lake"),
            ("Data Quality",          "data/reports/dq_report.md"),
            ("Dashboard",             "dashboard/app.py"),
        ]
        cols = st.columns(4)
        for i,(name,path) in enumerate(stages):
            ok    = Path(path).exists()
            color = "#10b981" if ok else "#f59e0b"
            icon  = "✅" if ok else "⏳"
            label = "DONE" if ok else "PENDING"
            with cols[i%4]:
                st.markdown(f"""<div style="background:#161c2e;border:1px solid #1e2840;
                    border-radius:8px;padding:.75rem;margin-bottom:.5rem;">
                    <div style="font-size:13px;color:#e2e8f0;font-weight:600;margin-bottom:.3rem">{name}</div>
                    <div style="font-size:11px;color:{color}">{icon} {label}</div>
                    </div>""",unsafe_allow_html=True)
        st.divider()
        st.subheader("Run pipeline")
        c1,c2,c3 = st.columns(3)
        with c1:
            if st.button("▶ Run main.py"):
                import subprocess
                with st.spinner("Running pipeline..."):
                    r = subprocess.run([sys.executable,"main.py","--skip-spark"],
                                       capture_output=True,text=True,timeout=180,
                                       cwd=str(Path(__file__).parent.parent))
                st.code((r.stdout+r.stderr)[-3000:]); st.cache_data.clear()
        with c2:
            if st.button("▶ Run Kafka simulation"):
                result = kafka_sim(300)
                st.success(f"Produced: {result['produced']} | Consumed: {result['consumed']}")
                st.json(result["partition_stats"])
        with c3:
            if st.button("▶ Generate sample data"):
                with st.spinner("Generating..."):
                    sample = generate_data(1000)
                    Path("data/processed").mkdir(parents=True,exist_ok=True)
                    sample.to_csv("data/processed/violations.csv",index=False)
                st.success("1,000 sample records saved"); st.cache_data.clear()
        st.subheader("Pipeline log")
        log_files=sorted(Path("data/logs").glob("*.log")) if Path("data/logs").exists() else []
        if log_files: st.code(log_files[-1].read_text()[-4000:],language="bash")
        else: st.info("No log files yet. Run the pipeline first.")

    # ── PAGE 3: Data Quality ──────────────────────────────────────────────────
    elif page == "✅ Data Quality":
        st.title("Data Quality Dashboard")
        st.caption("Validation checks and anomaly detection")
        with st.spinner("Running checks..."): dq=run_dq(df); anom=detect_anomalies(df)
        c1,c2,c3,c4=st.columns(4)
        c1.metric("DQ Score",     f"{dq['score']:.1f}%")
        c2.metric("Checks Passed",f"{dq['n_pass']}/{dq['n_pass']+dq['n_fail']}")
        c3.metric("Anomalies",    str(len(anom)))
        c4.metric("Total Records",f"{len(df):,}")
        fig=go.Figure(go.Indicator(mode="gauge+number",value=dq["score"],
            domain={"x":[0,1],"y":[0,1]},
            gauge={"axis":{"range":[0,100]},
                   "bar":{"color":"#10b981" if dq["score"]>=90 else "#f59e0b"},
                   "steps":[{"range":[0,60],"color":"#1a0a0a"},{"range":[60,90],"color":"#1a1200"},{"range":[90,100],"color":"#0a1a0a"}],
                   "threshold":{"line":{"color":"white","width":2},"value":90}},
            title={"text":"DQ Score (%)"}))
        fig.update_layout(template="plotly_dark",height=260)
        st.plotly_chart(fig,use_container_width=True)
        st.subheader("Check results")
        cdf=pd.DataFrame(dq["checks"])
        if not cdf.empty:
            st.dataframe(cdf.style.applymap(
                lambda v:"color:#10b981" if v=="PASS" else "color:#ff6b35",
                subset=["status"]),use_container_width=True)
        if len(anom):
            st.subheader(f"Anomalies — {len(anom)} records")
            cols=[c for c in ["violation_id","zone","violation_type","fine_amount","severity","anomaly_reason"] if c in anom.columns]
            st.dataframe(anom[cols].head(50),use_container_width=True)
        else: st.success("No anomalies detected.")
        with st.expander("Full DQ report"):
            p=Path("data/reports/dq_report.md")
            st.markdown(p.read_text() if p.exists() else "_Run pipeline to generate report._")

    # ── PAGE 4: Streaming Monitor ─────────────────────────────────────────────
    elif page == "⚡ Streaming Monitor":
        st.title("Kafka Streaming Monitor")
        st.caption("Real-time violation event pipeline")
        c1,c2,c3=st.columns(3)
        n_ev=c1.slider("Events",50,1000,200,50)
        c2.slider("Events/sec",10,200,50,10)
        if c3.button("▶ Run simulation"):
            with st.spinner(f"Simulating {n_ev} events..."): result=kafka_sim(n_ev)
            st.success(f"Produced: {result['produced']} | Consumed: {result['consumed']}")
            pf=pd.DataFrame([{"partition":f"P{k}","messages":v} for k,v in result["partition_stats"]["partitions"].items()])
            if not pf.empty:
                st.subheader("Partition distribution")
                fig=px.bar(pf,x="partition",y="messages",color="messages",
                            color_continuous_scale="Teal",template="plotly_dark")
                fig.update_layout(height=300,showlegend=False)
                st.plotly_chart(fig,use_container_width=True)
            st.subheader("Sample events")
            st.dataframe(pd.DataFrame(result["sample_records"]),use_container_width=True)
        st.divider()
        st.subheader("Structured Streaming — deployment code")
        st.code("""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType

spark = SparkSession.builder.appName("TrafficStream") \\
    .config("spark.sql.shuffle.partitions","8").getOrCreate()

schema = StructType([
    StructField("violation_type", StringType(), True),
    StructField("zone",           StringType(), True),
    StructField("speed_kmh",      FloatType(),  True),
    StructField("fine_amount",    FloatType(),  True),
])

stream = spark.readStream.format("kafka") \\
    .option("kafka.bootstrap.servers","localhost:9092") \\
    .option("subscribe","traffic-violations") \\
    .option("startingOffsets","latest").load()

violations = stream.select(
    F.from_json(F.col("value").cast("string"), schema).alias("d")
).select("d.*").withColumn("event_time", F.current_timestamp())

windowed = violations.withWatermark("event_time","2 minutes") \\
    .groupBy(F.window("event_time","5 minutes","1 minute"),"zone") \\
    .agg(F.count("*").alias("count"), F.avg("fine_amount").alias("avg_fine"))

windowed.writeStream.outputMode("update").format("console").start().awaitTermination()
        """,language="python")

    # ── PAGE 5: Cloud Status ──────────────────────────────────────────────────
    elif page == "☁️ Cloud Status":
        st.title("Cloud Infrastructure Status")
        st.caption("AWS, GCP, Azure — Delta Lake")
        st.subheader("Cloud provider comparison")
        for cat,providers in CLOUD_COMPARISON.items():
            with st.expander(f"📦 {cat}"):
                cdf=pd.DataFrame(providers).T.reset_index().rename(columns={"index":"Provider"})
                st.dataframe(cdf,use_container_width=True)
        st.divider()
        st.subheader("Simulated S3 uploads")
        sim_s3=Path("data/cloud_sim/s3")
        if sim_s3.exists():
            files=[str(p.relative_to(sim_s3)) for p in sim_s3.rglob("*") if p.is_file()]
            if files:
                st.dataframe(pd.DataFrame({"S3 Key":files,"URI":[f"s3://traffic-pipeline-bucket/{f}" for f in files]}),use_container_width=True)
            else: st.info("Run the pipeline to populate S3.")
        else: st.info("Run the pipeline to populate cloud storage.")
        st.divider()
        st.subheader("Delta Lake transaction history")
        dlog=Path("data/delta_lake/violations/_delta_log")
        if dlog.exists():
            logs=sorted(dlog.glob("*.json"))
            history=[]
            for lg in logs:
                try:
                    h=json.loads(lg.read_text())
                    history.append({"Version":h.get("version",""),"Operation":h.get("operation",""),
                                    "Timestamp":str(h.get("timestamp",""))[:19],
                                    "Rows":h.get("operationParameters",{}).get("numRows","—")})
                except: pass
            if history:
                st.dataframe(pd.DataFrame(history),use_container_width=True)
                if st.button("🔙 Time travel to version 0"):
                    v0=Path("data/delta_lake/violations/version_0/data.csv")
                    if v0.exists():
                        d=pd.read_csv(v0); st.success(f"Version 0: {len(d):,} rows")
                        st.dataframe(d.head(10),use_container_width=True)
                    else: st.warning("Version 0 not found.")
        else: st.info("Run the pipeline to initialise Delta Lake.")
        st.divider()
        st.subheader("EC2 deployment specification")
        st.json({"instance_type":"m5.xlarge","region":"us-east-1","vcpu":4,"ram_gb":16,
                  "iam_role":"pipeline-ec2-s3-role","os":"Ubuntu 22.04 LTS",
                  "status":"Simulated (set USE_REAL_AWS=true to deploy)"})

    # ── PAGE 6: Anomaly Detection ─────────────────────────────────────────────
    elif page == "🔬 Anomaly Detection":
        st.title("Anomaly Detection")
        st.caption("Z-score and IQR statistical outlier detection")
        c1,c2=st.columns(2)
        target=c1.selectbox("Target column",["fine_amount","speed_kmh"])
        method=c2.selectbox("Method",["combined","zscore","iqr"])
        z_thr=st.slider("Z-score threshold",1.5,5.0,3.0,0.5)
        with st.spinner("Detecting..."): anom=detect_anomalies(df,target,z_thr,method); spikes=detect_spikes(df)
        st.metric("Anomalies found",f"{len(anom)}",f"{len(anom)/max(len(df),1)*100:.2f}% of dataset")
        pf=df[[target]].copy().dropna()
        pf["is_anomaly"]=pf.index.isin(anom.index); pf["idx"]=range(len(pf))
        fig=px.scatter(pf,x="idx",y=target,color="is_anomaly",
                        color_discrete_map={False:"#00b4d8",True:"#ff6b35"},
                        template="plotly_dark",title=f"Anomaly scatter — {target}")
        fig.update_traces(marker_size=2); fig.update_layout(height=350)
        st.plotly_chart(fig,use_container_width=True)
        if len(spikes):
            st.subheader(f"Hourly spikes — {len(spikes)} hours flagged")
            st.dataframe(spikes,use_container_width=True)
        if len(anom):
            st.subheader("Anomalous records")
            cols=[c for c in ["violation_id","zone","violation_type",target,"severity","anomaly_reason"] if c in anom.columns]
            st.dataframe(anom[cols].head(100),use_container_width=True)
        else: st.success(f"No anomalies in {target} using {method} method.")

    # ── PAGE 7: Raw Data ──────────────────────────────────────────────────────
    elif page == "📋 Raw Data":
        st.title("Violations Explorer")
        st.caption("Filter and download violation records")
        c1,c2,c3=st.columns(3)
        zones=["All"]+sorted(df["zone"].dropna().unique().tolist())
        types=["All"]+sorted(df["violation_type"].dropna().unique().tolist())
        sel_zone=c1.selectbox("Zone",zones)
        sel_type=c2.selectbox("Violation type",types)
        sel_sev =c3.selectbox("Severity",["All","High","Medium","Low"])
        filt=df.copy()
        if sel_zone!="All": filt=filt[filt["zone"]==sel_zone]
        if sel_type!="All": filt=filt[filt["violation_type"]==sel_type]
        if sel_sev !="All": filt=filt[filt["severity"]==sel_sev]
        st.caption(f"Showing {len(filt):,} of {len(df):,} records")
        disp=[c for c in ["violation_id","violation_type","zone","speed_kmh","fine_amount","severity","hour","vehicle_type","officer_id"] if c in filt.columns]
        st.dataframe(filt[disp].head(500),use_container_width=True)
        st.download_button("⬇ Download filtered CSV",filt.to_csv(index=False).encode(),
                           f"violations_{sel_zone}_{sel_type}.csv","text/csv")

if __name__ == "__main__":
    main()
