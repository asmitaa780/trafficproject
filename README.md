
# Traffic Violations — Complete Data Engineering Pipeline
### All 30 Steps · End-to-End · Executable

```
python main.py --skip-spark       # run everything (Spark needs Java 11+)
streamlit run dashboard/app.py    # launch interactive dashboard
```

---

## Project Structure

```
traffic_pipeline/
├── main.py                          ← Master orchestrator (all 30 steps)
├── requirements.txt
│
├── pipeline/
│   ├── ingestion.py                 
│   ├── cleaning.py                 
│   ├── transform.py                 
│   ├── loader.py                    
│   ├── hadoop_hdfs.py               
│   ├── spark_jobs.py                
│   ├── kafka_streaming.py           
│   ├── quality.py                  
│   ├── cloud.py                 
│   └── utils.py                    
│
├── dags/
│   └── traffic_pipeline_dag.py      
│
└── dashboard/
    └── app.py                       
```



---

## Quick Start

```bash
# 1. Install core dependencies
pip install pandas numpy scipy requests sqlalchemy streamlit plotly kafka-python boto3

# 2. Run the pipeline (generates 100k synthetic records automatically)
python main.py --skip-spark          # skip Spark if Java not installed

# 3. Launch the dashboard
streamlit run dashboard/app.py
# → Open http://localhost:8501

# 4. (Optional) Run with real Kafka
USE_REAL_KAFKA=true python main.py

# 5. (Optional) Run with real AWS S3
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export S3_BUCKET=my-bucket
USE_REAL_AWS=true python main.py

# 6. (Optional) PySpark jobs
pip install pyspark                  # also needs Java 11+
python -m pipeline.spark_jobs

# 7. (Optional) Deploy Airflow DAG
pip install apache-airflow==2.9.0
export AIRFLOW_HOME=~/airflow
airflow db init
cp dags/traffic_pipeline_dag.py $AIRFLOW_HOME/dags/
airflow dags trigger traffic_violations_pipeline
```

---

## Dashboard Pages

| Page | Content |
|------|---------|
| 📊 Dashboard | KPI cards, bar/line/donut/heatmap charts |
| 🏗️ Pipeline Monitor | Stage status, run buttons, live log viewer |
| ✅ Data Quality | DQ score gauge, check table, anomaly scatter |
| ⚡ Streaming Monitor | Kafka sim controls, partition distribution chart |
| ☁️ Cloud Status | Provider comparison, S3 file list, Delta history + time travel |
| 🔬 Anomaly Detection | Z-score/IQR scatter, temporal spike table |
| 📋 Raw Data | Filterable violations table, CSV download |

---

## Environment Variables

| Variable | Default | Effect |
|----------|---------|--------|
| `USE_REAL_KAFKA` | `false` | Connect to real Kafka at `KAFKA_BOOTSTRAP_SERVERS` |
| `USE_REAL_AWS` | `false` | Use real S3 via boto3 |
| `USE_REAL_GCP` | `false` | Use real GCS via google-cloud-storage |
| `USE_REAL_HDFS` | `false` | Connect to real HDFS via hdfs3 |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `S3_BUCKET` | `traffic-pipeline-bucket` | S3 bucket name |
| `GCS_BUCKET` | `traffic-pipeline-gcs` | GCS bucket name |
| `HDFS_HOST` | `localhost` | HDFS Namenode host |
| `SPARK_MASTER` | `local[*]` | Spark master URL |
| `SPARK_SHUFFLE_PARTITIONS` | `16` | Spark shuffle partition count |
=======
# trafficviolation
Data Engineering Project Batch_2027, Name-Asmita Paul ,Roll number-23052957,Title -Traffic Violation Database

