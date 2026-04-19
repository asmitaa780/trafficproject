
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
│   ├── ingestion.py                 ← Steps 1-3   Linux, HTTP, CSV batch ingest
│   ├── cleaning.py                  ← Steps 4, 6  ETL cleaning, memory optimisation
│   ├── transform.py                 ← Step 5      Reusable transform modules
│   ├── loader.py                    ← Steps 8-10  SQL, star schema, Parquet
│   ├── hadoop_hdfs.py               ← Steps 11-12 HDFS client + Namenode sim
│   ├── spark_jobs.py                ← Steps 13-16 PySpark jobs, SQL, optimisation
│   ├── kafka_streaming.py           ← Steps 17-20 Kafka producer/consumer/streaming
│   ├── quality.py                   ← Step 28     DQ checks + anomaly detection
│   ├── cloud.py                     ← Steps 23-27 S3, GCS, EC2, BigQuery, Delta Lake
│   └── utils.py                     ← Steps 1-2   Logging, directory setup
│
├── dags/
│   └── traffic_pipeline_dag.py      ← Steps 21-22 Airflow DAG (copy to $AIRFLOW_HOME/dags/)
│
└── dashboard/
    └── app.py                       ← Steps 29-30 Streamlit dashboard (7 pages)
```

---

## Step Coverage

| Steps | File | What it does |
|-------|------|-------------|
| 1 | utils.py | Linux dir setup, chmod, rotating log files |
| 2 | ingestion.py | HTTP API ingestion via requests, curl equivalent |
| 3 | ingestion.py | Read N CSV files in chunks, merge, handle encoding errors |
| 4 | cleaning.py | Missing value imputation (group-median), type coercion |
| 5 | transform.py | `normalize()`, `aggregate_by_zone()`, `validate_schema()`, `compute_risk_score()` |
| 6 | cleaning.py | 100k–1M+ rows, chunked reads, float64→float32 downcast, Category dtype |
| 7 | loader.py | OLTP (SQLite) vs OLAP (Parquet/BigQuery) schema design |
| 8 | loader.py | Star schema: `fact_violation` + `dim_time/location/vehicle` |
| 9 | loader.py | ETL: clean → transform → load (SQLite + Parquet) |
| 10 | ingestion.py | Batch CSV ingestion with provenance metadata |
| 11 | hadoop_hdfs.py | HDFS client: `mkdir`, `put`, `get`, `ls`, structured + unstructured data |
| 12 | hadoop_hdfs.py | `NamenodeSimulator`: fsimage, block placement, DataNode assignment |
| 13 | spark_jobs.py | `SparkSession`, explicit schema, CSV/Parquet read |
| 14 | spark_jobs.py | `withColumn`, `dropna`, `when/otherwise`, speed bucketing |
| 15 | spark_jobs.py | `createOrReplaceTempView`, zone SQL, hourly trend, subquery, CTE |
| 16 | spark_jobs.py | `repartition(16, "zone","hour")`, `persist(MEMORY_AND_DISK)`, AQE |
| 17 | kafka_streaming.py | `generate_violation_event()`, continuous stream simulation |
| 18 | kafka_streaming.py | `ViolationProducer`, `ViolationConsumer`, batch + stream modes |
| 19 | kafka_streaming.py | `SimulatedKafkaBroker`: partitioned queues, offset tracking, lag reporting |
| 20 | kafka_streaming.py | `STRUCTURED_STREAMING_CODE`: watermark, sliding window, Delta sink |
| 21 | dags/traffic_pipeline_dag.py | Airflow DAG, `PythonOperator`, `FileSensor`, `BranchPythonOperator` |
| 22 | dags/traffic_pipeline_dag.py | `schedule_interval`, retries, exponential backoff, SLA, XCom |
| 23 | cloud.py | `CLOUD_COMPARISON` dict: AWS/GCP/Azure services side-by-side |
| 24 | cloud.py | `S3Client.upload/download`, `GCSClient.upload`, presigned URLs |
| 25 | cloud.py | `EC2_DEPLOYMENT_SPEC` + `user_data` bootstrap script |
| 26 | cloud.py | `BigQuerySimulator.query()` with SQL adapter for SQLite |
| 27 | cloud.py | `DeltaLakeSimulator`: `write`, `read`, `history`, `vacuum`, `z_order_by` |
| 28 | quality.py | 5-dimension DQ suite + Z-score/IQR anomaly detection + markdown report |
| 29 | main.py | Full orchestration: 9 stages, CLI flags, timing, summary |
| 30 | dashboard/app.py | Streamlit: 7 pages, Plotly charts, live KPIs, pipeline monitor |

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

