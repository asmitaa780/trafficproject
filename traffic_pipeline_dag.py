"""
dags/traffic_pipeline_dag.py
─────────────────────────────
Step 21 — Create DAG for ETL pipeline
Step 22 — Add scheduling, monitoring, retry mechanisms

Deploy:  copy this file to $AIRFLOW_HOME/dags/
Install: pip install apache-airflow apache-airflow-providers-slack

Local Airflow quick-start:
  pip install apache-airflow==2.8.1
  airflow db init
  airflow users create --username admin --password admin --role Admin
                       --firstname A --lastname B --email a@b.com
  airflow webserver -p 8080 &
  airflow scheduler &
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

# ── Guard: only import Airflow when it's installed ───────────────────────────
try:
    from airflow import DAG
    from airflow.operators.python   import PythonOperator, BranchPythonOperator
    from airflow.operators.bash     import BashOperator
    from airflow.operators.empty    import EmptyOperator
    from airflow.sensors.filesystem import FileSensor
    from airflow.utils.trigger_rule import TriggerRule
    from airflow.models import Variable
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    # Stubs so the file can be imported for documentation purposes
    class DAG:
        def __init__(self, *a, **kw): pass
        def __enter__(self): return self
        def __exit__(self, *a): pass

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# DEFAULT ARGUMENTS — Step 22: retry + alerting config
# ─────────────────────────────────────────────────────────────────────────────

default_args = {
    "owner":              "data-engineering",
    "depends_on_past":    False,
    "start_date":         datetime(2025, 1, 1),
    "email":              ["alerts@trafficpipeline.io"],
    "email_on_failure":   True,
    "email_on_retry":     False,

    # Step 22 — Retry mechanism
    "retries":            3,
    "retry_delay":        timedelta(minutes=5),
    "retry_exponential_backoff": True,    # 5m, 10m, 20m delays
    "max_retry_delay":    timedelta(hours=1),

    "execution_timeout":  timedelta(hours=2),
}

# ─────────────────────────────────────────────────────────────────────────────
# TASK FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def task_ingest(**context):
    """Stage 1 — Ingest raw CSV files."""
    from pathlib import Path
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from pipeline.ingestion import ingest_csv_files

    df = ingest_csv_files(Path("data/raw"))
    # Push record count to XCom for downstream tasks
    context["ti"].xcom_push(key="raw_count", value=len(df))
    # Save checkpoint
    df.to_csv("data/processed/.ingest_checkpoint.csv", index=False)
    logger.info(f"Ingest complete: {len(df):,} records")
    return len(df)


def task_clean(**context):
    """Stage 2 — Clean and validate data."""
    import pandas as pd
    from pipeline.cleaning import clean_dataframe

    df = pd.read_csv("data/processed/.ingest_checkpoint.csv")
    clean_df = clean_dataframe(df)
    clean_df.to_csv("data/processed/.clean_checkpoint.csv", index=False)
    context["ti"].xcom_push(key="clean_count", value=len(clean_df))
    logger.info(f"Clean complete: {len(clean_df):,} records")
    return len(clean_df)


def task_transform(**context):
    """Stage 3 — Apply transformations."""
    import pandas as pd
    from pipeline.transform import normalize, aggregate_by_zone, compute_risk_score

    df = pd.read_csv("data/processed/.clean_checkpoint.csv")
    df = normalize(df, "speed_kmh",   alias="speed_norm")
    df = normalize(df, "fine_amount", alias="fine_norm")
    df = compute_risk_score(df)
    df.to_csv("data/processed/violations.csv", index=False)

    zone_agg = aggregate_by_zone(df)
    zone_agg.to_csv("data/processed/zone_aggregates.csv", index=False)
    logger.info("Transform complete")
    return True


def task_data_quality(**context):
    """Stage 4 — Run DQ checks and branch on pass/fail."""
    import pandas as pd
    from pipeline.quality import run_dq_checks, detect_anomalies, generate_dq_report

    df        = pd.read_csv("data/processed/violations.csv")
    dq_result = run_dq_checks(df)
    anomalies = detect_anomalies(df, col="fine_amount")
    report    = generate_dq_report(dq_result, anomalies,
                                   out_path="data/reports/dq_report.md")

    context["ti"].xcom_push(key="dq_score",   value=dq_result["score"])
    context["ti"].xcom_push(key="dq_passed",  value=dq_result["passed"])
    context["ti"].xcom_push(key="n_anomalies",value=len(anomalies))

    if not dq_result["passed"]:
        raise ValueError(f"DQ checks failed — score {dq_result['score']}%")
    logger.info(f"DQ passed: {dq_result['score']}%")
    return dq_result["score"]


def task_branch_dq(**context):
    """Step 22 — Branching: route to alert task if DQ fails."""
    score = context["ti"].xcom_pull(key="dq_score", task_ids="data_quality")
    if score and score >= 90.0:
        return "load_warehouse"
    return "dq_alert"


def task_load_warehouse(**context):
    """Stage 5 — Load to SQLite + star schema."""
    import pandas as pd
    from pipeline.loader import load_to_sqlite, load_star_schema

    df = pd.read_csv("data/processed/violations.csv")
    load_to_sqlite(df, db_path="data/warehouse/violations.db")
    counts = load_star_schema(df, db_path="data/warehouse/violations_star.db")
    context["ti"].xcom_push(key="warehouse_counts", value=json.dumps(counts))
    logger.info(f"Warehouse loaded: {counts}")
    return counts


def task_hdfs_upload(**context):
    """Stage 6 — Upload outputs to HDFS."""
    from pipeline.hadoop_hdfs import HDFSClient, setup_hdfs_pipeline, upload_pipeline_outputs
    client  = HDFSClient()
    setup_hdfs_pipeline(client)
    uploads = upload_pipeline_outputs(client)
    logger.info(f"HDFS: {len(uploads)} files uploaded")
    return list(uploads.keys())


def task_kafka_stream(**context):
    """Stage 7 — Simulate Kafka streaming for Step 17-19."""
    from pipeline.kafka_streaming import run_kafka_simulation
    result = run_kafka_simulation(n_events=500)
    logger.info(f"Kafka: {result['produced']} produced, {result['consumed']} consumed")
    return result


def task_cloud_upload(**context):
    """Stage 8 — Upload to cloud storage (S3/GCS)."""
    from pipeline.cloud import upload_to_cloud
    result = upload_to_cloud()
    logger.info(f"Cloud upload: {result}")
    return result


def task_dq_alert(**context):
    """Step 22 — Send alert when DQ fails."""
    score = context["ti"].xcom_pull(key="dq_score", task_ids="data_quality")
    msg = f"⚠️ Traffic Pipeline DQ ALERT: score={score}% — below 90% threshold"
    logger.error(msg)
    # In production: send Slack/PagerDuty/email alert
    Path("data/logs/dq_alerts.log").open("a").write(
        f"{datetime.now().isoformat()} {msg}\n"
    )


def task_notify_success(**context):
    """Step 22 — Success notification."""
    ti      = context["ti"]
    clean   = ti.xcom_pull(key="clean_count",  task_ids="clean")
    dq      = ti.xcom_pull(key="dq_score",     task_ids="data_quality")
    kafka   = ti.xcom_pull(task_ids="kafka_stream")
    msg = (f"✅ Traffic Pipeline SUCCESS | "
           f"Records: {clean:,} | DQ: {dq}% | "
           f"Kafka: {kafka.get('consumed',0) if kafka else 'N/A'} events")
    logger.info(msg)
    Path("data/logs/pipeline_runs.log").open("a").write(
        f"{datetime.now().isoformat()} {msg}\n"
    )


# ─────────────────────────────────────────────────────────────────────────────
# DAG DEFINITION — Step 21 + Step 22
# ─────────────────────────────────────────────────────────────────────────────

if AIRFLOW_AVAILABLE:
    with DAG(
        dag_id="traffic_violations_pipeline",
        description="End-to-end traffic violations data engineering pipeline",
        default_args=default_args,

        # Step 22 — Scheduling
        schedule_interval="0 * * * *",   # hourly at :00
        catchup=False,                    # don't backfill missed runs
        max_active_runs=1,                # prevent concurrent runs
        tags=["data-engineering", "traffic", "etl"],

        # Step 22 — SLA monitoring
        sla_miss_callback=lambda dag, task_list, blocking_task_list, slas, blocking_tis:
            logger.warning(f"SLA missed for tasks: {task_list}"),

        doc_md="""
        ## Traffic Violations Pipeline
        Covers Steps 1–30 of the data engineering curriculum.
        **Schedule**: hourly  |  **Owner**: data-engineering  |  **Retries**: 3
        """,
    ) as dag:

        # ── Sensor: wait for raw data file ──────────────────────────────────
        wait_for_data = FileSensor(
            task_id="wait_for_raw_data",
            filepath="data/raw",
            poke_interval=60,
            timeout=3600,
            mode="reschedule",    # release worker slot while waiting
        )

        # ── Main ETL stages ─────────────────────────────────────────────────
        ingest = PythonOperator(
            task_id="ingest",
            python_callable=task_ingest,
            sla=timedelta(minutes=10),
        )
        clean = PythonOperator(
            task_id="clean",
            python_callable=task_clean,
            sla=timedelta(minutes=15),
        )
        transform = PythonOperator(
            task_id="transform",
            python_callable=task_transform,
            sla=timedelta(minutes=20),
        )
        data_quality = PythonOperator(
            task_id="data_quality",
            python_callable=task_data_quality,
            sla=timedelta(minutes=25),
        )

        # ── Step 22: Branch on DQ score ─────────────────────────────────────
        branch_dq = BranchPythonOperator(
            task_id="branch_dq",
            python_callable=task_branch_dq,
        )
        dq_alert = PythonOperator(
            task_id="dq_alert",
            python_callable=task_dq_alert,
        )

        # ── Storage + streaming stages ───────────────────────────────────────
        load_warehouse = PythonOperator(
            task_id="load_warehouse",
            python_callable=task_load_warehouse,
            sla=timedelta(minutes=35),
        )
        hdfs_upload = PythonOperator(
            task_id="hdfs_upload",
            python_callable=task_hdfs_upload,
            sla=timedelta(minutes=40),
        )
        kafka_stream = PythonOperator(
            task_id="kafka_stream",
            python_callable=task_kafka_stream,
            sla=timedelta(minutes=45),
        )
        cloud_upload = PythonOperator(
            task_id="cloud_upload",
            python_callable=task_cloud_upload,
            sla=timedelta(minutes=50),
        )

        # ── Bash: run Spark job ──────────────────────────────────────────────
        spark_job = BashOperator(
            task_id="spark_job",
            bash_command=(
                "python -m pipeline.spark_jobs "
                "|| echo 'PySpark not installed — skipping Spark job'"
            ),
            sla=timedelta(minutes=60),
        )

        # ── Notifications ────────────────────────────────────────────────────
        notify_success = PythonOperator(
            task_id="notify_success",
            python_callable=task_notify_success,
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )
        end = EmptyOperator(
            task_id="end",
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )

        # ── DAG wiring ───────────────────────────────────────────────────────
        (
            wait_for_data
            >> ingest
            >> clean
            >> transform
            >> data_quality
            >> branch_dq
        )
        branch_dq >> dq_alert      >> end
        branch_dq >> load_warehouse >> [hdfs_upload, kafka_stream, cloud_upload, spark_job]
        [hdfs_upload, kafka_stream, cloud_upload, spark_job] >> notify_success >> end
