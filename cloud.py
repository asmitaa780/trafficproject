"""
pipeline/cloud.py
──────────────────
Step 23 — Compare AWS, GCP, Azure services
Step 24 — Upload and retrieve files from S3/GCS
Step 25 — Launch EC2 instance and deploy data pipeline
Step 26 — Use BigQuery/Redshift to analyze large data
Step 27 — Implement Delta Lake/Iceberg and manage versions

All cloud operations have a LOCAL SIMULATION mode (default).
Set env vars to connect to real cloud:
  USE_REAL_AWS=true  + AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY + S3_BUCKET
  USE_REAL_GCP=true  + GOOGLE_APPLICATION_CREDENTIALS + GCS_BUCKET
"""

import os
import json
import shutil
import hashlib
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────────────────────
USE_REAL_AWS   = os.getenv("USE_REAL_AWS",   "false").lower() == "true"
USE_REAL_GCP   = os.getenv("USE_REAL_GCP",   "false").lower() == "true"
S3_BUCKET      = os.getenv("S3_BUCKET",      "traffic-pipeline-bucket")
GCS_BUCKET     = os.getenv("GCS_BUCKET",     "traffic-pipeline-gcs")
AWS_REGION     = os.getenv("AWS_REGION",     "us-east-1")
CLOUD_SIM_ROOT = Path("data/cloud_sim")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 23 — Cloud Service Comparison
# ─────────────────────────────────────────────────────────────────────────────

CLOUD_COMPARISON = {
    "object_storage": {
        "AWS":   {"service": "S3",           "cost_gb_month": 0.023, "egress": "paid"},
        "GCP":   {"service": "Cloud Storage","cost_gb_month": 0.020, "egress": "paid"},
        "Azure": {"service": "Blob Storage", "cost_gb_month": 0.018, "egress": "paid"},
    },
    "data_warehouse": {
        "AWS":   {"service": "Redshift",  "model": "cluster",   "sql": True,  "serverless": True},
        "GCP":   {"service": "BigQuery",  "model": "serverless","sql": True,  "serverless": True},
        "Azure": {"service": "Synapse",   "model": "hybrid",    "sql": True,  "serverless": True},
    },
    "compute": {
        "AWS":   {"service": "EC2",             "spot": True, "managed_spark": "EMR"},
        "GCP":   {"service": "Compute Engine",  "spot": True, "managed_spark": "Dataproc"},
        "Azure": {"service": "Virtual Machines","spot": True, "managed_spark": "HDInsight"},
    },
    "stream_processing": {
        "AWS":   {"service": "Kinesis",       "managed_kafka": "MSK"},
        "GCP":   {"service": "Pub/Sub",       "managed_kafka": "Confluent (marketplace)"},
        "Azure": {"service": "Event Hubs",    "managed_kafka": "Kafka protocol compatible"},
    },
    "orchestration": {
        "AWS":   {"service": "MWAA (Airflow)","step_functions": True},
        "GCP":   {"service": "Cloud Composer","workflows": True},
        "Azure": {"service": "ADF + ADF",     "logic_apps": True},
    },
    "lakehouse": {
        "AWS":   {"service": "Lake Formation + S3 + Glue",       "format": "Iceberg/Hudi"},
        "GCP":   {"service": "BigLake + GCS + Dataflow",         "format": "Iceberg"},
        "Azure": {"service": "Azure Synapse + ADLS + Databricks", "format": "Delta Lake"},
    },
}

def print_cloud_comparison():
    logger.info("=" * 70)
    logger.info("CLOUD PROVIDER COMPARISON (Step 23)")
    logger.info("=" * 70)
    for category, providers in CLOUD_COMPARISON.items():
        logger.info(f"\n  {category.upper()}")
        for cloud, details in providers.items():
            logger.info(f"    {cloud:6s}: {json.dumps(details)}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 24 — Cloud Storage (S3 / GCS)
# ─────────────────────────────────────────────────────────────────────────────

class S3Client:
    """
    Step 24 — Upload and retrieve files from S3.
    Simulation: mirrors to data/cloud_sim/s3/<bucket>/
    Real:       uses boto3 (pip install boto3)
    """

    def __init__(self, bucket: str = S3_BUCKET):
        self.bucket  = bucket
        self.sim_root = CLOUD_SIM_ROOT / "s3" / bucket
        self.sim_root.mkdir(parents=True, exist_ok=True)

        if USE_REAL_AWS:
            try:
                import boto3
                self._s3 = boto3.client("s3", region_name=AWS_REGION)
                # Create bucket if not exists
                try:
                    self._s3.head_bucket(Bucket=bucket)
                except:
                    self._s3.create_bucket(Bucket=bucket,
                        CreateBucketConfiguration={"LocationConstraint": AWS_REGION})
                logger.info(f"S3 connected: s3://{bucket}")
            except ImportError:
                logger.warning("boto3 not installed — using S3 simulation")
                self._s3 = None
        else:
            self._s3 = None

    def upload(self, local_path: str, s3_key: str,
               metadata: dict = None) -> dict:
        """Upload file to S3 with optional metadata tags."""
        local_path = Path(local_path)
        if not local_path.exists():
            raise FileNotFoundError(f"Not found: {local_path}")

        etag = hashlib.md5(local_path.read_bytes()).hexdigest()

        if self._s3:
            extra = {"Metadata": {k: str(v) for k, v in (metadata or {}).items()}}
            self._s3.upload_file(str(local_path), self.bucket, s3_key,
                                 ExtraArgs=extra)
        else:
            dest = self.sim_root / s3_key
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(local_path, dest)

        result = {
            "bucket":    self.bucket,
            "key":       s3_key,
            "size_bytes": local_path.stat().st_size,
            "etag":      etag,
            "uri":       f"s3://{self.bucket}/{s3_key}",
            "uploaded_at": datetime.now().isoformat(),
        }
        logger.info(f"  S3 upload: {local_path.name} → s3://{self.bucket}/{s3_key}")
        return result

    def download(self, s3_key: str, local_dest: str) -> Path:
        """Download file from S3."""
        local_dest = Path(local_dest)
        local_dest.parent.mkdir(parents=True, exist_ok=True)

        if self._s3:
            self._s3.download_file(self.bucket, s3_key, str(local_dest))
        else:
            src = self.sim_root / s3_key
            if not src.exists():
                raise FileNotFoundError(f"S3 key not found: {s3_key}")
            shutil.copy2(src, local_dest)

        logger.info(f"  S3 download: s3://{self.bucket}/{s3_key} → {local_dest}")
        return local_dest

    def list_objects(self, prefix: str = "") -> list:
        """List S3 objects with given prefix."""
        if self._s3:
            resp = self._s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            return [obj["Key"] for obj in resp.get("Contents", [])]
        else:
            base = self.sim_root / prefix
            if base.is_dir():
                return [str(p.relative_to(self.sim_root)) for p in base.rglob("*") if p.is_file()]
            return []

    def generate_presigned_url(self, s3_key: str, expiry_sec: int = 3600) -> str:
        """Generate a time-limited download URL."""
        if self._s3:
            return self._s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket, "Key": s3_key},
                ExpiresIn=expiry_sec,
            )
        return f"http://localhost:8080/sim_download/{self.bucket}/{s3_key}?expires={expiry_sec}"


class GCSClient:
    """Step 24 — Google Cloud Storage client (simulation + real)."""

    def __init__(self, bucket: str = GCS_BUCKET):
        self.bucket   = bucket
        self.sim_root = CLOUD_SIM_ROOT / "gcs" / bucket
        self.sim_root.mkdir(parents=True, exist_ok=True)

        if USE_REAL_GCP:
            try:
                from google.cloud import storage
                self._gcs = storage.Client()
                self._bucket_obj = self._gcs.bucket(bucket)
                logger.info(f"GCS connected: gs://{bucket}")
            except ImportError:
                logger.warning("google-cloud-storage not installed — using GCS simulation")
                self._gcs = None
        else:
            self._gcs = None

    def upload(self, local_path: str, blob_name: str) -> dict:
        local_path = Path(local_path)
        if self._gcs:
            blob = self._bucket_obj.blob(blob_name)
            blob.upload_from_filename(str(local_path))
        else:
            dest = self.sim_root / blob_name
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(local_path, dest)
        uri = f"gs://{self.bucket}/{blob_name}"
        logger.info(f"  GCS upload: {local_path.name} → {uri}")
        return {"uri": uri, "size_bytes": local_path.stat().st_size}


# ─────────────────────────────────────────────────────────────────────────────
# STEP 25 — EC2 / Compute (deployment spec)
# ─────────────────────────────────────────────────────────────────────────────

EC2_DEPLOYMENT_SPEC = {
    "instance_type": "m5.xlarge",       # 4 vCPU, 16 GB RAM — suitable for Spark
    "ami":           "ami-0c55b159cbfafe1f0",  # Ubuntu 22.04 LTS
    "region":        "us-east-1",
    "security_groups": ["sg-pipeline-inbound"],
    "key_pair":      "pipeline-key",
    "iam_role":      "pipeline-ec2-s3-role",   # grants S3 read/write
    "user_data":     """#!/bin/bash
set -e
apt-get update -y
apt-get install -y python3-pip openjdk-11-jdk git

# Install pipeline
git clone https://github.com/yourorg/traffic-pipeline /opt/pipeline
cd /opt/pipeline
pip3 install -r requirements.txt

# Set up cron for hourly runs (mirrors Airflow schedule)
echo "0 * * * * cd /opt/pipeline && python main.py >> /var/log/pipeline.log 2>&1" | crontab -
systemctl enable cron
""",
}

def simulate_ec2_deployment() -> dict:
    """Step 25 — Simulate EC2 instance launch and pipeline deployment."""
    logger.info("  EC2 Deployment Simulation (Step 25)")
    logger.info(f"  Instance type : {EC2_DEPLOYMENT_SPEC['instance_type']}")
    logger.info(f"  Region        : {EC2_DEPLOYMENT_SPEC['region']}")
    logger.info(f"  IAM Role      : {EC2_DEPLOYMENT_SPEC['iam_role']}")
    logger.info("  Status        : RUNNING (simulated)")
    return {
        "instance_id":  "i-0abc1234def56789" ,
        "public_ip":    "54.123.45.67",
        "private_ip":   "10.0.1.42",
        "state":        "running",
        "spec":         EC2_DEPLOYMENT_SPEC,
    }


# ─────────────────────────────────────────────────────────────────────────────
# STEP 26 — BigQuery / Redshift
# ─────────────────────────────────────────────────────────────────────────────

BIGQUERY_QUERIES = {
    "zone_revenue": """
        SELECT
            zone,
            SUM(fine_amount)         AS total_revenue,
            COUNT(*)                 AS violations,
            AVG(fine_amount)         AS avg_fine,
            COUNTIF(severity='High') AS high_severity
        FROM `traffic_dataset.fact_violation`
        WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        GROUP BY zone
        ORDER BY total_revenue DESC
    """,
    "hourly_heatmap": """
        SELECT
            EXTRACT(HOUR FROM timestamp) AS hour,
            zone,
            COUNT(*) AS count
        FROM `traffic_dataset.fact_violation`
        GROUP BY hour, zone
        ORDER BY hour, count DESC
    """,
    "officer_performance": """
        SELECT
            officer_id,
            COUNT(*)           AS total_violations,
            SUM(fine_amount)   AS revenue_generated,
            AVG(fine_amount)   AS avg_fine
        FROM `traffic_dataset.fact_violation`
        GROUP BY officer_id
        HAVING total_violations > 10
        ORDER BY revenue_generated DESC
        LIMIT 20
    """,
}

class BigQuerySimulator:
    """Step 26 — Simulates BigQuery queries against local SQLite."""

    def __init__(self, db_path: str = "data/warehouse/violations.db"):
        self.db_path = db_path

    def query(self, sql: str) -> list:
        """Run SQL against local SQLite (BigQuery substitute)."""
        import sqlite3, re
        # Adapt BigQuery SQL to SQLite
        sql = re.sub(r'`[^`]+\.([^`]+)`', r'\1', sql)   # strip project.dataset. prefix
        sql = re.sub(r'COUNTIF\(([^)]+)\)',
                     r"SUM(CASE WHEN \1 THEN 1 ELSE 0 END)", sql)
        sql = re.sub(r'DATE_SUB\(CURRENT_DATE\(\), INTERVAL \d+ DAY\)',
                     "date('now', '-30 days')", sql)
        sql = re.sub(r'DATE\(timestamp\)', "date(timestamp)", sql)
        sql = re.sub(r'EXTRACT\(HOUR FROM timestamp\)', "CAST(hour AS INTEGER)", sql)

        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.execute(sql)
                rows = [dict(r) for r in cur.fetchall()]
                logger.info(f"  BigQuery(sim): {len(rows)} rows returned")
                return rows
        except Exception as e:
            logger.warning(f"  BigQuery(sim) query error: {e}")
            return []

    def run_all_queries(self) -> dict:
        results = {}
        for name, sql in BIGQUERY_QUERIES.items():
            logger.info(f"  Running BigQuery query: {name}")
            results[name] = self.query(sql)
        return results


# ─────────────────────────────────────────────────────────────────────────────
# STEP 27 — Delta Lake / Lakehouse
# ─────────────────────────────────────────────────────────────────────────────

class DeltaLakeSimulator:
    """
    Step 27 — Simulate Delta Lake ACID transactions and versioning.
    Real Delta Lake: pip install delta-spark + configure SparkSession.
    This simulator tracks versions using JSON transaction logs.
    """

    def __init__(self, base_path: str = "data/delta_lake"):
        self.base_path = Path(base_path)
        self.log_path  = self.base_path / "_delta_log"
        self.log_path.mkdir(parents=True, exist_ok=True)
        self._version  = self._get_current_version()

    def _get_current_version(self) -> int:
        logs = sorted(self.log_path.glob("*.json"))
        return len(logs)

    def _write_commit(self, operation: str, stats: dict):
        """Write a Delta transaction log entry."""
        commit = {
            "version":   self._version,
            "timestamp": datetime.now().isoformat(),
            "operation": operation,
            "operationParameters": stats,
            "readVersion": self._version - 1,
        }
        log_file = self.log_path / f"{self._version:020d}.json"
        with open(log_file, "w") as f:
            json.dump(commit, f, indent=2)
        self._version += 1
        return commit

    def write(self, df, mode: str = "append") -> dict:
        """
        Step 27 — ACID write to Delta table.
        Modes: append | overwrite | merge
        """
        import pandas as pd

        version_dir = self.base_path / f"version_{self._version}"
        version_dir.mkdir(exist_ok=True)

        out_path = version_dir / "data.csv"
        df.to_csv(out_path, index=False)

        stats = {
            "mode":       mode,
            "numFiles":   1,
            "numRows":    len(df),
            "numBytes":   out_path.stat().st_size,
            "path":       str(version_dir),
        }
        commit = self._write_commit(f"WRITE ({mode})", stats)
        logger.info(f"  Delta Lake WRITE: v{commit['version']-1} → v{self._version-1} "
                    f"({len(df):,} rows, mode={mode})")
        return commit

    def read(self, version: int = None) -> "pd.DataFrame":
        """Step 27 — Time travel: read any historical version."""
        import pandas as pd

        if version is None:
            version = self._version - 1

        version_dir = self.base_path / f"version_{version}"
        data_file   = version_dir / "data.csv"

        if not data_file.exists():
            raise FileNotFoundError(f"Delta version {version} not found")

        df = pd.read_csv(data_file)
        logger.info(f"  Delta Lake READ: version={version} ({len(df):,} rows)")
        return df

    def history(self) -> list:
        """Step 27 — Show full transaction history (Delta DESCRIBE HISTORY)."""
        logs = sorted(self.log_path.glob("*.json"))
        history = []
        for log in logs:
            with open(log) as f:
                history.append(json.load(f))
        return history

    def vacuum(self, retain_versions: int = 7):
        """
        Step 27 — Remove old versions (equivalent to VACUUM in Delta Lake).
        Retains last `retain_versions` versions.
        """
        all_versions = sorted(self.base_path.glob("version_*"))
        to_delete    = all_versions[:-retain_versions] if len(all_versions) > retain_versions else []
        for v_dir in to_delete:
            shutil.rmtree(v_dir)
        self._write_commit("VACUUM", {"retainVersions": retain_versions,
                                      "deletedVersions": len(to_delete)})
        logger.info(f"  Delta VACUUM: removed {len(to_delete)} old versions, "
                    f"retained {retain_versions}")
        return len(to_delete)

    def optimize(self):
        """Step 27 — Simulate OPTIMIZE (compaction of small files)."""
        self._write_commit("OPTIMIZE", {"type": "compaction"})
        logger.info("  Delta OPTIMIZE: compaction logged")

    def z_order_by(self, cols: list):
        """Step 27 — Z-ORDER clustering for query performance."""
        self._write_commit("OPTIMIZE (Z-ORDER)", {"zOrderBy": cols})
        logger.info(f"  Delta Z-ORDER BY {cols}: logged (improves skip efficiency)")

    def print_history(self):
        hist = self.history()
        logger.info("  Delta Lake transaction history:")
        for h in hist:
            logger.info(f"    v{h['version']:3d} | {h['timestamp'][:19]} | {h['operation']}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN UPLOAD FUNCTION (called from Airflow DAG + main.py)
# ─────────────────────────────────────────────────────────────────────────────

def upload_to_cloud() -> dict:
    """
    Steps 24-27 — Full cloud pipeline:
      1. Upload processed files to S3 + GCS
      2. Simulate BigQuery analytics
      3. Write to Delta Lake + version history
    """
    results = {}

    print_cloud_comparison()

    # ── Step 24: S3 upload ───────────────────────────────────────────────────
    s3 = S3Client(S3_BUCKET)
    uploads = {}
    today = datetime.now().strftime("%Y/%m/%d")
    file_map = {
        "data/processed/violations.csv":      f"raw/{today}/violations.csv",
        "data/processed/zone_aggregates.csv": f"processed/{today}/zone_aggregates.csv",
        "data/warehouse/violations.db":       f"warehouse/{today}/violations.db",
        "data/reports/dq_report.md":          f"reports/{today}/dq_report.md",
    }
    for local, key in file_map.items():
        if Path(local).exists():
            meta = s3.upload(local, key, metadata={"pipeline": "traffic-v1",
                                                     "date": today})
            uploads[key] = meta

    results["s3_uploads"] = len(uploads)

    # ── Step 24: GCS upload ──────────────────────────────────────────────────
    gcs = GCSClient(GCS_BUCKET)
    gcs_uploads = 0
    for local, key in file_map.items():
        if Path(local).exists():
            gcs.upload(local, key)
            gcs_uploads += 1
    results["gcs_uploads"] = gcs_uploads

    # ── Step 25: EC2 deployment spec ─────────────────────────────────────────
    ec2_info = simulate_ec2_deployment()
    results["ec2"] = ec2_info["state"]

    # ── Step 26: BigQuery analytics ──────────────────────────────────────────
    bq = BigQuerySimulator()
    bq_results = bq.run_all_queries()
    results["bigquery_queries"] = {k: len(v) for k, v in bq_results.items()}

    # ── Step 27: Delta Lake ──────────────────────────────────────────────────
    import pandas as pd
    if Path("data/processed/violations.csv").exists():
        delta = DeltaLakeSimulator("data/delta_lake/violations")
        df = pd.read_csv("data/processed/violations.csv").head(1000)

        delta.write(df, mode="overwrite")             # v0
        delta.optimize()                               # v1 — compaction
        delta.z_order_by(["zone", "severity"])         # v2 — clustering
        delta.write(df.sample(100), mode="append")    # v3 — incremental load
        delta.vacuum(retain_versions=3)                # v4 — cleanup
        delta.print_history()

        # Time travel: read version 0
        # time-travel to latest data version that exists on disk
        existing = sorted([int(p.name.split("_")[1])
                           for p in Path("data/delta_lake/violations").glob("version_*")
                           if p.is_dir()])
        if existing:
            v0_df = delta.read(version=existing[0])
            logger.info(f"  Delta time-travel v{existing[0]}: {len(v0_df)} rows")
        results["delta_versions"] = delta._version

    logger.info(f"  Cloud pipeline complete: {results}")
    return results
