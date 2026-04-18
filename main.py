"""
main.py — Complete Traffic Violations Data Engineering Pipeline
==============================================================
Orchestrates all 30 steps end-to-end.

Run:
  python main.py                      # full pipeline
  python main.py --skip-cloud         # skip cloud (no AWS/GCP creds)
  python main.py --skip-kafka         # skip streaming simulation
  streamlit run dashboard/app.py      # launch dashboard
"""

import argparse
import logging
import sys
from pathlib import Path
from datetime import datetime

from pipeline.ingestion      import ingest_csv_files
from pipeline.cleaning       import clean_dataframe
from pipeline.transform      import (normalize, aggregate_by_zone,
                                     validate_schema, compute_risk_score)
from pipeline.quality        import (run_dq_checks, detect_anomalies,
                                     detect_temporal_spikes, generate_dq_report)
from pipeline.loader         import (load_to_sqlite, export_parquet, export_csv,
                                     load_star_schema)
from pipeline.hadoop_hdfs    import HDFSClient, setup_hdfs_pipeline, upload_pipeline_outputs
from pipeline.kafka_streaming import run_kafka_simulation
from pipeline.cloud          import upload_to_cloud
from pipeline.utils          import setup_dirs, setup_logging, log_run_summary


def parse_args():
    p = argparse.ArgumentParser(description="Traffic Violations Pipeline")
    p.add_argument("--skip-spark",  action="store_true")
    p.add_argument("--skip-kafka",  action="store_true")
    p.add_argument("--skip-cloud",  action="store_true")
    p.add_argument("--skip-hdfs",   action="store_true")
    p.add_argument("--n-kafka",     type=int, default=300)
    return p.parse_args()


def main():
    args  = parse_args()
    start = datetime.now()
    setup_dirs()
    logger = setup_logging()

    logger.info("=" * 65)
    logger.info("TRAFFIC VIOLATIONS PIPELINE — ALL 30 STEPS")
    logger.info("=" * 65)

    # STEPS 1-3
    logger.info("\n[STAGE 1/9] Ingestion — Steps 1-3")
    raw_df = ingest_csv_files(Path("data/raw"))
    logger.info(f"  Loaded {len(raw_df):,} records")

    # STEPS 4-7
    logger.info("\n[STAGE 2/9] Python ETL — Steps 4-7")
    clean_df = clean_dataframe(raw_df)
    clean_df = normalize(clean_df, col="speed_kmh",   alias="speed_norm")
    clean_df = normalize(clean_df, col="fine_amount",  alias="fine_norm")
    clean_df = compute_risk_score(clean_df)
    zone_agg = aggregate_by_zone(clean_df)
    schema_report = validate_schema(clean_df)
    logger.info(f"  Clean: {len(clean_df):,} rows | Schema score: {schema_report['score']:.1f}%")

    # STEPS 8-11
    logger.info("\n[STAGE 3/9] SQL & Warehouse — Steps 8-11")
    load_to_sqlite(clean_df, db_path="data/warehouse/violations.db")
    star_counts = load_star_schema(clean_df, db_path="data/warehouse/violations_star.db")
    logger.info(f"  Star schema: {list(star_counts.keys())}")
    try:
        export_parquet(clean_df, out_path="data/processed/violations.parquet")
        export_parquet(zone_agg,  out_path="data/processed/zone_aggregates.parquet")
    except ImportError:
        export_csv(clean_df, out_path="data/processed/violations.csv")
        export_csv(zone_agg,  out_path="data/processed/zone_aggregates.csv")

    # STEPS 11-12: HDFS
    if not args.skip_hdfs:
        logger.info("\n[STAGE 4/9] HDFS — Steps 11-12")
        hdfs = HDFSClient()
        setup_hdfs_pipeline(hdfs)
        hdfs_uploads = upload_pipeline_outputs(hdfs)
        logger.info(f"  HDFS: {len(hdfs_uploads)} files registered")

    # STEPS 13-16: Spark
    if not args.skip_spark:
        logger.info("\n[STAGE 5/9] Spark — Steps 13-16")
        try:
            from pipeline.spark_jobs import run_spark_pipeline
            f = ("data/processed/violations.parquet"
                 if Path("data/processed/violations.parquet").exists()
                 else "data/processed/violations.csv")
            run_spark_pipeline(input_path=f, output_path="data/spark_output")
        except ImportError:
            logger.warning("  PySpark not installed. pip install pyspark (needs Java 11+)")

    # STEPS 17-20: Kafka
    if not args.skip_kafka:
        logger.info(f"\n[STAGE 6/9] Kafka — Steps 17-20")
        kafka_result = run_kafka_simulation(n_events=args.n_kafka)
        logger.info(f"  Produced: {kafka_result['produced']} | Consumed: {kafka_result['consumed']}")

    # STEPS 21-22: Airflow DAG
    logger.info("\n[STAGE 7/9] Airflow DAG — Steps 21-22")
    logger.info("  DAG file: dags/traffic_pipeline_dag.py")
    logger.info("  Deploy:   cp dags/ $AIRFLOW_HOME/dags/ && airflow dags trigger traffic_violations_pipeline")

    # STEPS 23-27: Cloud
    if not args.skip_cloud:
        logger.info("\n[STAGE 8/9] Cloud — Steps 23-27")
        cloud_result = upload_to_cloud()
        logger.info(f"  S3: {cloud_result.get('s3_uploads',0)} | GCS: {cloud_result.get('gcs_uploads',0)} | Delta v{cloud_result.get('delta_versions',0)}")

    # STEP 28: Data Quality
    logger.info("\n[STAGE 9/9] Data Quality — Step 28")
    dq_result  = run_dq_checks(clean_df)
    anomalies  = detect_anomalies(clean_df, col="fine_amount")
    spikes     = detect_temporal_spikes(clean_df)
    Path("data/reports").mkdir(exist_ok=True)
    generate_dq_report(dq_result, anomalies, out_path="data/reports/dq_report.md")
    logger.info(f"  DQ score: {dq_result['score']:.1f}% | Anomalies: {len(anomalies)} | Spikes: {len(spikes)}")

    elapsed = (datetime.now() - start).total_seconds()
    log_run_summary(logger, raw_df, clean_df, dq_result, anomalies, elapsed)

    logger.info("\n" + "=" * 65)
    logger.info("STEPS 29-30: Launch dashboard:")
    logger.info("  pip install streamlit plotly")
    logger.info("  streamlit run dashboard/app.py")
    logger.info("=" * 65)


if __name__ == "__main__":
    main()
