"""
pipeline/spark_jobs.py
──────────────────────
Step 13 — Create Spark job to process large dataset
Step 14 — DataFrame transformations and actions
Step 15 — Query large dataset using Spark SQL
Step 16 — Optimize Spark jobs using partitioning and caching

Usage (local mode — no cluster needed):
    python -m pipeline.spark_jobs

With real cluster:
    export SPARK_MASTER=spark://master:7077
    python -m pipeline.spark_jobs

Requires: pip install pyspark
"""

import os
import logging
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

SPARK_MASTER       = os.getenv("SPARK_MASTER", "local[*]")
SPARK_APP_NAME     = "TrafficViolationsPipeline"
SHUFFLE_PARTITIONS = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "16"))
INPUT_PATH         = os.getenv("SPARK_INPUT",  "data/processed/violations.csv")
OUTPUT_PATH        = os.getenv("SPARK_OUTPUT", "data/spark_output")


def get_spark_session():
    """
    Build a configured SparkSession.
    Step 16 — Optimization configs are set here at session level.
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        raise ImportError(
            "PySpark not installed. Run: pip install pyspark\n"
            "Also requires Java 11+: sudo apt install openjdk-11-jdk"
        )

    spark = (
        SparkSession.builder
        .master(SPARK_MASTER)
        .appName(SPARK_APP_NAME)
        # ── Step 16: Optimization settings ────────────────────────────────
        .config("spark.sql.shuffle.partitions",       str(SHUFFLE_PARTITIONS))
        .config("spark.sql.adaptive.enabled",         "true")   # AQE — auto re-partitions
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", "10mb") # broadcast small dims
        .config("spark.serializer",  "org.apache.spark.serializer.KryoSerializer")
        .config("spark.driver.memory",  "2g")
        .config("spark.executor.memory","2g")
        .config("spark.ui.showConsoleProgress", "false")
        # Delta Lake (Step 27) — comment out if delta-spark not installed
        # .config("spark.sql.extensions",
        #         "io.delta.sql.DeltaSparkSessionExtension")
        # .config("spark.sql.catalog.spark_catalog",
        #         "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"SparkSession ready — master={SPARK_MASTER}, "
                f"shuffle_partitions={SHUFFLE_PARTITIONS}")
    return spark


# ─────────────────────────────────────────────────────────────────────────────
# STEP 13 — Main Spark job entry point
# ─────────────────────────────────────────────────────────────────────────────

def run_spark_pipeline(input_path: str = INPUT_PATH,
                       output_path: str = OUTPUT_PATH) -> dict:
    """
    Full Spark ETL job:
      1. Read CSV from HDFS / local
      2. Apply DataFrame transformations (Step 14)
      3. Repartition + cache (Step 16)
      4. Run Spark SQL analytics (Step 15)
      5. Write output as Parquet (Step 16 — columnar storage)

    Returns dict of result summaries.
    """
    spark = get_spark_session()
    results = {}

    logger.info("[Spark] Stage 1 — Reading data...")
    df = read_violations(spark, input_path)
    logger.info(f"[Spark] Loaded {df.count():,} rows, {len(df.columns)} columns")

    logger.info("[Spark] Stage 2 — Transformations...")
    df = apply_transformations(df)

    logger.info("[Spark] Stage 3 — Repartition + cache (Step 16)...")
    df = optimize_partitioning(df)

    logger.info("[Spark] Stage 4 — Spark SQL analytics (Step 15)...")
    results["zone_summary"]     = run_zone_sql(spark, df)
    results["hourly_trend"]     = run_hourly_trend_sql(spark, df)
    results["severity_report"]  = run_severity_report_sql(spark, df)
    results["window_functions"] = run_window_functions(spark, df)

    logger.info("[Spark] Stage 5 — Writing output...")
    write_output(df, output_path)

    df.unpersist()
    logger.info("[Spark] Cache released. Job complete.")
    spark.stop()
    return results


# ─────────────────────────────────────────────────────────────────────────────
# STEP 13 — Read
# ─────────────────────────────────────────────────────────────────────────────

def read_violations(spark, path: str):
    """
    Read violations CSV with inferred schema.
    In production: read from HDFS path hdfs:///traffic/processed/violations.csv
    or Parquet:    spark.read.parquet("hdfs:///traffic/processed/violations.parquet")
    """
    from pyspark.sql.types import (
        StructType, StructField, StringType, FloatType, IntegerType, BooleanType
    )

    # Explicit schema — faster than inferSchema=True on large files
    schema = StructType([
        StructField("violation_id",   StringType(),  True),
        StructField("timestamp",      StringType(),  True),
        StructField("violation_type", StringType(),  True),
        StructField("zone",           StringType(),  True),
        StructField("speed_kmh",      FloatType(),   True),
        StructField("fine_amount",    FloatType(),   True),
        StructField("vehicle_type",   StringType(),  True),
        StructField("officer_id",     StringType(),  True),
        StructField("latitude",       FloatType(),   True),
        StructField("longitude",      FloatType(),   True),
        StructField("hour",           IntegerType(), True),
        StructField("day_of_week",    StringType(),  True),
        StructField("is_weekend",     StringType(),  True),
        StructField("is_peak",        StringType(),  True),
        StructField("severity",       StringType(),  True),
        StructField("fine_amount",    FloatType(),   True),
        StructField("risk_score",     FloatType(),   True),
    ])

    df = (
        spark.read
        .option("header", "true")
        .option("mode", "DROPMALFORMED")
        .option("nullValue", "")
        .csv(path)           # schema inference used here for flexibility
    )
    return df


# ─────────────────────────────────────────────────────────────────────────────
# STEP 14 — DataFrame Transformations & Actions
# ─────────────────────────────────────────────────────────────────────────────

def apply_transformations(df):
    """
    Step 14 — Core DataFrame transformations.
    These are LAZY (no computation until an action like .count() or .show()).
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import FloatType, IntegerType

    # Cast types correctly
    df = (df
        .withColumn("speed_kmh",   F.col("speed_kmh").cast(FloatType()))
        .withColumn("fine_amount", F.col("fine_amount").cast(FloatType()))
        .withColumn("hour",        F.col("hour").cast(IntegerType()))
    )

    # Drop nulls in critical columns
    df = df.dropna(subset=["violation_type", "zone", "severity"])

    # Derive is_peak if not already present
    if "is_peak" not in df.columns:
        df = df.withColumn(
            "is_peak",
            F.col("hour").between(7, 9) | F.col("hour").between(17, 19)
        )

    # Speed category — bucketing (useful for GROUP BY later)
    df = df.withColumn(
        "speed_category",
        F.when(F.col("speed_kmh") <= 0,   "stationary")
         .when(F.col("speed_kmh") <= 15,  "slow")
         .when(F.col("speed_kmh") <= 40,  "moderate")
         .when(F.col("speed_kmh") <= 80,  "fast")
         .otherwise("excessive")
    )

    # Fine tier
    df = df.withColumn(
        "fine_tier",
        F.when(F.col("fine_amount") <= 100, "low")
         .when(F.col("fine_amount") <= 300, "medium")
         .otherwise("high")
    )

    # Normalise speed (min-max) using Spark
    speed_min = 0.0
    speed_max = 200.0
    df = df.withColumn(
        "speed_norm_spark",
        (F.col("speed_kmh") - speed_min) / (speed_max - speed_min)
    )

    # Revenue contribution (per record fine as % of zone total)
    # This is computed via a window function — see run_window_functions()

    logger.info(f"  Transformations applied: {len(df.columns)} columns")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# STEP 16 — Partitioning & Caching Optimizations
# ─────────────────────────────────────────────────────────────────────────────

def optimize_partitioning(df):
    """
    Step 16 — Optimization strategy:
      1. repartition() by query key → eliminates shuffle during GROUP BY
      2. cache()        → pin to memory for repeated access
      3. Trigger action → materialise the cache now (not lazily)
    """
    from pyspark.sql import functions as F
    from pyspark import StorageLevel

    # Repartition by the most common GROUP BY key
    df = df.repartition(SHUFFLE_PARTITIONS, "zone", "hour")

    # Cache at MEMORY_AND_DISK — spills to disk if RAM is tight
    df = df.persist(StorageLevel.MEMORY_AND_DISK)

    # Trigger materialisation (cache is lazy without an action)
    count = df.count()
    logger.info(f"  Cached {count:,} rows across {SHUFFLE_PARTITIONS} partitions")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# STEP 15 — Spark SQL Queries
# ─────────────────────────────────────────────────────────────────────────────

def run_zone_sql(spark, df):
    """Step 15 — Zone-level aggregation via Spark SQL."""
    df.createOrReplaceTempView("violations")
    result = spark.sql("""
        SELECT
            zone,
            violation_type,
            COUNT(*)                        AS total_violations,
            ROUND(AVG(fine_amount), 2)      AS avg_fine,
            ROUND(SUM(fine_amount), 2)      AS total_revenue,
            ROUND(MAX(fine_amount), 2)      AS max_fine,
            ROUND(AVG(speed_kmh), 2)        AS avg_speed
        FROM violations
        WHERE fine_amount IS NOT NULL
          AND zone IS NOT NULL
        GROUP BY zone, violation_type
        ORDER BY total_revenue DESC
    """)
    result.show(10, truncate=False)
    return result


def run_hourly_trend_sql(spark, df):
    """Step 15 — Hourly violation trend for time-series analysis."""
    df.createOrReplaceTempView("violations")
    result = spark.sql("""
        SELECT
            hour,
            COUNT(*)                   AS violations_count,
            ROUND(AVG(fine_amount), 2) AS avg_fine,
            SUM(CASE WHEN severity = 'High' THEN 1 ELSE 0 END) AS high_severity_count,
            SUM(CASE WHEN is_peak = 'True' THEN 1 ELSE 0 END)  AS peak_hour_count
        FROM violations
        GROUP BY hour
        ORDER BY hour
    """)
    result.show(24, truncate=False)
    return result


def run_severity_report_sql(spark, df):
    """Step 15 — Severity breakdown with percentage share (subquery pattern)."""
    df.createOrReplaceTempView("violations")
    result = spark.sql("""
        WITH totals AS (
            SELECT COUNT(*) AS grand_total FROM violations
        )
        SELECT
            v.severity,
            COUNT(*)                                          AS count,
            ROUND(AVG(v.fine_amount), 2)                     AS avg_fine,
            ROUND(COUNT(*) * 100.0 / t.grand_total, 1)       AS pct_share
        FROM violations v
        CROSS JOIN totals t
        GROUP BY v.severity, t.grand_total
        ORDER BY avg_fine DESC
    """)
    result.show()
    return result


def run_window_functions(spark, df):
    """
    Step 15 + Advanced SQL —
    Window functions: RANK, DENSE_RANK, LAG, running totals.
    These mirror Step 6 (Advanced SQL window functions) in PySpark.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    # Window: rank violations by fine within each zone
    win_zone = Window.partitionBy("zone").orderBy(F.desc("fine_amount"))
    win_hour = Window.partitionBy("zone").orderBy("hour")

    result = (
        df.select("violation_id", "zone", "violation_type",
                  "fine_amount", "severity", "hour")
          .withColumn("rank_in_zone",     F.rank()      .over(win_zone))
          .withColumn("dense_rank_zone",  F.dense_rank().over(win_zone))
          .withColumn("prev_hour_fine",   F.lag("fine_amount", 1).over(win_hour))
          .withColumn("running_revenue",  F.sum("fine_amount")  .over(win_hour))
          .filter(F.col("rank_in_zone") <= 5)   # top 5 per zone
    )
    result.show(20, truncate=False)
    return result


# ─────────────────────────────────────────────────────────────────────────────
# STEP 16 — Write optimized output
# ─────────────────────────────────────────────────────────────────────────────

def write_output(df, output_path: str):
    """
    Write final DataFrame as partitioned Parquet.
    Partitioning by zone+severity allows Spark/Presto to skip
    irrelevant file groups via predicate pushdown.
    """
    Path(output_path).mkdir(parents=True, exist_ok=True)

    (df.write
       .mode("overwrite")
       .partitionBy("zone", "severity")        # Hive-style partitioning
       .option("compression", "snappy")
       .parquet(f"{output_path}/violations_partitioned")
    )
    logger.info(f"  Parquet written to {output_path}/violations_partitioned")

    # Also write zone summary as single CSV for dashboards
    df.createOrReplaceTempView("violations")
    from pyspark.sql import functions as F
    zone_summary = (
        df.groupBy("zone", "violation_type", "severity")
          .agg(
              F.count("*").alias("count"),
              F.round(F.avg("fine_amount"), 2).alias("avg_fine"),
              F.round(F.sum("fine_amount"), 2).alias("total_revenue"),
          )
          .orderBy(F.desc("total_revenue"))
    )
    (zone_summary.coalesce(1)           # single output file
                 .write
                 .mode("overwrite")
                 .option("header", "true")
                 .csv(f"{output_path}/zone_summary"))
    logger.info(f"  Zone summary CSV written to {output_path}/zone_summary")


# ─────────────────────────────────────────────────────────────────────────────
# STANDALONE ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    input_file = INPUT_PATH
    if not Path(input_file).exists():
        print(f"Input not found: {input_file}")
        print("Run main.py first to generate violations.csv")
    else:
        run_spark_pipeline(input_file, OUTPUT_PATH)
