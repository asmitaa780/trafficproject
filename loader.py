

import pandas as pd
import numpy as np
import sqlite3
import logging
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# PRIMARY LOADERS
# ─────────────────────────────────────────────────────────────────────────────

def load_to_sqlite(df: pd.DataFrame, db_path: str = "data/warehouse/violations.db",
                   table: str = "fact_violation", if_exists: str = "replace") -> int:
    """
    Load DataFrame to SQLite (local data warehouse simulation).

    Args:
        df:        Cleaned DataFrame.
        db_path:   Target SQLite database file path.
        table:     Destination table name.
        if_exists: 'replace' | 'append' | 'fail'

    Returns:
        Number of rows written.
    """
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    # SQLite doesn't support all pandas dtypes — coerce categoricals to str
    load_df = df.copy()
    for col in load_df.select_dtypes(include=["category"]).columns:
        load_df[col] = load_df[col].astype(str)
    for col in load_df.select_dtypes(include=["bool"]).columns:
        load_df[col] = load_df[col].astype(int)
    # Ensure datetimes serialise correctly
    for col in load_df.select_dtypes(include=["datetime64"]).columns:
        load_df[col] = load_df[col].astype(str)

    with sqlite3.connect(db_path) as conn:
        load_df.to_sql(table, conn, if_exists=if_exists, index=False, chunksize=10_000)
        # Create indexes for common query patterns
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_zone ON {table}(zone)")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_type ON {table}(violation_type)")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_sev  ON {table}(severity)")
        conn.commit()

    logger.info(f"  SQLite: {len(load_df):,} rows → {table} @ {db_path}")
    return len(load_df)


def export_parquet(df: pd.DataFrame, out_path: str, partition_cols: list = None) -> None:
    """
    Export DataFrame to Parquet format (HDFS-compatible, Spark-readable).

    Args:
        df:             DataFrame to export.
        out_path:       Output file or directory path.
        partition_cols: Columns to partition by (creates Hive-style dirs).
                        e.g. ['zone', 'severity'] → zone=Downtown/severity=High/...

    Notes:
        Parquet preserves dtypes, supports predicate pushdown in Spark,
        and typically compresses 3-8× vs CSV (Steps 11, 13).
    """
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)

    # Parquet can't handle object columns with mixed types — coerce
    save_df = df.copy()
    for col in save_df.select_dtypes(include=["category"]).columns:
        save_df[col] = save_df[col].astype(str)

    if partition_cols and all(c in save_df.columns for c in partition_cols):
        save_df.to_parquet(out_path, index=False, partition_cols=partition_cols,
                           compression="snappy", engine="pyarrow")
        logger.info(f"  Parquet: partitioned by {partition_cols} → {out_path}")
    else:
        save_df.to_parquet(out_path, index=False, compression="snappy")
        size_kb = Path(out_path).stat().st_size / 1024
        logger.info(f"  Parquet: {len(save_df):,} rows → {out_path} ({size_kb:.0f} KB)")


def export_csv(df: pd.DataFrame, out_path: str, chunk: bool = False,
               chunksize: int = 50_000) -> None:
    """Export to CSV — optionally in chunks for large datasets."""
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    if chunk:
        header = True
        for i in range(0, len(df), chunksize):
            df.iloc[i:i+chunksize].to_csv(out_path, mode="a" if i else "w",
                                           header=header, index=False)
            header = False
            logger.debug(f"  CSV chunk {i//chunksize+1} written")
    else:
        df.to_csv(out_path, index=False)
    size_kb = Path(out_path).stat().st_size / 1024
    logger.info(f"  CSV: {len(df):,} rows → {out_path} ({size_kb:.0f} KB)")


# ─────────────────────────────────────────────────────────────────────────────
# STAR SCHEMA LOADER (Steps 8–9)
# ─────────────────────────────────────────────────────────────────────────────

def load_star_schema(df: pd.DataFrame, db_path: str = "data/warehouse/violations.db") -> dict:
    """
    Decompose flat DataFrame into a star schema:
      fact_violation ← dim_time, dim_location, dim_vehicle, dim_violation_type

    Returns:
        dict of row counts per table.
    """
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    # ── dim_time ────────────────────────────────────────────────────────────
    time_cols = [c for c in ["hour", "day_of_week", "is_weekend", "is_peak"] if c in df.columns]
    if time_cols:
        dim_time = df[time_cols].drop_duplicates().reset_index(drop=True)
        dim_time.insert(0, "time_id", range(1, len(dim_time)+1))
        for col in dim_time.select_dtypes(include=["bool","category"]).columns:
            dim_time[col] = dim_time[col].astype(str)
    else:
        dim_time = pd.DataFrame({"time_id": [1], "hour": [0]})

    # ── dim_location ────────────────────────────────────────────────────────
    loc_cols = [c for c in ["zone", "latitude", "longitude"] if c in df.columns]
    dim_location = df[loc_cols].drop_duplicates().reset_index(drop=True) if loc_cols else pd.DataFrame()
    if not dim_location.empty:
        dim_location.insert(0, "loc_id", range(1, len(dim_location)+1))
        for col in dim_location.select_dtypes(include=["category"]).columns:
            dim_location[col] = dim_location[col].astype(str)

    # ── dim_vehicle ─────────────────────────────────────────────────────────
    veh_cols = [c for c in ["vehicle_type"] if c in df.columns]
    if veh_cols:
        dim_vehicle = df[veh_cols].drop_duplicates().reset_index(drop=True)
        dim_vehicle.insert(0, "veh_id", range(1, len(dim_vehicle)+1))
        dim_vehicle["vehicle_type"] = dim_vehicle["vehicle_type"].astype(str)
    else:
        dim_vehicle = pd.DataFrame({"veh_id": [1], "vehicle_type": ["Unknown"]})

    # ── fact_violation ──────────────────────────────────────────────────────
    # Build fact with all columns first, then join surrogate keys
    all_merge_cols = list(set(time_cols + loc_cols + veh_cols))
    fact_base = [c for c in [
        "violation_id", "violation_type", "severity",
        "speed_kmh", "fine_amount", "fine_is_outlier",
        "risk_score", "speed_norm", "fine_norm",
        "ingested_at", "pipeline_run"
    ] if c in df.columns]
    extra = [c for c in all_merge_cols if c in df.columns and c not in fact_base]
    fact = df[fact_base + extra].copy()
    for col in fact.select_dtypes(["category"]).columns:
        fact[col] = fact[col].astype(str)

    # Join surrogate keys
    if "hour" in df.columns and not dim_time.empty:
        merge_cols = [c for c in time_cols if c in fact.columns and c in dim_time.columns]
        if merge_cols:
            dt = dim_time.copy()
            for c in dt.select_dtypes(["category","bool"]).columns:
                dt[c] = dt[c].astype(str)
            fact = fact.merge(dt[["time_id"] + merge_cols], on=merge_cols, how="left")

    if "zone" in df.columns and not dim_location.empty:
        loc_merge = [c for c in loc_cols if c in fact.columns and c in dim_location.columns]
        if loc_merge:
            dl = dim_location.copy()
            for c in dl.select_dtypes(["category"]).columns:
                dl[c] = dl[c].astype(str)
            fact = fact.merge(dl[["loc_id"] + loc_merge], on=loc_merge, how="left")

    if "vehicle_type" in df.columns:
        veh_merge = [c for c in veh_cols if c in fact.columns and c in dim_vehicle.columns]
        if veh_merge:
            dv = dim_vehicle.copy()
            for c in dv.select_dtypes(["category"]).columns:
                dv[c] = dv[c].astype(str)
            fact = fact.merge(dv[["veh_id"] + veh_merge], on=veh_merge, how="left")

    # Coerce for SQLite
    for col in fact.select_dtypes(include=["category", "bool"]).columns:
        fact[col] = fact[col].astype(str)
    for col in fact.select_dtypes(include=["datetime64"]).columns:
        fact[col] = fact[col].astype(str)

    counts = {}
    with sqlite3.connect(db_path) as conn:
        for tbl, frame in [("dim_time", dim_time), ("dim_location", dim_location),
                            ("dim_vehicle", dim_vehicle), ("fact_violation", fact)]:
            if frame is not None and not frame.empty:
                frame.to_sql(tbl, conn, if_exists="replace", index=False)
                counts[tbl] = len(frame)
                logger.info(f"  Star schema: {len(frame):,} rows → {tbl}")

    return counts
