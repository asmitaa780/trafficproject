"""
pipeline/ingestion.py
─────────────────────
Step 3  — Read multiple CSV files
Step 10 — Batch ingestion pipeline from CSV to database

Reads all .csv files from a directory, adds provenance metadata,
and returns a single merged DataFrame. Handles encoding errors,
malformed rows, and missing headers gracefully.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
import hashlib

logger = logging.getLogger(__name__)

# ── Expected raw schema ─────────────────────────────────────────────────────
RAW_SCHEMA = {
    "violation_id":    str,
    "timestamp":       str,
    "violation_type":  str,
    "zone":            str,
    "speed_kmh":       float,
    "fine_amount":     float,
    "vehicle_type":    str,
    "officer_id":      str,
    "latitude":        float,
    "longitude":       float,
}

REQUIRED_COLS = ["violation_id", "timestamp", "violation_type", "zone"]


def ingest_csv_files(directory: Path, chunksize: int = 50_000) -> pd.DataFrame:
    """
    Read every .csv file in `directory`, enforce schema, and merge.

    Args:
        directory:  Path to folder containing raw CSV files.
        chunksize:  Rows per chunk — enables 1M+ row processing (Step 6).

    Returns:
        Merged DataFrame with provenance columns added.
    """
    directory = Path(directory)
    csv_files = sorted(directory.glob("*.csv"))

    if not csv_files:
        logger.warning(f"No CSV files found in {directory}. Generating synthetic data.")
        return _generate_synthetic_data(n=100_000)

    frames = []
    for csv_path in csv_files:
        logger.info(f"  Reading: {csv_path.name}")
        df = _read_csv_chunked(csv_path, chunksize)
        if df is not None:
            frames.append(df)

    if not frames:
        raise RuntimeError("All CSV files failed to load — check logs.")

    merged = pd.concat(frames, ignore_index=True)
    merged = _add_provenance(merged)
    merged = _cast_schema(merged)
    return merged


def _read_csv_chunked(path: Path, chunksize: int) -> pd.DataFrame | None:
    """Read a single CSV in chunks, handling encoding & bad rows."""
    chunks = []
    try:
        reader = pd.read_csv(
            path,
            chunksize=chunksize,
            encoding="utf-8",
            on_bad_lines="warn",       # skip malformed rows, don't crash
            low_memory=False,
        )
        for chunk in reader:
            # Drop fully empty rows immediately (saves memory)
            chunk.dropna(how="all", inplace=True)
            # Rename common alternate column names
            chunk = _normalize_headers(chunk)
            chunks.append(chunk)

        df = pd.concat(chunks, ignore_index=True)
        df["source_file"] = path.name
        logger.info(f"    → {len(df):,} rows loaded")
        return df

    except UnicodeDecodeError:
        logger.warning(f"UTF-8 failed for {path.name}, retrying with latin-1")
        try:
            df = pd.read_csv(path, encoding="latin-1", on_bad_lines="warn")
            df["source_file"] = path.name
            return df
        except Exception as e:
            logger.error(f"Failed to load {path.name}: {e}")
            return None

    except Exception as e:
        logger.error(f"Error reading {path.name}: {e}")
        return None


def _normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    """Map common column name variants to canonical names."""
    rename_map = {
        "speed":      "speed_kmh",
        "Speed":      "speed_kmh",
        "fine":       "fine_amount",
        "Fine":       "fine_amount",
        "type":       "violation_type",
        "Type":       "violation_type",
        "lat":        "latitude",
        "lon":        "longitude",
        "lng":        "longitude",
        "officer":    "officer_id",
        "vehicle":    "vehicle_type",
        "id":         "violation_id",
    }
    return df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})


def _cast_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Cast columns to expected dtypes, coercing errors to NaN."""
    for col, dtype in RAW_SCHEMA.items():
        if col not in df.columns:
            df[col] = np.nan if dtype == float else None
            continue
        if dtype == float:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif dtype == str:
            df[col] = df[col].astype(str).str.strip()
    return df


def _add_provenance(df: pd.DataFrame) -> pd.DataFrame:
    """Add pipeline metadata columns for lineage tracking."""
    df["ingested_at"]  = pd.Timestamp.now().isoformat()
    df["pipeline_run"] = hashlib.md5(str(pd.Timestamp.now()).encode()).hexdigest()[:8]
    return df


def _generate_synthetic_data(n: int = 100_000) -> pd.DataFrame:
    """
    Generate realistic synthetic traffic violations data.
    Used when no raw CSV files are present (demo / test mode).
    """
    rng = np.random.default_rng(42)

    TYPES  = ["Speeding", "Red-Light", "DUI", "Illegal Park",
              "Reckless", "Mobile Use", "No Seatbelt"]
    ZONES  = ["Downtown", "Highway", "School Zone", "Residential", "Industrial"]
    VEHCLS = ["Car", "Truck", "Motorcycle", "Bus", "Van"]

    vtype  = rng.choice(TYPES,  n)
    zone   = rng.choice(ZONES,  n)
    veh    = rng.choice(VEHCLS, n)
    speed  = np.where(vtype == "Speeding",
                      rng.integers(5, 80, n),
                      rng.integers(0, 25, n)).astype(float)

    FINE_MAP = {
        "Downtown":    {"High": 500, "Medium": 250, "Low": 100},
        "Highway":     {"High": 600, "Medium": 300, "Low": 150},
        "School Zone": {"High": 800, "Medium": 400, "Low": 200},
        "Residential": {"High": 350, "Medium": 175, "Low":  75},
        "Industrial":  {"High": 400, "Medium": 200, "Low": 100},
    }

    def severity(t, s):
        if t in ("DUI", "Reckless", "Red-Light"):    return "High"
        if t == "Speeding":                           return "High" if s > 30 else ("Medium" if s > 15 else "Low")
        if t == "Mobile Use":                         return "Medium"
        return "Low"

    sev   = [severity(t, s) for t, s in zip(vtype, speed)]
    fines = [FINE_MAP[z][s] for z, s in zip(zone, sev)]
    # Inject 5% missing fines
    mask  = rng.random(n) < 0.05
    fines = [None if m else f for m, f in zip(mask, fines)]

    timestamps = pd.date_range("2024-01-01", periods=n, freq="5min")

    df = pd.DataFrame({
        "violation_id":   [f"TRF-{i:06d}" for i in range(1, n+1)],
        "timestamp":      timestamps,
        "violation_type": vtype,
        "zone":           zone,
        "speed_kmh":      speed,
        "fine_amount":    fines,
        "vehicle_type":   veh,
        "officer_id":     [f"OFF-{rng.integers(1,50):03d}" for _ in range(n)],
        "latitude":       rng.uniform(28.40, 28.90, n).round(6),
        "longitude":      rng.uniform(77.00, 77.40, n).round(6),
        "source_file":    "synthetic_data.csv",
    })

    df["ingested_at"]  = pd.Timestamp.now().isoformat()
    df["pipeline_run"] = "synthetic"
    return df
