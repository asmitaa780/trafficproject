"""
pipeline/cleaning.py
────────────────────
Step 4  — Python basics: read CSVs, clean missing values, merge datasets
Step 6  — Pandas + NumPy: memory optimization, large-scale analysis

Handles:
  • Missing value imputation (median / mode / forward-fill)
  • Outlier capping (IQR method)
  • Duplicate detection & removal
  • Type coercion
  • Memory footprint reduction via downcasting
"""

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

# ── Domain constants ────────────────────────────────────────────────────────
VALID_VIOLATION_TYPES = {
    "Speeding", "Red-Light", "DUI",
    "Illegal Park", "Reckless", "Mobile Use", "No Seatbelt",
}
VALID_ZONES = {"Downtown", "Highway", "School Zone", "Residential", "Industrial"}
SPEED_BOUNDS = (0.0, 200.0)     # km/h physical limits
FINE_BOUNDS  = (0.0, 5_000.0)   # $ reasonable range


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Full cleaning pipeline — applies all cleaning steps in order.

    Returns a clean copy; original df is never mutated.
    """
    df = df.copy()
    original_len = len(df)

    logger.info("  [clean] Removing duplicates...")
    df = _remove_duplicates(df)

    logger.info("  [clean] Dropping rows missing required fields...")
    df = _drop_required_nulls(df)

    logger.info("  [clean] Standardizing categoricals...")
    df = _standardize_categoricals(df)

    logger.info("  [clean] Imputing missing values...")
    df = _impute_missing(df)

    logger.info("  [clean] Capping outliers...")
    df = _cap_outliers(df)

    logger.info("  [clean] Parsing timestamps...")
    df = _parse_timestamps(df)

    logger.info("  [clean] Deriving severity column...")
    df = _derive_severity(df)

    logger.info("  [clean] Downcasting numeric types (memory opt)...")
    df = _downcast_numerics(df)

    logger.info(f"  [clean] Done — kept {len(df):,}/{original_len:,} rows")
    return df.reset_index(drop=True)


# ── Individual cleaning steps ───────────────────────────────────────────────

def _remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove exact duplicates and duplicate violation IDs (keep first)."""
    before = len(df)
    df = df.drop_duplicates()
    if "violation_id" in df.columns:
        df = df.drop_duplicates(subset=["violation_id"], keep="first")
    logger.info(f"    Dropped {before - len(df)} duplicates")
    return df


def _drop_required_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows where critical identifying fields are null."""
    required = [c for c in ["violation_type", "zone", "timestamp"] if c in df.columns]
    before = len(df)
    df = df.dropna(subset=required)
    logger.info(f"    Dropped {before - len(df)} rows missing required fields")
    return df


def _standardize_categoricals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize free-text categorical fields to canonical values.
    Unknown values are mapped to 'Unknown' rather than dropped.
    """
    if "violation_type" in df.columns:
        df["violation_type"] = (
            df["violation_type"]
            .str.strip().str.title()
            .where(df["violation_type"].str.strip().str.title().isin(VALID_VIOLATION_TYPES),
                   other="Unknown")
        )

    if "zone" in df.columns:
        zone_map = {
            "downtown":    "Downtown",
            "highway":     "Highway",
            "school zone": "School Zone",
            "school":      "School Zone",
            "residential": "Residential",
            "industrial":  "Industrial",
        }
        df["zone"] = (
            df["zone"].str.strip().str.lower()
              .map(zone_map)
              .fillna("Unknown")
        )

    if "vehicle_type" in df.columns:
        df["vehicle_type"] = df["vehicle_type"].str.strip().str.title()

    return df


def _impute_missing(df: pd.DataFrame) -> pd.DataFrame:
    """
    Impute missing values using contextually appropriate strategies:
      • fine_amount  → median within (zone, violation_type) group
      • speed_kmh    → median within violation_type group
      • officer_id   → 'UNKNOWN'
      • vehicle_type → mode
    """
    # Group-median imputation for fine_amount
    if "fine_amount" in df.columns:
        group_median = (
            df.groupby(["zone", "violation_type"])["fine_amount"]
              .transform("median")
        )
        df["fine_amount"] = df["fine_amount"].fillna(group_median)
        # Fallback: global median for groups that are entirely NaN
        df["fine_amount"] = df["fine_amount"].fillna(df["fine_amount"].median())

    # Group-median imputation for speed
    if "speed_kmh" in df.columns:
        group_median = df.groupby("violation_type")["speed_kmh"].transform("median")
        df["speed_kmh"] = df["speed_kmh"].fillna(group_median)
        df["speed_kmh"] = df["speed_kmh"].fillna(0.0)

    # Categorical fallbacks
    if "officer_id" in df.columns:
        df["officer_id"] = df["officer_id"].fillna("UNKNOWN")
    if "vehicle_type" in df.columns:
        mode = df["vehicle_type"].mode()
        df["vehicle_type"] = df["vehicle_type"].fillna(mode[0] if len(mode) else "Unknown")

    # Coordinate fallbacks (use zone centroid — simplified)
    for coord in ["latitude", "longitude"]:
        if coord in df.columns:
            df[coord] = df[coord].fillna(df[coord].median())

    return df


def _cap_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cap numeric fields to domain-valid ranges, then apply IQR
    outlier clipping for fine_amount.
    """
    if "speed_kmh" in df.columns:
        df["speed_kmh"] = df["speed_kmh"].clip(*SPEED_BOUNDS)

    if "fine_amount" in df.columns:
        # Hard cap first
        df["fine_amount"] = df["fine_amount"].clip(*FINE_BOUNDS)
        # IQR soft cap (flag extreme outliers rather than drop)
        Q1 = df["fine_amount"].quantile(0.25)
        Q3 = df["fine_amount"].quantile(0.75)
        IQR = Q3 - Q1
        lower, upper = Q1 - 3 * IQR, Q3 + 3 * IQR
        df["fine_is_outlier"] = ~df["fine_amount"].between(lower, upper)
        n_outliers = df["fine_is_outlier"].sum()
        if n_outliers:
            logger.info(f"    Flagged {n_outliers} fine_amount outliers (IQR×3)")

    return df


def _parse_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    """Parse timestamp string → datetime and extract time features."""
    if "timestamp" not in df.columns:
        return df
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["hour"]      = df["timestamp"].dt.hour
    df["day_of_week"] = df["timestamp"].dt.day_name()
    df["is_weekend"]  = df["timestamp"].dt.dayofweek >= 5
    df["is_peak"]     = df["hour"].between(7, 9) | df["hour"].between(17, 19)
    return df


def _derive_severity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Derive severity label from violation type and speed.
    High > Medium > Low — used for star schema dimension.
    """
    def classify(row):
        vtype = row.get("violation_type", "")
        speed = row.get("speed_kmh", 0) or 0
        if vtype in ("DUI", "Reckless", "Red-Light"):
            return "High"
        if vtype == "Speeding":
            return "High" if speed > 30 else ("Medium" if speed > 15 else "Low")
        if vtype == "Mobile Use":
            return "Medium"
        return "Low"

    # Vectorized version (fast for 1M+ rows)
    conditions = [
        df["violation_type"].isin(["DUI", "Reckless", "Red-Light"]),
        (df["violation_type"] == "Speeding") & (df["speed_kmh"] > 30),
        (df["violation_type"] == "Speeding") & (df["speed_kmh"].between(15, 30)),
        df["violation_type"] == "Mobile Use",
    ]
    choices = ["High", "High", "Medium", "Medium"]
    df["severity"] = np.select(conditions, choices, default="Low")
    return df


def _downcast_numerics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Step 6 — Memory optimization:
    Downcast float64 → float32, int64 → int32 where safe.
    Converts low-cardinality strings to Categorical.
    Typical savings: 40-60% on large datasets.
    """
    for col in df.select_dtypes(include=["float64"]).columns:
        df[col] = pd.to_numeric(df[col], downcast="float")

    for col in df.select_dtypes(include=["int64"]).columns:
        df[col] = pd.to_numeric(df[col], downcast="integer")

    low_card_cols = ["violation_type", "zone", "severity",
                     "vehicle_type", "day_of_week"]
    for col in low_card_cols:
        if col in df.columns:
            df[col] = df[col].astype("category")

    mem_mb = df.memory_usage(deep=True).sum() / 1_048_576
    logger.info(f"    Memory footprint after downcast: {mem_mb:.1f} MB")
    return df
