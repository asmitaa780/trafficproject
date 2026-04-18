"""
pipeline/transform.py
─────────────────────
Step 5 — Advanced Python: reusable modules for data transformation.

Provides:
  • normalize()        — min-max normalization
  • aggregate_by_zone()— grouped aggregation with multiple statistics
  • validate_schema()  — schema contract enforcement
  • encode_categoricals()— label encoding for ML readiness
  • compute_risk_score() — composite feature engineering
"""

import pandas as pd
import numpy as np
import logging
from typing import Optional

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# NORMALIZATION
# ─────────────────────────────────────────────────────────────────────────────

def normalize(
    df: pd.DataFrame,
    col: str,
    alias: Optional[str] = None,
    method: str = "minmax",
) -> pd.DataFrame:
    """
    Normalize a numeric column.

    Args:
        df:     Input DataFrame.
        col:    Column to normalize.
        alias:  Output column name (defaults to col + '_norm').
        method: 'minmax' | 'zscore' | 'robust'
                  minmax  → scale to [0, 1]
                  zscore  → subtract mean, divide by std
                  robust  → subtract median, divide by IQR (outlier-resistant)

    Returns:
        DataFrame with normalized column appended.

    Example:
        df = normalize(df, col='speed_kmh', method='minmax')
    """
    if col not in df.columns:
        logger.warning(f"normalize: column '{col}' not found, skipping")
        return df

    out = alias or f"{col}_norm"
    series = df[col].astype(float)

    if method == "minmax":
        mn, mx = series.min(), series.max()
        if mx == mn:
            df[out] = 0.0
        else:
            df[out] = (series - mn) / (mx - mn)

    elif method == "zscore":
        mean, std = series.mean(), series.std()
        df[out] = (series - mean) / std if std > 0 else 0.0

    elif method == "robust":
        median = series.median()
        q75, q25 = series.quantile(0.75), series.quantile(0.25)
        iqr = q75 - q25
        df[out] = (series - median) / iqr if iqr > 0 else 0.0

    else:
        raise ValueError(f"Unknown normalization method: '{method}'. "
                         f"Choose from: minmax, zscore, robust")

    logger.info(f"  Normalized '{col}' → '{out}' (method={method})")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# AGGREGATION
# ─────────────────────────────────────────────────────────────────────────────

def aggregate_by_zone(df: pd.DataFrame) -> pd.DataFrame:
    """
    Produce a rich zone-level summary table.

    Returns one row per (zone, violation_type) with:
      total_violations, avg_fine, max_fine, total_revenue,
      pct_high_severity, avg_speed, peak_hour
    """
    required = {"zone", "violation_type", "fine_amount", "severity"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"aggregate_by_zone: missing columns {missing}")

    # Convert categoricals for groupby compatibility
    work = df.copy()
    for c in ["zone", "violation_type", "severity"]:
        if hasattr(work[c], "cat"):
            work[c] = work[c].astype(str)

    agg = (
        work
        .groupby(["zone", "violation_type"], observed=True)
        .agg(
            total_violations =("violation_id",  "count"  if "violation_id" in work.columns else "size"),
            avg_fine         =("fine_amount",    "mean"  ),
            max_fine         =("fine_amount",    "max"   ),
            min_fine         =("fine_amount",    "min"   ),
            total_revenue    =("fine_amount",    "sum"   ),
            avg_speed        =("speed_kmh",      "mean"  ) if "speed_kmh" in work.columns else ("fine_amount","count"),
        )
        .round(2)
        .reset_index()
    )

    # High-severity percentage
    high_counts = (
        work[work["severity"] == "High"]
        .groupby(["zone", "violation_type"], observed=True)
        .size()
        .reset_index(name="high_count")
    )
    agg = agg.merge(high_counts, on=["zone", "violation_type"], how="left")
    agg["high_count"] = agg["high_count"].fillna(0)
    agg["pct_high_severity"] = (
        (agg["high_count"] / agg["total_violations"] * 100).round(1)
    )
    agg.drop(columns=["high_count"], inplace=True)

    # Peak hour per zone
    if "hour" in work.columns:
        peak_hour = (
            work.groupby(["zone", "violation_type"], observed=True)["hour"]
               .agg(lambda x: x.value_counts().idxmax())
               .reset_index(name="peak_hour")
        )
        agg = agg.merge(peak_hour, on=["zone", "violation_type"], how="left")

    logger.info(f"  Aggregated {len(df):,} rows → {len(agg)} zone-type groups")
    return agg


def aggregate_by_hour(df: pd.DataFrame) -> pd.DataFrame:
    """Hourly violation counts — useful for trend analysis."""
    if "hour" not in df.columns:
        raise ValueError("aggregate_by_hour requires 'hour' column")
    return (
        df.groupby("hour", observed=True)
          .agg(count=("violation_type", "size"),
               avg_fine=("fine_amount", "mean"),
               avg_speed=("speed_kmh", "mean") if "speed_kmh" in df.columns else ("fine_amount","count"))
          .round(2)
          .reset_index()
    )


# ─────────────────────────────────────────────────────────────────────────────
# VALIDATION
# ─────────────────────────────────────────────────────────────────────────────

def validate_schema(df: pd.DataFrame) -> dict:
    """
    Schema contract enforcement — returns a validation report dict.

    Checks:
      • All required columns present
      • No nulls in non-nullable columns
      • Numeric ranges within bounds
      • Categorical values in allowed sets
      • No exact duplicate rows

    Example:
        result = validate_schema(df)
        if not result['passed']:
            raise ValueError(result['errors'])
    """
    errors   = []
    warnings = []

    REQUIRED_COLS = ["violation_type", "zone", "severity", "fine_amount"]
    NUMERIC_BOUNDS = {
        "speed_kmh":   (0,     200),
        "fine_amount": (0,   5_000),
        "latitude":    (-90,     90),
        "longitude":   (-180,   180),
    }
    CATEGORICAL_SETS = {
        "violation_type": {"Speeding", "Red-Light", "DUI", "Illegal Park",
                           "Reckless", "Mobile Use", "No Seatbelt", "Unknown"},
        "zone":           {"Downtown", "Highway", "School Zone",
                           "Residential", "Industrial", "Unknown"},
        "severity":       {"High", "Medium", "Low"},
    }

    # 1. Required columns
    for col in REQUIRED_COLS:
        if col not in df.columns:
            errors.append(f"Missing required column: '{col}'")

    # 2. Null checks on non-nullable cols
    for col in ["violation_type", "zone", "severity"]:
        if col in df.columns:
            null_count = df[col].isna().sum()
            if null_count > 0:
                warnings.append(f"'{col}' has {null_count} nulls")

    # 3. Numeric range checks
    for col, (lo, hi) in NUMERIC_BOUNDS.items():
        if col in df.columns:
            vals = pd.to_numeric(df[col], errors="coerce")
            oob  = (~vals.between(lo, hi) & vals.notna()).sum()
            if oob:
                errors.append(f"'{col}' has {oob} values outside [{lo}, {hi}]")

    # 4. Categorical set checks
    for col, allowed in CATEGORICAL_SETS.items():
        if col in df.columns:
            actual = set(df[col].dropna().astype(str).unique())
            unknown = actual - allowed
            if unknown:
                warnings.append(f"'{col}' contains unexpected values: {unknown}")

    # 5. Duplicate check
    n_dups = df.duplicated().sum()
    if n_dups:
        warnings.append(f"{n_dups} duplicate rows detected")

    # 6. Fine amount null rate
    if "fine_amount" in df.columns:
        null_rate = df["fine_amount"].isna().mean() * 100
        if null_rate > 5:
            errors.append(f"fine_amount null rate {null_rate:.1f}% exceeds 5% threshold")

    passed = len(errors) == 0
    score  = max(0.0, 100.0 - len(errors) * 10 - len(warnings) * 2)
    report = {
        "passed":   passed,
        "score":    score,
        "errors":   errors,
        "warnings": warnings,
        "rows":     len(df),
        "cols":     len(df.columns),
    }

    if passed:
        logger.info(f"  Schema validation PASSED — score {score:.1f}%")
    else:
        logger.error(f"  Schema validation FAILED — {len(errors)} errors")
        for e in errors:
            logger.error(f"    ✗ {e}")

    return report


# ─────────────────────────────────────────────────────────────────────────────
# FEATURE ENGINEERING
# ─────────────────────────────────────────────────────────────────────────────

def encode_categoricals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ordinal-encode low-cardinality categoricals for ML pipelines.
    Produces integer codes alongside original columns.
    """
    ordinal_map = {
        "severity":    {"Low": 0, "Medium": 1, "High": 2},
        "is_peak":     {False: 0, True: 1},
        "is_weekend":  {False: 0, True: 1},
    }
    for col, mapping in ordinal_map.items():
        if col in df.columns:
            df[f"{col}_code"] = df[col].map(mapping)

    # One-hot encode zone
    if "zone" in df.columns:
        zone_dummies = pd.get_dummies(
            df["zone"].astype(str), prefix="zone", drop_first=False
        )
        df = pd.concat([df, zone_dummies], axis=1)

    return df


def compute_risk_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Composite risk score per violation (0–100) using:
      severity weight (50%) + normalised speed (30%) + fine percentile (20%)
    """
    sev_weight = {"High": 1.0, "Medium": 0.5, "Low": 0.1}
    sev_series = df["severity"].astype(str) if hasattr(df["severity"], "cat") else df["severity"]
    df["_sev_w"] = sev_series.map(sev_weight).fillna(0)

    if "speed_kmh" in df.columns:
        sp_max = df["speed_kmh"].max() or 1
        df["_sp_norm"] = df["speed_kmh"] / sp_max
    else:
        df["_sp_norm"] = 0

    if "fine_amount" in df.columns:
        df["_fine_pct"] = df["fine_amount"].rank(pct=True)
    else:
        df["_fine_pct"] = 0

    df["risk_score"] = (
        df["_sev_w"]   * 50 +
        df["_sp_norm"] * 30 +
        df["_fine_pct"]* 20
    ).round(1)

    df.drop(columns=["_sev_w", "_sp_norm", "_fine_pct"], inplace=True)
    logger.info("  Computed risk_score (0–100) for all records")
    return df
