"""
pipeline/quality.py
───────────────────
Step 28 — Data quality validation checks and anomaly detection system.

Provides:
  • run_dq_checks()     — full DQ suite (nulls, ranges, uniqueness, freshness)
  • detect_anomalies()  — Z-score + IQR-based statistical outlier detection
  • generate_dq_report()— human-readable markdown summary
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# DATA QUALITY CHECKS
# ─────────────────────────────────────────────────────────────────────────────

def run_dq_checks(df: pd.DataFrame) -> dict:
    """
    Execute the full data quality suite.

    Checks:
      1. Completeness  — null rates per column
      2. Validity      — range / set membership
      3. Uniqueness    — duplicate violation IDs
      4. Consistency   — speed vs violation_type alignment
      5. Freshness     — timestamp recency (max 7 days lag)

    Returns:
        dict with keys: score, passed, checks (list of individual results)
    """
    checks  = []
    passed  = 0
    failed  = 0

    # 1 ── Completeness ──────────────────────────────────────────────────────
    COMPLETENESS_THRESHOLDS = {
        "violation_type": 1.00,   # 100% required
        "zone":           1.00,
        "severity":       1.00,
        "fine_amount":    0.95,   # 95% — 5% missing is acceptable
        "timestamp":      0.99,
        "speed_kmh":      0.90,
    }
    for col, threshold in COMPLETENESS_THRESHOLDS.items():
        if col not in df.columns:
            checks.append(_check("completeness", col, "SKIP", f"column absent"))
            continue
        complete_rate = 1 - df[col].isna().mean()
        ok = complete_rate >= threshold
        passed += ok; failed += not ok
        checks.append(_check(
            "completeness", col,
            "PASS" if ok else "FAIL",
            f"{complete_rate*100:.1f}% complete (need ≥{threshold*100:.0f}%)"
        ))

    # 2 ── Range validity ────────────────────────────────────────────────────
    RANGE_CHECKS = {
        "speed_kmh":   (0,     200),
        "fine_amount": (0,   5_000),
        "latitude":    (-90,    90),
        "longitude":   (-180,  180),
        "hour":        (0,      23),
    }
    for col, (lo, hi) in RANGE_CHECKS.items():
        if col not in df.columns:
            continue
        vals = pd.to_numeric(df[col], errors="coerce")
        oob_rate = (~vals.between(lo, hi) & vals.notna()).mean()
        ok = oob_rate < 0.01          # < 1% out-of-bounds acceptable
        passed += ok; failed += not ok
        checks.append(_check(
            "validity", col,
            "PASS" if ok else "FAIL",
            f"{oob_rate*100:.2f}% out-of-bounds (threshold: 1%)"
        ))

    # 3 ── Uniqueness ────────────────────────────────────────────────────────
    if "violation_id" in df.columns:
        dup_rate = df["violation_id"].duplicated().mean()
        ok = dup_rate == 0
        passed += ok; failed += not ok
        checks.append(_check(
            "uniqueness", "violation_id",
            "PASS" if ok else "FAIL",
            f"{dup_rate*100:.3f}% duplicate IDs"
        ))

    # 4 ── Consistency: speed vs violation_type ──────────────────────────────
    if {"speed_kmh", "violation_type"}.issubset(df.columns):
        # Speeding violations should have speed > 0
        speeding = df[df["violation_type"].astype(str) == "Speeding"]
        bad_speed = (speeding["speed_kmh"] <= 0).mean() if len(speeding) else 0
        ok = bad_speed < 0.02
        passed += ok; failed += not ok
        checks.append(_check(
            "consistency", "speed_kmh/violation_type",
            "PASS" if ok else "FAIL",
            f"{bad_speed*100:.1f}% 'Speeding' records with speed ≤ 0"
        ))

    # 5 ── Freshness ─────────────────────────────────────────────────────────
    if "timestamp" in df.columns:
        ts = pd.to_datetime(df["timestamp"], errors="coerce")
        if ts.notna().any():
            max_ts = ts.max()
            lag_days = (pd.Timestamp.now() - max_ts).days
            ok = lag_days <= 7
            passed += ok; failed += not ok
            checks.append(_check(
                "freshness", "timestamp",
                "PASS" if ok else "WARN",
                f"Most recent record is {lag_days} days old (threshold: 7 days)"
            ))

    total  = passed + failed
    score  = round(passed / total * 100, 1) if total else 0.0
    result = {
        "score":   score,
        "passed":  failed == 0,
        "n_pass":  passed,
        "n_fail":  failed,
        "checks":  checks,
        "run_at":  datetime.now().isoformat(),
    }

    icon = "✓" if failed == 0 else "✗"
    logger.info(f"  DQ: {icon} {passed}/{total} checks passed — score {score}%")
    if failed:
        for c in checks:
            if c["status"] == "FAIL":
                logger.warning(f"    FAIL [{c['dimension']}] {c['field']}: {c['detail']}")

    return result


def _check(dimension: str, field: str, status: str, detail: str) -> dict:
    return {"dimension": dimension, "field": field, "status": status, "detail": detail}


# ─────────────────────────────────────────────────────────────────────────────
# ANOMALY DETECTION
# ─────────────────────────────────────────────────────────────────────────────

def detect_anomalies(
    df: pd.DataFrame,
    col: str = "fine_amount",
    method: str = "combined",
    z_threshold: float = 3.0,
) -> pd.DataFrame:
    """
    Statistical anomaly detection on a numeric column.

    Args:
        df:          Input DataFrame.
        col:         Column to analyse.
        method:      'zscore' | 'iqr' | 'combined' (flags if either method flags)
        z_threshold: Z-score cutoff (default 3σ).

    Returns:
        DataFrame of anomalous rows with 'anomaly_reason' column added.

    Example:
        anomalies = detect_anomalies(df, col='fine_amount')
        print(f"Found {len(anomalies)} anomalies")
    """
    if col not in df.columns:
        logger.warning(f"detect_anomalies: column '{col}' not found")
        return pd.DataFrame()

    series = pd.to_numeric(df[col], errors="coerce").dropna()
    work   = df.loc[series.index].copy()
    reasons = pd.Series("", index=work.index)

    # Z-score method
    z_scores = np.abs((series - series.mean()) / series.std())
    zscore_flag = z_scores > z_threshold
    reasons[zscore_flag] += f"Z-score={z_scores[zscore_flag].round(2).astype(str)}; "

    # IQR method
    Q1, Q3 = series.quantile(0.25), series.quantile(0.75)
    IQR = Q3 - Q1
    iqr_flag = (series < Q1 - 1.5 * IQR) | (series > Q3 + 1.5 * IQR)
    reasons[iqr_flag] += "IQR outlier; "

    if method == "zscore":
        flag = zscore_flag
    elif method == "iqr":
        flag = iqr_flag
    else:  # combined: either method flags
        flag = zscore_flag | iqr_flag

    anomalies = work[flag].copy()
    anomalies["anomaly_reason"] = reasons[flag]
    anomalies["anomaly_col"]    = col
    anomalies["anomaly_value"]  = series[flag]

    logger.info(
        f"  Anomaly detection on '{col}': "
        f"{len(anomalies)} / {len(work)} records flagged ({method} method)"
    )
    return anomalies.reset_index(drop=True)


def detect_temporal_spikes(df: pd.DataFrame, window: int = 3) -> pd.DataFrame:
    """
    Detect unusual spikes in hourly violation counts.
    Flags hours where count > mean + 2*std across a rolling window.
    """
    if "hour" not in df.columns:
        return pd.DataFrame()

    hourly = df.groupby("hour").size().rename("count").reset_index()
    rolling_mean = hourly["count"].rolling(window, center=True, min_periods=1).mean()
    rolling_std  = hourly["count"].rolling(window, center=True, min_periods=1).std().fillna(0)
    threshold    = rolling_mean + 2 * rolling_std
    spikes       = hourly[hourly["count"] > threshold].copy()
    spikes["expected_range"] = (rolling_mean - rolling_std).clip(0).round(0).astype(int).astype(str) + \
                               "–" + (rolling_mean + rolling_std).round(0).astype(int).astype(str)

    logger.info(f"  Temporal spike detection: {len(spikes)} hours flagged")
    return spikes


# ─────────────────────────────────────────────────────────────────────────────
# REPORT GENERATION
# ─────────────────────────────────────────────────────────────────────────────

def generate_dq_report(dq_result: dict, anomalies: pd.DataFrame, out_path: str = None) -> str:
    """Generate a markdown-formatted DQ report string."""
    lines = [
        "# Data Quality Report",
        f"**Run at:** {dq_result.get('run_at', 'N/A')}",
        f"**Overall score:** {dq_result['score']}%  |  "
        f"Passed: {dq_result['n_pass']}  |  Failed: {dq_result['n_fail']}",
        "",
        "## Check Results",
        "| Dimension | Field | Status | Detail |",
        "|-----------|-------|--------|--------|",
    ]
    for c in dq_result.get("checks", []):
        icon = "✓" if c["status"] == "PASS" else ("⚠" if c["status"] == "WARN" else "✗")
        lines.append(f"| {c['dimension']} | {c['field']} | {icon} {c['status']} | {c['detail']} |")

    lines += [
        "",
        "## Anomaly Detection",
        f"**Total anomalies flagged:** {len(anomalies)}",
    ]
    if len(anomalies) and "anomaly_reason" in anomalies.columns:
        lines.append("")
        lines.append("| violation_id | fine_amount | reason |")
        lines.append("|---|---|---|")
        for _, row in anomalies.head(10).iterrows():
            vid  = row.get("violation_id", "N/A")
            fine = row.get("fine_amount", "N/A")
            why  = row.get("anomaly_reason", "")
            lines.append(f"| {vid} | {fine} | {why.strip()} |")

    report = "\n".join(lines)
    if out_path:
        Path(out_path).write_text(report)
        logger.info(f"  DQ report written to {out_path}")
    return report
