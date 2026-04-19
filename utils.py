

import logging
import os
import sys
from pathlib import Path
from datetime import datetime

# ─────────────────────────────────────────────────────────────────────────────
# DIRECTORY & ENVIRONMENT SETUP
# ─────────────────────────────────────────────────────────────────────────────

DIRS = [
    "data/raw",
    "data/processed",
    "data/archive",
    "data/warehouse",
    "data/logs",
    "data/reports",
]

def setup_dirs():
    """Create the pipeline directory structure (Step 1 — Linux)."""
    for d in DIRS:
        Path(d).mkdir(parents=True, exist_ok=True)
    print("[setup] Directory structure ready")


def setup_logging(log_dir: str = "data/logs", level: int = logging.INFO) -> logging.Logger:
    """Configure root logger to write to console + rotating file (Step 1)."""
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    log_file = Path(log_dir) / f"pipeline_{datetime.now():%Y%m%d}.log"

    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, encoding="utf-8"),
    ]
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(level=level, format=fmt, handlers=handlers)
    return logging.getLogger("traffic_pipeline")


def log_run_summary(logger, raw_df, clean_df, dq_result, anomalies, elapsed):
    logger.info("=" * 60)
    logger.info("PIPELINE RUN SUMMARY")
    logger.info(f"  Raw records loaded :  {len(raw_df):>10,}")
    logger.info(f"  Clean records out  :  {len(clean_df):>10,}")
    logger.info(f"  Drop rate          :  {(1-len(clean_df)/max(len(raw_df),1))*100:>9.1f}%")
    logger.info(f"  DQ score           :  {dq_result['score']:>9.1f}%")
    logger.info(f"  Anomalies flagged  :  {len(anomalies):>10,}")
    logger.info(f"  Elapsed            :  {elapsed:>9.1f}s")
    logger.info("=" * 60)
