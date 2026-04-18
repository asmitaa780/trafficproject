"""
pipeline/hadoop_hdfs.py
───────────────────────
Step 11 — Set up HDFS locally, upload structured/unstructured data
Step 12 — Explain Namenode/Datanode architecture, simulate data storage

Since a real HDFS cluster requires Java + Hadoop daemons, this module:
  1. Simulates HDFS directory structure locally under data/hdfs/
  2. Implements the same API you'd use with hdfs3 / snakebite / WebHDFS
  3. Logs block-distribution metadata to simulate Namenode tracking
  4. Can switch to REAL WebHDFS by setting USE_REAL_HDFS=True + HDFS_HOST env var
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
USE_REAL_HDFS   = os.getenv("USE_REAL_HDFS", "false").lower() == "true"
HDFS_HOST       = os.getenv("HDFS_HOST", "localhost")
HDFS_PORT       = int(os.getenv("HDFS_PORT", "9870"))       # WebHDFS REST port
HDFS_USER       = os.getenv("HDFS_USER", "hadoop")
HDFS_SIM_ROOT   = Path("data/hdfs")                          # local simulation root
BLOCK_SIZE_BYTES = 128 * 1024 * 1024                         # 128 MB — HDFS default
REPLICATION      = 3                                          # default replication factor


# ─────────────────────────────────────────────────────────────────────────────
# NAMENODE METADATA TRACKER
# ─────────────────────────────────────────────────────────────────────────────

class NamenodeSimulator:
    """
    Step 12 — Simulates the Namenode's role:
      • Maintains the file-system namespace (path → inode mapping)
      • Tracks which DataNodes hold each block
      • Enforces replication factor metadata

    Real Namenode stores this in fsimage + edit logs (not user data).
    DataNodes store the actual blocks and send heartbeats every 3 seconds.
    """

    METADATA_FILE = HDFS_SIM_ROOT / "namenode" / "fsimage.json"

    def __init__(self):
        self.METADATA_FILE.parent.mkdir(parents=True, exist_ok=True)
        self._load()

    def _load(self):
        if self.METADATA_FILE.exists():
            with open(self.METADATA_FILE) as f:
                self.fs = json.load(f)
        else:
            self.fs = {"inodes": {}, "datanodes": ["dn0", "dn1", "dn2"]}

    def _save(self):
        with open(self.METADATA_FILE, "w") as f:
            json.dump(self.fs, f, indent=2, default=str)

    def register_file(self, hdfs_path: str, local_path: Path, replication: int = REPLICATION):
        """Record file metadata in the simulated Namenode fsimage."""
        size = local_path.stat().st_size if local_path.exists() else 0
        n_blocks = max(1, (size + BLOCK_SIZE_BYTES - 1) // BLOCK_SIZE_BYTES)
        blocks = []
        for i in range(n_blocks):
            block_id = hashlib.md5(f"{hdfs_path}:{i}".encode()).hexdigest()[:12]
            # Distribute blocks across simulated DataNodes (round-robin)
            assigned_dns = [self.fs["datanodes"][j % len(self.fs["datanodes"])]
                            for j in range(i, i + replication)]
            blocks.append({"block_id": block_id, "datanodes": assigned_dns, "size_bytes":
                           min(BLOCK_SIZE_BYTES, size - i * BLOCK_SIZE_BYTES)})

        self.fs["inodes"][hdfs_path] = {
            "path":        hdfs_path,
            "size_bytes":  size,
            "replication": replication,
            "blocks":      blocks,
            "owner":       HDFS_USER,
            "created_at":  datetime.now().isoformat(),
            "block_size":  BLOCK_SIZE_BYTES,
        }
        self._save()
        logger.debug(f"  Namenode: registered {hdfs_path} → {n_blocks} block(s)")
        return self.fs["inodes"][hdfs_path]

    def get_file_info(self, hdfs_path: str) -> Optional[dict]:
        return self.fs["inodes"].get(hdfs_path)

    def list_dir(self, hdfs_dir: str) -> list:
        return [k for k in self.fs["inodes"] if k.startswith(hdfs_dir)]

    def delete(self, hdfs_path: str):
        self.fs["inodes"].pop(hdfs_path, None)
        self._save()

    def report(self) -> str:
        total_files = len(self.fs["inodes"])
        total_bytes = sum(v["size_bytes"] for v in self.fs["inodes"].values())
        lines = [
            "═══════════════════════════════════════",
            "  HDFS NAMENODE REPORT",
            "═══════════════════════════════════════",
            f"  Total files  : {total_files}",
            f"  Total data   : {total_bytes/1_048_576:.1f} MB",
            f"  DataNodes    : {', '.join(self.fs['datanodes'])}",
            f"  Replication  : {REPLICATION}x",
            "───────────────────────────────────────",
        ]
        for path, meta in list(self.fs["inodes"].items())[:10]:
            lines.append(f"  {path}")
            lines.append(f"    {meta['size_bytes']/1024:.1f} KB | "
                         f"{len(meta['blocks'])} block(s) | "
                         f"rep={meta['replication']}")
        lines.append("═══════════════════════════════════════")
        return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# HDFS CLIENT (local simulation + optional real WebHDFS)
# ─────────────────────────────────────────────────────────────────────────────

class HDFSClient:
    """
    Unified HDFS client.
    • USE_REAL_HDFS=False  → mirrors files into data/hdfs/ (local sim)
    • USE_REAL_HDFS=True   → uses hdfs3 library against a real cluster
    """

    def __init__(self):
        self.namenode = NamenodeSimulator()
        if USE_REAL_HDFS:
            try:
                import hdfs3
                self._client = hdfs3.HDFileSystem(host=HDFS_HOST, port=HDFS_PORT)
                logger.info(f"Connected to real HDFS at {HDFS_HOST}:{HDFS_PORT}")
            except ImportError:
                logger.warning("hdfs3 not installed — falling back to local simulation")
                self._client = None
        else:
            self._client = None

    # ── Core operations ──────────────────────────────────────────────────────

    def mkdir(self, hdfs_path: str):
        """Create directory (simulation: create local mirror)."""
        local = HDFS_SIM_ROOT / hdfs_path.lstrip("/")
        local.mkdir(parents=True, exist_ok=True)
        if self._client:
            self._client.mkdir(hdfs_path)
        logger.info(f"  HDFS mkdir: {hdfs_path}")

    def put(self, local_path: str, hdfs_path: str,
            replication: int = REPLICATION, overwrite: bool = True) -> dict:
        """
        Upload file to HDFS.
        Simulation: copies to data/hdfs/ mirror + registers in Namenode.
        Real:       uses WebHDFS PUT /webhdfs/v1/<path>?op=CREATE
        """
        local_path = Path(local_path)
        if not local_path.exists():
            raise FileNotFoundError(f"Source not found: {local_path}")

        if self._client:
            self._client.put(str(local_path), hdfs_path, replication=replication)
        else:
            dest = HDFS_SIM_ROOT / hdfs_path.lstrip("/")
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(local_path, dest)

        meta = self.namenode.register_file(hdfs_path, local_path, replication)
        size_mb = local_path.stat().st_size / 1_048_576
        logger.info(f"  HDFS PUT: {local_path.name} → {hdfs_path} "
                    f"({size_mb:.2f} MB, {len(meta['blocks'])} block(s), rep={replication})")
        return meta

    def get(self, hdfs_path: str, local_dest: str) -> Path:
        """Download file from HDFS."""
        local_dest = Path(local_dest)
        local_dest.parent.mkdir(parents=True, exist_ok=True)
        if self._client:
            self._client.get(hdfs_path, str(local_dest))
        else:
            src = HDFS_SIM_ROOT / hdfs_path.lstrip("/")
            if not src.exists():
                raise FileNotFoundError(f"HDFS path not found: {hdfs_path}")
            shutil.copy2(src, local_dest)
        logger.info(f"  HDFS GET: {hdfs_path} → {local_dest}")
        return local_dest

    def ls(self, hdfs_dir: str) -> list:
        """List directory contents."""
        if self._client:
            return self._client.ls(hdfs_dir)
        return self.namenode.list_dir(hdfs_dir)

    def exists(self, hdfs_path: str) -> bool:
        if self._client:
            return self._client.exists(hdfs_path)
        return (HDFS_SIM_ROOT / hdfs_path.lstrip("/")).exists()

    def delete(self, hdfs_path: str, recursive: bool = False):
        if self._client:
            self._client.rm(hdfs_path, recursive=recursive)
        else:
            local = HDFS_SIM_ROOT / hdfs_path.lstrip("/")
            if local.is_dir() and recursive:
                shutil.rmtree(local)
            elif local.exists():
                local.unlink()
        self.namenode.delete(hdfs_path)
        logger.info(f"  HDFS DELETE: {hdfs_path}")

    def report(self) -> str:
        return self.namenode.report()


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE INTEGRATION
# ─────────────────────────────────────────────────────────────────────────────

def setup_hdfs_pipeline(client: HDFSClient = None) -> HDFSClient:
    """
    Create the standard HDFS directory structure for the pipeline.

    /traffic/
      raw/        ← original CSV ingestion
      processed/  ← cleaned Parquet files
      warehouse/  ← star schema / fact tables
      streaming/  ← Kafka → Spark Structured Streaming output
      archive/    ← historical snapshots
    """
    if client is None:
        client = HDFSClient()

    dirs = [
        "/traffic/raw",
        "/traffic/processed",
        "/traffic/warehouse/fact_violation",
        "/traffic/warehouse/dim_time",
        "/traffic/warehouse/dim_location",
        "/traffic/warehouse/dim_vehicle",
        "/traffic/streaming",
        "/traffic/archive",
        "/traffic/checkpoints",
    ]
    for d in dirs:
        client.mkdir(d)
    logger.info("  HDFS directory structure initialised")
    return client


def upload_pipeline_outputs(client: HDFSClient = None) -> dict:
    """
    Upload all processed pipeline outputs to HDFS.
    Called after ETL completes (Steps 10 → 11).
    """
    if client is None:
        client = HDFSClient()

    uploads = {}
    file_map = {
        "data/processed/violations.csv":       "/traffic/processed/violations.csv",
        "data/processed/zone_aggregates.csv":  "/traffic/processed/zone_aggregates.csv",
        "data/warehouse/violations.db":        "/traffic/warehouse/violations.db",
    }

    for local, hdfs in file_map.items():
        if Path(local).exists():
            meta = client.put(local, hdfs, replication=REPLICATION)
            uploads[hdfs] = meta
        else:
            logger.warning(f"  Skip (not found): {local}")

    # Upload unstructured data example (Step 11 — unstructured data)
    log_dir = Path("data/logs")
    for log_file in log_dir.glob("*.log"):
        hdfs_path = f"/traffic/raw/logs/{log_file.name}"
        meta = client.put(str(log_file), hdfs_path, replication=2)
        uploads[hdfs_path] = meta

    logger.info(client.report())
    return uploads
