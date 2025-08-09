# stream_processor/writer.py
import os
import uuid
import math
import shutil
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone

import pandas as pd

def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def _utc_parts(ts: datetime):
    ts = ts.astimezone(timezone.utc)
    return ts.year, ts.month, ts.day, ts.hour

def _fmt_ts_hour(ts: datetime) -> str:
    return ts.strftime("%Y%m%dT%H%M%SZ")

def _filename(style: str, ts_hour: datetime, uid: str, compression: str, run_id: Optional[str], seq: int) -> str:
    if style == "spark":
        # Databricks-like (sequence is informative; c000 constant kept)
        name = f"part-{seq:05d}-{uid}.c000.{compression}.parquet"
    else:
        # human-friendly timestamped alternative
        name = f"part-{_fmt_ts_hour(ts_hour)}-{uid}.{compression}.parquet"
    return f"{run_id}-{name}" if run_id else name

def _link_or_copy(src: str, dst: str, strategy: str):
    # Create parent dir, then try hardlink (atomic), else copy to tmp and replace
    _ensure_dir(os.path.dirname(dst))
    if strategy in ("auto", "hardlink"):
        try:
            os.link(src, dst)
            return
        except OSError:
            # fall through to copy
            pass
    tmp = f"{dst}.tmp"
    shutil.copy2(src, tmp)
    os.replace(tmp, dst)

class ParquetPartitionWriter:
    """
    Writes records into Hive-style hourly partitions under a dataset folder:
      <root>/<dataset_name>/yyyy=YYYY/mm=MM/dd=DD/hour=HH/part-*.parquet
    Single tmp write, then mirror into each root via hardlink/copy (atomic where possible).
    Chunks batches to target ~N MiB per file with max files per partition.
    """
    def __init__(
        self,
        root_dirs: List[str],
        dataset_name: str,
        tmp_dir: str,
        compression: str = "snappy",
        naming: str = "spark",              # spark|simple
        mirror_strategy: str = "auto",      # auto|hardlink|copy
        run_id: Optional[str] = None,
        target_file_size_mib: int = 192,
        max_files_per_partition: int = 4,
        max_rows_per_file: int = 0,         # 0 disables row cap
        compression_factor: float = 0.45,   # mem_bytes * factor ≈ compressed bytes
    ):
        assert root_dirs, "At least one root dir required"
        self.root_dirs = [d.rstrip("/") for d in root_dirs]
        self.dataset_name = dataset_name.strip("/")

        self.tmp_dir = tmp_dir.rstrip("/")
        self.compression = compression
        self.naming = naming
        self.mirror_strategy = mirror_strategy
        self.run_id = run_id

        self.target_bytes = max(1, int(target_file_size_mib * (1024**2)))
        self.max_files_per_partition = max(1, int(max_files_per_partition))
        self.max_rows_per_file = max(0, int(max_rows_per_file))
        self.compression_factor = max(0.05, float(compression_factor))

        _ensure_dir(self.tmp_dir)
        for d in self.root_dirs:
            _ensure_dir(d)

    def _dest_dir(self, root: str, ts_hour: datetime) -> str:
        y, m, d, h = _utc_parts(ts_hour)
        return os.path.join(
            root,
            self.dataset_name,
            f"yyyy={y:04d}",
            f"mm={m:02d}",
            f"dd={d:02d}",
            f"hour={h:02d}",
        )

    def _decide_chunks(self, df: pd.DataFrame) -> int:
        rows = len(df)
        if rows <= 0:
            return 0
        mem_bytes = int(df.memory_usage(deep=True).sum())
        compressed_est = max(1, int(mem_bytes * self.compression_factor))
        est_files = math.ceil(compressed_est / self.target_bytes)
        n = max(1, est_files)
        n = min(n, self.max_files_per_partition)
        return n

    def write_partitioned(self, records: List[Dict[str, Any]], ts_field: str = "_event_ts_utc") -> List[str]:
        if not records:
            return []
        df = pd.DataFrame(records)
        # Normalize timestamp column
        df["_ts"] = pd.to_datetime(df[ts_field], utc=True, errors="coerce")
        df = df.dropna(subset=["_ts"])

        # Build hourly partition hints
        df["yyyy"] = df["_ts"].dt.year.astype("int32")
        df["mm"]   = df["_ts"].dt.month.astype("int16")
        df["dd"]   = df["_ts"].dt.day.astype("int16")
        df["hour"] = df["_ts"].dt.hour.astype("int16")

        written: List[str] = []

        for (y, m, d, h), sub in df.groupby(["yyyy", "mm", "dd", "hour"], sort=True):
            ts_hour = datetime(int(y), int(m), int(d), int(h), tzinfo=timezone.utc)

            # Decide how many files to make for this hour
            n_files = self._decide_chunks(sub)
            if n_files <= 1:
                chunks = [sub]
            else:
                rows_per_chunk = max(1, math.ceil(len(sub) / n_files))
                if self.max_rows_per_file > 0:
                    rows_per_chunk = min(rows_per_chunk, self.max_rows_per_file)
                # slice into chunks
                chunks = [sub.iloc[i:i+rows_per_chunk] for i in range(0, len(sub), rows_per_chunk)]
                # limit to max_files_per_partition defensively
                chunks = chunks[: self.max_files_per_partition]

            # Write each chunk to TMP once, then mirror to all roots
            for seq, chunk in enumerate(chunks):
                uid = uuid.uuid4().hex[:12]
                fname = _filename(self.naming, ts_hour, uid, self.compression, self.run_id, seq)
                tmp_path = os.path.join(self.tmp_dir, f"{fname}.tmp")

                out = chunk.drop(columns=["_ts", "yyyy", "mm", "dd", "hour"], errors="ignore")
                out.to_parquet(tmp_path, engine="pyarrow", compression=self.compression, index=False)

                # Mirror into each root
                for root in self.root_dirs:
                    dest_dir = self._dest_dir(root, ts_hour)
                    final_path = os.path.join(dest_dir, fname)
                    _link_or_copy(tmp_path, final_path, self.mirror_strategy)
                    written.append(final_path)

                # Remove source tmp
                try:
                    os.remove(tmp_path)
                except FileNotFoundError:
                    pass

        return written
