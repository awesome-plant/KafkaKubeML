# stream_processor/config.py
import os
from dataclasses import dataclass
from typing import List, Optional

def _split_csv(val: Optional[str]) -> List[str]:
    if not val:
        return []
    return [p.strip() for p in val.split(",") if p.strip()]

@dataclass
class Config:
    # Kafka
    kafka_brokers: str
    topic: str
    group_id: str
    auto_offset_reset: str
    session_timeout_ms: int
    max_poll_interval_ms: int

    # Output layout
    dataset_name: str
    parquet_dirs: List[str]            # one or more roots (mirrored)
    tmp_dir: str
    parquet_compression: str           # e.g., snappy|zstd|gzip
    file_naming: str                   # "spark" | "simple"

    # File sizing
    target_file_size_mib: int          # e.g., 192
    max_files_per_partition: int       # e.g., 4
    max_rows_per_file: int             # 0 = disabled
    compression_factor: float          # heuristic, e.g., 0.45

    # Batching/flush
    batch_size: int
    flush_interval_seconds: int
    event_ts_field: str
    run_id: Optional[str]              # optional prefix in filename

    # Mirroring & DLQ
    mirror_strategy: str               # "auto" | "hardlink" | "copy"
    dlq_brokers: Optional[str]
    dlq_topic: Optional[str]

def load_config() -> Config:
    # Back-compat: allow single PARQUET_DIR if PARQUET_DIRS not set
    dirs = _split_csv(os.getenv("PARQUET_DIRS"))
    if not dirs:
        single = os.getenv("PARQUET_DIR", "/data/parquet")
        dirs = [single]

    return Config(
        # Kafka
        kafka_brokers=os.getenv("KAFKA_BROKERS", "kafka-cluster-kafka-bootstrap.kafka-stream.svc:9092"),
        topic=os.getenv("KAFKA_TOPIC", "user-interactions"),
        group_id=os.getenv("GROUP_ID", "parquet-consumer-group"),
        auto_offset_reset=os.getenv("AUTO_OFFSET_RESET", "earliest"),
        session_timeout_ms=int(os.getenv("SESSION_TIMEOUT_MS", "45000")),
        max_poll_interval_ms=int(os.getenv("MAX_POLL_INTERVAL_MS", "600000")),

        # Output layout
        dataset_name=os.getenv("DATASET_NAME", "user_interactions"),
        parquet_dirs=dirs,
        tmp_dir=os.getenv("TMP_DIR", "/data/tmp"),
        parquet_compression=os.getenv("PARQUET_COMPRESSION", "snappy"),
        file_naming=os.getenv("FILE_NAMING", "spark"),  # spark|simple

        # File sizing
        target_file_size_mib=int(os.getenv("TARGET_FILE_SIZE_MIB", "192")),
        max_files_per_partition=int(os.getenv("MAX_FILES_PER_PARTITION", "4")),
        max_rows_per_file=int(os.getenv("MAX_ROWS_PER_FILE", "0")),
        compression_factor=float(os.getenv("COMPRESSION_FACTOR", "0.45")),

        # Batching/flush
        batch_size=int(os.getenv("BATCH_SIZE", "1000")),
        flush_interval_seconds=int(os.getenv("FLUSH_INTERVAL_SECONDS", "30")),
        event_ts_field=os.getenv("EVENT_TS_FIELD", "event_ts"),
        run_id=os.getenv("RUN_ID"),

        # Mirroring & DLQ
        mirror_strategy=os.getenv("MIRROR_STRATEGY", "auto"),
        dlq_brokers=os.getenv("DLQ_BROKERS"),
        dlq_topic=os.getenv("DLQ_TOPIC"),
    )
