# src/user_simulator/config.py
import os
from dataclasses import dataclass

@dataclass
class Config:
    brokers: str
    topic: str
    rate: int
    linger_ms: int
    batch_num_messages: int
    compression: str
    key_prefix: str
    log_level: str
    metrics_port: int
    seed: int | None
    create_topic: bool
    topic_partitions: int
    topic_replication: int

def env_bool(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in {"1","true","yes","y"}

def load() -> Config:
    return Config(
        brokers=os.environ.get("KAFKA_BROKERS", "kafka-cluster-kafka-bootstrap.kafka-stream.svc:9092"),
        topic=os.environ.get("KAFKA_TOPIC", "user-interactions"),
        rate=int(os.environ.get("RATE", "100")),  # events/sec per pod
        linger_ms=int(os.environ.get("LINGER_MS", "20")),
        batch_num_messages=int(os.environ.get("BATCH_NUM_MESSAGES", "10000")),
        compression=os.environ.get("COMPRESSION", "snappy"),  # snappy|zstd|gzip
        key_prefix=os.environ.get("KEY_PREFIX", "user-"),
        log_level=os.environ.get("LOG_LEVEL", "INFO"),
        metrics_port=int(os.environ.get("METRICS_PORT", "8000")),
        seed=int(os.environ["SEED"]) if os.environ.get("SEED") else None,
        create_topic=env_bool("CREATE_TOPIC", "false"),
        topic_partitions=int(os.environ.get("TOPIC_PARTITIONS", "12")),
        topic_replication=int(os.environ.get("TOPIC_REPLICATION", "1")),
    )
