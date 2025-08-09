# src/user_simulator/producer.py
from __future__ import annotations
from typing import Callable, Optional
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

def build_producer(
    brokers: str,
    linger_ms: int,
    batch_num_messages: int,
    compression: str,
) -> Producer:
    conf = {
        "bootstrap.servers": brokers,
        "enable.idempotence": True,
        "acks": "all",
        "retries": 2147483647,               # effectively unlimited
        "compression.type": compression,
        "linger.ms": linger_ms,
        "batch.num.messages": batch_num_messages,
        "socket.keepalive.enable": True,
        "message.timeout.ms": 60000,
    }
    return Producer(conf)

def ensure_topic(
    brokers: str,
    topic: str,
    partitions: int = 12,
    replication: int = 1,
    timeout: float = 10.0,
) -> None:
    """
    Best effort topic creation (use Strimzi KafkaTopic in prod).
    Safe to call at startup; no-op if exists.
    """
    admin = AdminClient({"bootstrap.servers": brokers})
    md = admin.list_topics(timeout=timeout)
    if topic in md.topics and not md.topics[topic].error:
        return
    fs = admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=replication)])
    f = fs[topic]
    try:
        f.result()
    except Exception:
        # if already exists or race, ignore
        pass
