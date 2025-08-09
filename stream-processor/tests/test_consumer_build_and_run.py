# tests/unit/test_consumer_build_and_run.py
import json
import types
import uuid
import time
import pytest

import stream_processor.app as app

# ---------- fakes ----------

class FakeMsg:
    def __init__(self, topic, partition, offset, payload):
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._payload = payload
    def error(self): return None
    def topic(self): return self._topic
    def partition(self): return self._partition
    def offset(self): return self._offset
    def value(self): return self._payload

class FakeTopicPartition:
    def __init__(self, topic, partition, offset=None):
        self.topic = topic
        self.partition = partition
        self.offset = offset
    def __hash__(self): return hash((self.topic, self.partition))
    def __eq__(self, other): return isinstance(other, FakeTopicPartition) and (self.topic, self.partition)==(other.topic, other.partition)

class FakeConsumer:
    def __init__(self, conf):
        self.conf = conf
        self._subscribed = []
        self._queue = []
        self._committed = []
    def subscribe(self, topics): self._subscribed = list(topics)
    def push(self, msg): self._queue.append(msg)
    def poll(self, timeout=1.0):
        if self._queue:
            return self._queue.pop(0)
        time.sleep(0.01)
        return None
    def commit(self, offsets=None, asynchronous=False): self._committed.append(offsets or [])
    def close(self): pass

class FakeWriter:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.calls = []
    def write_partitioned(self, batch, ts_field="_event_ts_utc"):
        self.calls.append((len(batch), ts_field))
        # stop the loop after first successful write
        app._stop = True
        return ["/dev/null/part-00000.parquet"]

# ---------- helpers ----------

def _make_event():
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": str(uuid.uuid4()),
        "event_type": "click",
        "event_ts": int(time.time()),
        "url": "https://example.test",
        "product_id": str(uuid.uuid4()),
        "user_agent": "pytest",
        "schema_version": 1,
    }

# ---------- tests ----------

def test_build_consumer_uses_expected_config(monkeypatch):
    created = {}
    def fake_consumer_ctor(conf):
        created["conf"] = conf
        return FakeConsumer(conf)
    monkeypatch.setattr(app, "Consumer", fake_consumer_ctor)

    # minimal cfg
    monkeypatch.setenv("KAFKA_BROKERS", "broker:9092")
    monkeypatch.setenv("KAFKA_TOPIC", "user-interactions")
    monkeypatch.setenv("GROUP_ID", "parquet-consumer-group")

    cfg = app.load_config()
    c = app.build_consumer(cfg)
    assert isinstance(c, FakeConsumer)
    assert created["conf"]["bootstrap.servers"] == "broker:9092"
    assert created["conf"]["group.id"] == "parquet-consumer-group"
    assert created["conf"]["enable.auto.commit"] is False

def test_run_pulls_messages_and_writes(monkeypatch, tmp_path):
    # env config
    monkeypatch.setenv("KAFKA_BROKERS", "broker:9092")
    monkeypatch.setenv("KAFKA_TOPIC", "user-interactions")
    monkeypatch.setenv("GROUP_ID", "parquet-consumer-group")
    monkeypatch.setenv("PARQUET_DIRS", str(tmp_path / "bronze"))
    monkeypatch.setenv("TMP_DIR", str(tmp_path / "tmp"))
    monkeypatch.setenv("PARTITION_GRANULARITY", "minute")
    monkeypatch.setenv("BATCH_SIZE", "2")
    monkeypatch.setenv("FLUSH_INTERVAL_SECONDS", "3600")  # force size-based flush

    # fake Kafka classes
    fc = FakeConsumer({"bootstrap.servers":"broker:9092"})
    # two messages → triggers flush
    evt1 = _make_event()
    evt2 = _make_event()
    fc.push(FakeMsg("user-interactions", 0, 0, json.dumps(evt1).encode()))
    fc.push(FakeMsg("user-interactions", 0, 1, json.dumps(evt2).encode()))
    def fake_consumer_ctor(conf): return fc
    monkeypatch.setattr(app, "Consumer", fake_consumer_ctor)
    monkeypatch.setattr(app, "TopicPartition", FakeTopicPartition)

    # fake writer
    fw = FakeWriter()
    def fake_writer_ctor(**kwargs):
        fw.kwargs = kwargs
        return fw
    monkeypatch.setattr(app, "ParquetPartitionWriter", fake_writer_ctor)

    # avoid real signals
    monkeypatch.setattr(app.signal, "signal", lambda *args, **kwargs: None)

    # reset stop flag
    app._stop = False

    # run
    app.run()

    # assertions: writer was called once with 2 records
    assert fw.calls and fw.calls[0][0] == 2
    # consumer subscribed to topic and committed offsets
    assert fc._subscribed == ["user-interactions"]
    assert fc._committed, "expected at least one commit after write"
