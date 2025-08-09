# tests/test_smoke_config.py
from user_simulator.config import load

def test_smoke_load_config(monkeypatch):
    monkeypatch.setenv("KAFKA_BROKERS", "broker:9092")
    monkeypatch.setenv("KAFKA_TOPIC", "t")
    cfg = load()
    assert cfg.brokers == "broker:9092"
    assert cfg.topic == "t"
    assert cfg.rate > 0
