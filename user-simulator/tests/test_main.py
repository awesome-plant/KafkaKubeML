# tests/test_main.py
from user_simulator import main


def test_run_simulation_runs(monkeypatch):
    calls = {"sent": 0}
    class DummyProducer:
        def close(self):
            calls["closed"] = True
    monkeypatch.setattr(main, "create_kafka_producer", lambda brokers: DummyProducer())
    monkeypatch.setattr(main, "generate_event", lambda: {"fake": "event"})
    monkeypatch.setattr(main, "send_event", lambda producer, topic, event: calls.update({"sent": calls["sent"] + 1}))
    monkeypatch.setattr(main, "time", __import__("time"))

    main.run_simulation("dummy_broker", "dummy_topic", num_events=3, interval=0)
    assert calls["sent"] == 3
    assert calls.get("closed", False)
