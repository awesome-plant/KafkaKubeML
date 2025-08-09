# tests/integration/test_kafka_end_to_end.py
import json, time, uuid, pytest
ck = pytest.importorskip("confluent_kafka", reason="confluent-kafka not installed")
from confluent_kafka import Producer, Consumer

from user_simulator.generator import make_event, key_for_event
from user_simulator.producer import build_producer

pytestmark = pytest.mark.integration

def _consume_one(brokers, topic, target_event_id, timeout_s=20):
    c = Consumer({
        "bootstrap.servers": brokers,
        "group.id": f"it-consumer-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    c.subscribe([topic])
    end = time.time() + timeout_s
    found = None
    try:
        while time.time() < end:
            msg = c.poll(1.0)
            if msg is None or msg.error():
                continue
            try:
                evt = json.loads(msg.value().decode("utf-8"))
                if evt.get("event_id") == target_event_id:
                    found = evt
                    break
            except Exception:
                continue
    finally:
        c.close()
    return found

def test_produce_and_consume_roundtrip(brokers, test_topic):
    # Build producer (librdkafka) and send one event
    p: Producer = build_producer(
        brokers=brokers, linger_ms=10, batch_num_messages=1000, compression="snappy"
    )
    evt = make_event()
    key = key_for_event(evt)
    payload = json.dumps(evt)
    # send + flush
    p.produce(test_topic, key=key, value=payload)
    p.flush(10)

    got = _consume_one(brokers, test_topic, evt["event_id"], timeout_s=30)
    assert got is not None, "Did not consume the produced event"
    # basic shape checks
    assert got["event_id"] == evt["event_id"]
    assert "user_id" in got and "event_ts" in got and "event_type" in got
