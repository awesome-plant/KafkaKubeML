# tests/test_kafka_client.py
"""
Unit tests for user_simulator.kafka_client

Covers:
- Kafka producer creation
- Sending event (using a dummy producer to avoid real Kafka)
- Serialization and flush logic
"""

from user_simulator import kafka_client

class DummyProducer:
    """
    Dummy Kafka producer for testing send_event.
    Records send/flush calls.
    """
    def __init__(self):
        self.sent = []
        self.flushed = False

    def send(self, topic, value):
        self.sent.append((topic, value))

    def flush(self):
        self.flushed = True

    def close(self):
        pass  # Not needed for send_event, but may be used in other tests

def test_create_kafka_producer_returns_producer(monkeypatch):
    """
    create_kafka_producer should return an instance of KafkaProducer.
    We monkeypatch KafkaProducer to our dummy to avoid network calls.
    """
    class FakeKafkaProducer:
        def __init__(*a, **k): pass
    monkeypatch.setattr(kafka_client, "KafkaProducer", FakeKafkaProducer)
    producer = kafka_client.create_kafka_producer("localhost:9092")
    assert isinstance(producer, FakeKafkaProducer)

def test_send_event_calls_send_and_flush():
    """
    send_event should call send() and flush() on the producer.
    """
    producer = DummyProducer()
    topic = "test-topic"
    event = {"foo": "bar"}
    kafka_client.send_event(producer, topic, event)
    # Should record the send call with correct data
    assert producer.sent == [(topic, event)], "send_event did not call send() correctly"
    # Should have called flush
    assert producer.flushed, "send_event did not call flush()"

def test_send_event_serializes_to_json(monkeypatch):
    """
    Test that create_kafka_producer sets up a producer that serializes to JSON.
    We'll simulate this by using the value_serializer and checking output.
    """
    # Patch KafkaProducer to capture value_serializer
    captured = {}
    class FakeKafkaProducer:
        def __init__(self, **kwargs):
            captured['serializer'] = kwargs.get("value_serializer")
    monkeypatch.setattr(kafka_client, "KafkaProducer", FakeKafkaProducer)
    kafka_client.create_kafka_producer("dummy:123")
    serializer = captured["serializer"]
    # It should convert dict to bytes (via json)
    sample = {"hello": "world"}
    out = serializer(sample)
    assert isinstance(out, bytes)
    import json
    assert json.loads(out.decode("utf-8")) == sample

