# tests/integration/conftest.py
import os, uuid, time, pytest
ck = pytest.importorskip("confluent_kafka", reason="confluent-kafka not installed")
from confluent_kafka.admin import AdminClient, NewTopic

BROKERS = os.environ.get("KAFKA_BROKERS") or os.environ.get("KAFKA_BROKER") or "kafka-cluster-kafka-bootstrap.kafka-stream.svc:9092"

@pytest.fixture(scope="session")
def brokers():
    return BROKERS

@pytest.fixture(scope="session")
def admin(brokers):
    return AdminClient({"bootstrap.servers": brokers})

@pytest.fixture(scope="session")
def test_topic(admin):
    topic = f"it-user-events-{uuid.uuid4().hex[:8]}"
    fs = admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    try:
        fs[topic].result(10)
    except Exception:
        # already exists or race; fine for tests
        pass

    # Wait until metadata shows the topic
    deadline = time.time() + 30
    while time.time() < deadline:
        md = admin.list_topics(timeout=5)
        if topic in md.topics and not md.topics[topic].error:
            break
        time.sleep(1)
    else:
        pytest.skip(f"Kafka topic {topic} not available")

    return topic
