# tests/integration/test_kafka_end_to_end.py

import pytest
import time
import uuid
import json
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
from kafka.admin import KafkaAdminClient, NewTopic
from user_simulator.kafka_client import create_kafka_producer, send_event, wait_for_topic, ensure_topic_exists

@pytest.fixture(scope="module")
brokers = os.environ.get("KAFKA_BROKER")
topic = os.environ.get("KAFKA_TOPIC")
# brokers = "kafka-cluster-kafka-bootstrap.kafka-stream:9092"

@pytest.fixture(scope="module")
def test_topic():
    """Create a unique topic name for testing."""
    return f"test-user-events-{uuid.uuid4()}"

@pytest.fixture(scope="module")
def sample_event():
    """Create a reproducible sample event."""
    return {
        "user_id": str(uuid.uuid4()),
        "event_type": "integration_test",
        "timestamp": int(time.time()),
        "url": "https://example.com/test",
        "product_id": str(uuid.uuid4()),
        "user_agent": "pytest"
    }

@pytest.fixture(scope="module")
def test_topic():
    import uuid
    topic = f"test-user-events-{uuid.uuid4()}"
    admin_client = KafkaAdminClient(bootstrap_servers=brokers)
    topic_obj = NewTopic(name=topic, num_partitions=1, replication_factor=1)
    try:
        admin_client.create_topics([topic_obj])
        print(f"Created topic {topic}")
    except Exception as e:
        print(f"Error creating topic: {e}")
    admin_client.close()
    return topic

def test_produce_message(test_topic, sample_event):
    """Test producing a message to Kafka."""
    producer = create_kafka_producer(brokers)
    send_event(producer, test_topic, sample_event)
    producer.flush()
    producer.close()
    time.sleep(3)

def test_topic_available(test_topic):
    """Test topic becomes available after producing."""
    admin_client = KafkaAdminClient(bootstrap_servers=brokers)
    topics = admin_client.list_topics()
    print("Available topics:", topics)
    wait_for_topic(admin_client, test_topic, timeout=30)
    admin_client.close()

def test_consume_message(test_topic, sample_event):
    """Test consuming the produced message from Kafka."""
    # Wait for message to be available
    # time.sleep(30)
    ensure_topic_exists(brokers, test_topic)
    consumer = KafkaConsumer(
        test_topic,
        bootstrap_servers=brokers,
        group_id=f"debug-group-{uuid.uuid4()}",
        auto_offset_reset='earliest',
        value_deserializer=lambda m: m.decode("utf-8"),
        consumer_timeout_ms=10000,
    )
    found = False
    while not consumer.assignment():
        print("Waiting for assignment...")
        consumer.poll(timeout_ms=100)
        time.sleep(1)
    
    print(f"Assignment: {consumer.assignment()}")
    for tp in consumer.assignment():
        print(f"Partition {tp}: position {consumer.position(tp)}")
        # Only seek to beginning if you want all old messages
        consumer.seek_to_beginning(tp)

    for msg in consumer:
        try:
            event = json.loads(msg.value)
            if event.get("user_id") == sample_event["user_id"]:
                found = True
                break
        except Exception:
            continue
    consumer.close()
    assert found, "Produced event was not found in Kafka topic."
