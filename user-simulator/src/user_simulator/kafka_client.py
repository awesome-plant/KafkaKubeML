# src/user_simulator/kafka_client.py

import json
from kafka import KafkaProducer
from time import time, sleep 
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# BROKERS = "kafka-cluster-kafka-bootstrap.kafka-stream:9092"
# TOPIC = 'USER-DATA-TOPIC'

def ensure_topic_exists(brokers, topic, num_partitions=1, replication_factor=1):
    admin = KafkaAdminClient(bootstrap_servers=brokers)
    try:
        admin.create_topics([NewTopic(topic, num_partitions=num_partitions, replication_factor=replication_factor)])
        print(f"Created topic {topic}")
    except TopicAlreadyExistsError:
        print(f"Topic {topic} already exists.")
    finally:
        admin.close()

def create_topic_if_not_exists(brokers, topic, num_partitions=1, replication_factor=1):
    admin = KafkaAdminClient(bootstrap_servers=brokers)
    topic_list = [NewTopic(name=topic, num_partitions=num_partitions, replication_factor=replication_factor)]
    try:
        admin.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Topic '{topic}' created.")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic}' already exists.")
    finally:
        admin.close()

def create_kafka_producer(brokers):
    """Create and return a KafkaProducer."""
    return KafkaProducer(
        bootstrap_servers=brokers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        linger_ms=10,
        acks="all",
    )

def send_event(producer, topic, event):
    """Serialize and send event to Kafka."""
    producer.send(topic, value=event)
    # Flush for demo reliability; remove for higher performance
    producer.flush()

def wait_for_topic(admin_client, topic, timeout=30):
    deadline = time() + timeout
    while time() < deadline:
        topics = admin_client.list_topics()
        if topic in topics:
            return
        sleep(1)
    raise TimeoutError(f"Timed out waiting for topic {topic} to be available.")