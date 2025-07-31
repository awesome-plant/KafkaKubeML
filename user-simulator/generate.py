import json
import random
import time
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer

fake = Faker()

# Kafka config
KAFKA_TOPIC = "user-events"
KAFKA_BOOTSTRAP_SERVERS = "my-cluster-kafka-bootstrap:9092"

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def generate_event():
    return {
        "user_id": fake.uuid4(),
        "timestamp": datetime.utcnow().isoformat(),
        "event_type": random.choice(["click", "view", "purchase"]),
        "page": random.choice(["/home", "/product/123", "/search", "/cart"]),
        "duration": round(random.uniform(0.1, 10.0), 2)
    }

def main():
    print(f"Sending events to Kafka topic '{KAFKA_TOPIC}'...")
    while True:
        event = generate_event()
        producer.send(KAFKA_TOPIC, value=event)
        print("Sent event:", event)
        time.sleep(1)  # adjustable frequency

if __name__ == "__main__":
    main()
