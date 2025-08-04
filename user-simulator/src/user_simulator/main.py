# src/user_simulator/main.py

import os
import time
from user_simulator.generator import generate_event
from user_simulator.kafka_client import create_kafka_producer, send_event

def run_simulation(brokers, topic, num_events=100, interval=1):
    """Generate and send fake events in a loop."""
    producer = create_kafka_producer(brokers)
    for i in range(num_events):
        event = generate_event()
        send_event(producer, topic, event)
        print(f"[{i+1}/{num_events}] Sent: {event}")
        time.sleep(interval)
    producer.close()

if __name__ == "__main__":
    brokers = os.environ.get("KAFKA_BROKER")
    topic = os.environ.get("KAFKA_TOPIC")
    num_events = int(os.environ.get("NUM_EVENTS", "100"))
    interval = float(os.environ.get("INTERVAL", "1"))

    print(f"Sending {num_events} fake events to topic '{topic}' at {brokers} ...")
    run_simulation(brokers, topic, num_events, interval)
