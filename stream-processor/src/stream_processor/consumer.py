import os
import json
import pandas as pd
from kafka import KafkaConsumer

def main():
    KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka-cluster-kafka-bootstrap.kafka-stream:9092")
    KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "user-interactions")
    PARQUET_DIR = os.environ.get("PARQUET_DIR", "/data/parquet")
    BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 100))
    os.makedirs(PARQUET_DIR, exist_ok=True)

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='parquet-consumer-group'
    )

    batch = []
    file_counter = 0

    print(f"Listening to Kafka topic {KAFKA_TOPIC} at {KAFKA_BROKER}")

    for message in consumer:
        batch.append(message.value)
        if len(batch) >= BATCH_SIZE:
            df = pd.DataFrame(batch)
            file_path = os.path.join(PARQUET_DIR, f"batch_{file_counter}.parquet")
            df.to_parquet(file_path)
            print(f"Wrote {len(batch)} messages to {file_path}")
            batch.clear()
            file_counter += 1

if __name__ == "__main__":
    main()
