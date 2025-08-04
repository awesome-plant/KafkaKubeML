# user-simulator

**Fake User Event Generator for Streaming Demo**

---

## Overview

`user-simulator` is a Python microservice designed to generate and send fake user interaction events (such as clicks, views, and purchases) to an Apache Kafka topic.  
It’s built to be part of a larger data engineering and AI demo pipeline, running in Kubernetes with orchestration via Airflow and downstream analytics/inference.

---

## Features

- Generates realistic, randomized user interaction data using [Faker](https://faker.readthedocs.io/)
- Streams events to Kafka in real-time (configurable rate and volume)
- Modular code structure for easy extension/testing
- Ready for Docker and Kubernetes deployment
- Includes unit tests for data generation and Kafka producer logic

---

## Directory Structure
```
user-simulator/
├── src/
│   └── user_simulator/
│       ├── __init__.py
│       ├── generator.py
│       ├── kafka_client.py
│       └── main.py
├── tests/
│   ├── __init__.py
│   ├── test_generator.py
│   └── test_main.py
|   └── integration/
|       └── test_kafka_end_to_end.py     # actually hits Kafka
├── requirements.txt
├── Dockerfile
├── README.md
└── pyproject.toml
```
---
## Quickstart
### 1. **Install Requirements**

```bash
pip install -r requirements.txt
```

### 2. **Set Environment Variables**

    KAFKA_BROKER — Comma-separated Kafka broker addresses (e.g. kafka:9092)

    KAFKA_TOPIC — Target Kafka topic for events (e.g. user-interactions)

### 3. **Run the User Simulator**
```
python -m user_simulator.main
```
Or, with custom arguments (to be implemented):
```
python -m user_simulator.main --num-events 1000 --interval 0.5
```
### 4. **Run Tests**
```
pytest
```

### Configuration

You can configure event frequency, total number of events, Kafka connection, and more via environment variables or CLI arguments (see code for details).
Docker Usage

### Build and run with Docker:
```
docker build -t user-simulator .
docker run --env KAFKA_BROKER=kafka:9092 --env KAFKA_TOPIC=user-interactions user-simulator
```
### Development & Extending

    Extend generator.py to add new event types or fields.

    Mock Kafka producer in tests for CI-friendliness.

    See tests/ for usage examples.

### License

MIT (or insert your preferred license here)
Contact

For questions or issues, please open an issue or contact [your-name/email].