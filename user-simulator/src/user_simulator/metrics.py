# src/user_simulator/metrics.py
from prometheus_client import Counter, Gauge, Summary, start_http_server

events_total = Counter("generator_events_total", "Events successfully queued to Kafka")
errors_total = Counter("generator_send_errors_total", "Producer send errors")
loop_seconds = Summary("generator_loop_seconds", "Loop duration per tick")
inflight = Gauge("generator_inflight", "Messages in-flight to broker")

def start_metrics(port: int):
    start_http_server(port)
