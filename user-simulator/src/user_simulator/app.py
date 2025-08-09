# src/user_simulator/app.py
from __future__ import annotations
import json
import logging
import signal
import sys
import time
from typing import Optional

from .config import load
from .generator import make_event, key_for_event, seed_everything
from .metrics import events_total, errors_total, loop_seconds, inflight, start_metrics
from .producer import build_producer, ensure_topic
from confluent_kafka import Producer

log = logging.getLogger("user-simulator")
_stop = False

def _setup_logging(level: str):
    h = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    h.setFormatter(fmt)
    log.addHandler(h)
    log.setLevel(level.upper())

def _handle_signal(signum, _frame):
    global _stop
    log.info(f"signal {signum} received; draining…")
    _stop = True

def _delivery_cb(err, msg):
    if err is not None:
        errors_total.inc()
        # keep logs sparse; debug on failure bursts only
        if errors_total._value.get() % 100 == 1:
            log.warning("delivery_error: %s", err)
    else:
        events_total.inc()
        val = inflight._value.get() - 1
        if val >= 0:
            inflight.set(val)

def _second_pacer(rate: int):
    """
    Simple pacing: attempt to send `rate` events each wall-second.
    """
    if rate <= 0:
        rate = 1
    next_tick = time.time()
    while not _stop:
        start = time.time()
        yield rate
        # sleep until the next wall second
        next_tick += 1.0
        remaining = next_tick - time.time()
        if remaining > 0:
            time.sleep(remaining)
        # guard drift
        if time.time() - next_tick > 5:
            next_tick = time.time()

def main():
    cfg = load()
    _setup_logging(cfg.log_level)
    seed_everything(cfg.seed)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    start_metrics(cfg.metrics_port)
    log.info("metrics listening", extra={"port": cfg.metrics_port})

    if cfg.create_topic:
        ensure_topic(cfg.brokers, cfg.topic, cfg.topic_partitions, cfg.topic_replication)

    prod: Producer = build_producer(
        brokers=cfg.brokers,
        linger_ms=cfg.linger_ms,
        batch_num_messages=cfg.batch_num_messages,
        compression=cfg.compression,
    )
    log.info(
        "producer ready",
        extra={
            "brokers": cfg.brokers,
            "topic": cfg.topic,
            "rate": cfg.rate,
            "linger_ms": cfg.linger_ms,
            "compression": cfg.compression,
        },
    )

    try:
        for per_second in _second_pacer(cfg.rate):
            with loop_seconds.time():
                sent = 0
                while sent < per_second and not _stop:
                    evt = make_event()
                    key = key_for_event(evt)
                    payload = json.dumps(evt)
                    inflight.inc()
                    prod.produce(cfg.topic, key=key, value=payload, on_delivery=_delivery_cb)
                    sent += 1
                # allow background IO to progress
                prod.poll(0)  # serves delivery callbacks
    finally:
        log.info("flushing…")
        prod.flush(10)
        log.info("stopped")

if __name__ == "__main__":
    main()
