# stream_processor/app.py
import os, sys, signal, time, json, logging
from typing import List, Dict, Any

from confluent_kafka import Consumer, KafkaException, TopicPartition, Producer
import pandas as pd

from stream_processor.config import Config, load_config
from stream_processor.util import parse_event_ts_utc, now_utc_iso
from stream_processor.writer import ParquetPartitionWriter

log = logging.getLogger("parquet-consumer")
_stop = False

def _setup_logging():
    h = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s')
    h.setFormatter(fmt)
    log.addHandler(h)
    log.setLevel(os.getenv("LOG_LEVEL", "INFO"))

def _handle_signal(signum, _frame):
    global _stop
    log.info(f"Received signal {signum}; stopping gracefully...")
    _stop = True

def build_consumer(cfg: Config) -> Consumer:
    conf = {
        "bootstrap.servers": cfg.kafka_brokers,
        "group.id": cfg.group_id,
        "auto.offset.reset": cfg.auto_offset_reset,
        "enable.auto.commit": False,
        "session.timeout.ms": cfg.session_timeout_ms,
        "max.poll.interval.ms": cfg.max_poll_interval_ms,
        "allow.auto.create.topics": False,
    }
    return Consumer(conf)

def build_dlq_producer(brokers: str | None) -> Producer | None:
    if not brokers:
        return None
    return Producer({"bootstrap.servers": brokers})

def maybe_send_dlq(p: Producer | None, topic: str | None, raw_value: bytes, headers: list | None = None):
    if not p or not topic:
        return
    try:
        p.produce(topic, value=raw_value, headers=headers or [])
    except BufferError:
        p.poll(0)
        p.produce(topic, value=raw_value, headers=headers or [])
    p.poll(0)

def run():
    _setup_logging()
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    cfg = load_config()
    log.info(
        "Starting consumer",
        extra={
            "topic": cfg.topic,
            "brokers": cfg.kafka_brokers,
            "roots": ",".join(cfg.parquet_dirs),
            "dataset": cfg.dataset_name,
            "target_mib": cfg.target_file_size_mib,
            "max_files_per_partition": cfg.max_files_per_partition,
            "naming": cfg.file_naming,
            "compression": cfg.parquet_compression,
        },
    )

    consumer = build_consumer(cfg)
    dlq_producer = build_dlq_producer(cfg.dlq_brokers)
    consumer.subscribe([cfg.topic])

    writer = ParquetPartitionWriter(
        root_dirs=cfg.parquet_dirs,
        dataset_name=cfg.dataset_name,
        tmp_dir=cfg.tmp_dir,
        compression=cfg.parquet_compression,
        naming=cfg.file_naming,
        mirror_strategy=cfg.mirror_strategy,
        run_id=cfg.run_id,
        target_file_size_mib=cfg.target_file_size_mib,
        max_files_per_partition=cfg.max_files_per_partition,
        max_rows_per_file=cfg.max_rows_per_file,
        compression_factor=cfg.compression_factor,
    )

    batch: List[Dict[str, Any]] = []
    offsets: Dict[TopicPartition, int] = {}
    last_flush = time.monotonic()
    files_written = 0
    records_total = 0

    try:
        while not _stop:
            msg = consumer.poll(timeout=1.0)
            now = time.monotonic()

            if msg is None:
                pass
            elif msg.error():
                log.error(f"kafka_error: {msg.error()}")
            else:
                raw = msg.value()
                try:
                    record = json.loads(raw)
                    ts = parse_event_ts_utc(record.get(cfg.event_ts_field))
                    record["_event_ts_utc"] = ts.isoformat()
                    record["_ingested_ts_utc"] = now_utc_iso()
                    record["_topic"] = msg.topic()
                    record["_partition"] = msg.partition()
                    record["_offset"] = msg.offset()
                    batch.append(record)

                    tp = TopicPartition(msg.topic(), msg.partition())
                    offsets[tp] = msg.offset()
                except Exception as e:
                    log.warning(
                        "json_or_ts_parse_error; sending to DLQ" if cfg.dlq_topic else f"json_or_ts_parse_error: {e}; dropping"
                    )
                    maybe_send_dlq(dlq_producer, cfg.dlq_topic, raw, headers=[("error", b"parse")])

            # Flush on size or time
            should_flush = len(batch) >= cfg.batch_size or (now - last_flush) >= cfg.flush_interval_seconds
            if should_flush and batch:
                try:
                    paths = writer.write_partitioned(batch, ts_field="_event_ts_utc")
                    files_written += len(paths)
                    records_total += len(batch)
                    log.info(f"wrote files={len(paths)} rows={len(batch)} total_rows={records_total}")
                    batch.clear()
                    last_flush = now
                    # Commit after success
                    commits = [TopicPartition(tp.topic, tp.partition, off + 1) for tp, off in offsets.items()]
                    consumer.commit(offsets=commits, asynchronous=False)
                    offsets.clear()
                except Exception as e:
                    log.exception(f"write_or_commit_failed; will NOT commit offsets; retrying: {e}")
                    time.sleep(1.0)

        # Final flush on shutdown
        if batch:
            try:
                paths = writer.write_partitioned(batch, ts_field="_event_ts_utc")
                files_written += len(paths)
                records_total += len(batch)
                log.info(f"final_flush wrote files={len(paths)} rows={len(batch)} total_rows={records_total}")
                batch.clear()
                commits = [TopicPartition(tp.topic, tp.partition, off + 1) for tp, off in offsets.items()]
                consumer.commit(offsets=commits, asynchronous=False)
                offsets.clear()
            except Exception as e:
                log.exception(f"final_flush_failed: {e}")

    except KafkaException as ke:
        log.exception(f"kafka_exception: {ke}")
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        if dlq_producer:
            dlq_producer.flush(5)
        log.info(f"stopped; files_written={files_written} records_total={records_total}")

if __name__ == "__main__":
    run()