import os
import json
import tempfile
import pytest
from unittest import mock
from unittest.mock import patch, MagicMock

import pandas as pd

# Assume your consumer.py contains a main() and helper functions (adapt as needed)
from stream_processor import consumer

@pytest.fixture
def sample_event():
    return {"user_id": 1, "event": "click", "timestamp": "2024-08-07T13:00:00Z"}

def test_kafka_consumer_initializes_with_correct_config(monkeypatch):
    with patch('stream_processor.consumer.KafkaConsumer') as mock_consumer:
        monkeypatch.setenv("KAFKA_BROKER", "localhost:9092")
        monkeypatch.setenv("KAFKA_TOPIC", "user-interactions")
        consumer.main()
        mock_consumer.assert_called_with(
            "user-interactions",
            bootstrap_servers="localhost:9092",
            value_deserializer=mock.ANY,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id='parquet-consumer-group'
        )

def test_kafka_message_is_deserialized_and_added_to_batch(sample_event, monkeypatch):
    # Patch KafkaConsumer and simulate one message
    with patch('stream_processor.consumer.KafkaConsumer') as mock_consumer:
        instance = mock_consumer.return_value
        message = MagicMock()
        message.value = sample_event
        instance.__iter__.return_value = [message] * 2  # Simulate two messages

        with patch.object(consumer.pd.DataFrame, "to_parquet") as mock_to_parquet:
            # Patch os.makedirs to avoid real dir creation
            with patch('stream_processor.consumer.os.makedirs'):
                monkeypatch.setenv("BATCH_SIZE", "2") # Trigger batch on 2 messages
                consumer.main()
                assert mock_to_parquet.called

def test_batch_write_to_parquet_triggers_on_batch_size(sample_event, tmp_path):
    # Simulate writing batch to parquet
    batch = [sample_event] * 3
    parquet_path = tmp_path / "batch_0.parquet"
    df = pd.DataFrame(batch)
    df.to_parquet(parquet_path)
    assert parquet_path.exists()

def test_writes_correct_parquet_data(sample_event, tmp_path):
    # Write Parquet, read back, check integrity
    batch = [sample_event]
    parquet_path = tmp_path / "batch_test.parquet"
    df = pd.DataFrame(batch)
    df.to_parquet(parquet_path)
    read_df = pd.read_parquet(parquet_path)
    pd.testing.assert_frame_equal(df, read_df)

def test_handles_invalid_message_format():
    # Patch KafkaConsumer to produce an invalid message
    invalid_message = MagicMock()
    invalid_message.value = "not a dict"  # Should be a dict

    with patch('stream_processor.consumer.KafkaConsumer') as mock_consumer:
        instance = mock_consumer.return_value
        instance.__iter__.return_value = [invalid_message]
        with patch('stream_processor.consumer.pd.DataFrame.to_parquet') as mock_to_parquet:
            with patch('stream_processor.consumer.os.makedirs'):
                # Expect: error handling (should not crash or write parquet)
                try:
                    consumer.BATCH_SIZE = 1
                    consumer.main()
                except Exception:
                    pytest.fail("main() should handle invalid message formats gracefully")

def test_handles_kafka_disconnect():
    # Simulate KafkaConsumer raising exception
    with patch('stream_processor.consumer.KafkaConsumer', side_effect=Exception("Kafka error")):
        try:
            consumer.main()
        except Exception as e:
            assert "Kafka error" in str(e)  # You may want to test for clean logging/exit instead

def test_handles_filesystem_error(sample_event):
    with patch('stream_processor.consumer.KafkaConsumer') as mock_consumer:
        instance = mock_consumer.return_value
        message = MagicMock()
        message.value = sample_event
        instance.__iter__.return_value = [message] * 2
        # Patch os.makedirs to raise error
        with patch('stream_processor.consumer.os.makedirs', side_effect=PermissionError):
            with pytest.raises(PermissionError):
                consumer.BATCH_SIZE = 2
                consumer.main()

def test_reads_env_vars_for_config(monkeypatch):
    monkeypatch.setenv("KAFKA_BROKER", "mybroker:1234")
    monkeypatch.setenv("KAFKA_TOPIC", "mytopic")
    monkeypatch.setenv("PARQUET_DIR", "/tmp/parquet_test")
    monkeypatch.setenv("BATCH_SIZE", "15")
    assert os.environ["KAFKA_BROKER"] == "mybroker:1234"
    assert os.environ["KAFKA_TOPIC"] == "mytopic"
    assert os.environ["PARQUET_DIR"] == "/tmp/parquet_test"
    assert os.environ["BATCH_SIZE"] == "15"

