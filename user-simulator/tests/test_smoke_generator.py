# tests/test_smoke_generator.py
import re
from user_simulator.generator import make_event, key_for_event

def test_smoke_event_shape():
    evt = make_event()
    assert {"event_id","user_id","event_type","event_ts","url","product_id","user_agent","schema_version"} <= set(evt)
    assert isinstance(evt["event_ts"], int)
    assert isinstance(evt["event_id"], str)
    assert key_for_event(evt)  # non-empty key

def test_smoke_event_id_format():
    evt = make_event()
    assert re.match(r"^[0-9a-fA-F-]{36}$", evt["event_id"])
