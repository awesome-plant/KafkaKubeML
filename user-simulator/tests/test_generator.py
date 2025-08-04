# tests/test_generator.py
"""
Unit tests for user_simulator.generator.generate_event

Covers:
- Structure and contents of generated fake events
- Field presence and value types
- Event randomness for user_id and event_type
"""

from user_simulator import generator

def test_generate_event_returns_dict():
    """generate_event should return a dict."""
    event = generator.generate_event()
    assert isinstance(event, dict), "Event is not a dictionary"

def test_generate_event_has_expected_fields():
    """The event dict should have all required fields."""
    event = generator.generate_event()
    expected_fields = {"user_id", "event_type", "timestamp", "url", "product_id", "user_agent"}
    assert expected_fields <= event.keys(), f"Missing fields: {expected_fields - event.keys()}"

def test_generate_event_field_types():
    """Check that field types are as expected."""
    event = generator.generate_event()
    assert isinstance(event["user_id"], str)
    assert isinstance(event["event_type"], str)
    assert isinstance(event["timestamp"], int)
    assert isinstance(event["url"], str)
    assert isinstance(event["product_id"], str)
    assert isinstance(event["user_agent"], str)

def test_generate_event_event_type_choices():
    """Event type should be one of the allowed types."""
    allowed_types = {"click", "view", "purchase", "signup"}
    event = generator.generate_event()
    assert event["event_type"] in allowed_types

def test_generate_event_randomness():
    """
    Multiple events should have different user_ids.
    This is a simple check for randomness.
    """
    events = [generator.generate_event() for _ in range(5)]
    user_ids = set(e["user_id"] for e in events)
    assert len(user_ids) > 1, "User IDs are not random across events"
