

def test_generate_event():
    event = generate_event()
    assert "user_id" in event
    assert event["event_type"] in ["click", "view", "purchase"]
