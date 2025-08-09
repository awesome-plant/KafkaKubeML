# src/user_simulator/generator.py
import random
import time
import uuid
from typing import Dict, Any
from faker import Faker

faker = Faker()

def seed_everything(seed: int | None):
    if seed is None:
        return
    random.seed(seed)
    Faker.seed(seed)

EVENT_TYPES = ["click", "view", "purchase", "signup"]

def make_event() -> Dict[str, Any]:
    """
    Generate a user interaction event.
    Field names align with downstream consumers (event_ts is epoch seconds).
    """
    user_id = faker.uuid4()
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "event_type": random.choice(EVENT_TYPES),
        "event_ts": int(time.time()),
        "url": faker.url(),
        "product_id": faker.uuid4(),
        "user_agent": faker.user_agent(),
        "schema_version": 1,
    }

def key_for_event(evt: Dict[str, Any]) -> bytes:
    # keep ordering per user on a partition
    return (evt.get("user_id") or "").encode("utf-8")
