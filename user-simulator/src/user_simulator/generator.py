# src/user_simulator/generator.py

import random
import time
from faker import Faker

faker = Faker()

def generate_event():
    """Generate a fake user interaction event as a dict."""
    return {
        "user_id": faker.uuid4(),
        "event_type": random.choice(["click", "view", "purchase", "signup"]),
        "timestamp": int(time.time()),
        "url": faker.url(),
        "product_id": faker.uuid4(),
        "user_agent": faker.user_agent(),
    }
