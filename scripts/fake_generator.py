import json
import random
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path

BASE_DIR = Path.cwd()
OUTPUT_DIR = BASE_DIR / "data" / "streaming"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

NUM_FILES = 5
MIN_RECORDS_PER_FILE = 10_000
MAX_RECORDS_PER_FILE = 15_000
NUM_USERS = 5_000      # scaled up so ~28k sessions don't reuse 300 users constantly
NUM_PRODUCTS = 200

PRODUCT_CATEGORIES = [
    "pain_relief", "vitamins", "skin_care", "baby_care",
    "cold_flu", "personal_care", "prescription", "fitness",
]
DEVICES = ["mobile", "desktop", "tablet"]
COUNTRIES = ["Germany", "Netherlands", "Belgium", "Austria", "France"]
QUANTITY_EVENTS = {"add_to_cart", "checkout", "purchase"}

PRODUCTS = {
    f"prod_{i}": {
        "category": random.choice(PRODUCT_CATEGORIES),
        "price": round(random.uniform(3.99, 149.99), 2),
    }
    for i in range(1, NUM_PRODUCTS + 1)
}
USERS = {
    f"user_{i}": {"country": random.choice(COUNTRIES)}
    for i in range(1, NUM_USERS + 1)
}
USER_IDS = list(USERS)
PRODUCT_IDS = list(PRODUCTS)


def create_event(user_id, session_id, event_type, product_id, device, event_time, quantity):
    product = PRODUCTS[product_id]
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "session_id": session_id,
        "event_time": event_time.isoformat(),
        "event_type": event_type,
        "product_id": product_id,
        "category": product["category"],
        "device": device,
        "country": USERS[user_id]["country"],
        "price": product["price"],
        "quantity": quantity if event_type in QUANTITY_EVENTS else None,
    }


def generate_session(session_number):
    user_id = random.choice(USER_IDS)
    session_id = f"session_{session_number}"
    product_id = random.choice(PRODUCT_IDS)
    device = random.choice(DEVICES)
    quantity = random.randint(1, 5)
    t = datetime.now(timezone.utc) - timedelta(minutes=random.randint(1, 10_000))

    def evt(event_type):
        nonlocal t
        t += timedelta(seconds=random.randint(5, 120))
        return create_event(user_id, session_id, event_type, product_id, device, t, quantity)

    events = [evt("page_view")]
    if random.random() < 0.70:
        events.append(evt("product_view"))
        if random.random() < 0.40:
            events.append(evt("add_to_cart"))
            if random.random() < 0.50:
                events.append(evt("checkout"))
                if random.random() < 0.60:
                    events.append(evt("purchase"))
    return events


def generate_clickstream():
    # Pick a random size per file, then generate sessions until we have enough events
    file_sizes = [
        random.randint(MIN_RECORDS_PER_FILE, MAX_RECORDS_PER_FILE)
        for _ in range(NUM_FILES)
    ]
    total_needed = sum(file_sizes)

    all_events = []
    session_number = 0
    while len(all_events) < total_needed:
        session_number += 1
        all_events.extend(generate_session(session_number))

    random.shuffle(all_events)
    all_events = all_events[:total_needed]

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    start = 0
    for file_number, size in enumerate(file_sizes, 1):
        batch = all_events[start:start + size]
        start += size

        output_file = OUTPUT_DIR / f"clickstream_{run_id}_{file_number}.json"
        with output_file.open("w", encoding="utf-8") as f:
            for event in batch:
                f.write(json.dumps(event) + "\n")

        print(f"Generated {len(batch):,} events: {output_file}")

    print(f"Total events generated: {total_needed:,} across {NUM_FILES} files "
        f"({session_number:,} sessions)")


if __name__ == "__main__":
    generate_clickstream()