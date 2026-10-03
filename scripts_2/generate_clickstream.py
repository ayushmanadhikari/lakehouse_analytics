"""
Fake clickstream generator (bronze layer source).

Outputs (under ./data/streaming):
  clickstream_<run_id>_<n>.json  -> event feed (NDJSON), contains 6 injected anomalies
  users_<run_id>.json            -> user profile change feed (NDJSON) for an SCD2 dim_user

Injected anomalies in the clickstream (to be fixed in the silver layer):
  1. NULL user_id / product_id          -> drop rows (cannot be joined / attributed)
  2. NULL device / country / category   -> fill ("unknown") or backfill from dimensions
  3. Duplicate events (same event_id)   -> dedupe on event_id
  4. Invalid quantity on cart/checkout/purchase events (null, 0, negative) -> drop or quarantine
  5. Future-dated event_time            -> filter event_time > ingestion time
  6. Dirty strings in event_type/device (casing, whitespace) -> trim + lower

SCD2 tracked attributes (users feed): country, loyalty_tier
  Each user has an initial record plus 0-3 later changes, each with its own updated_at.
  Event `country` is the country valid at event_time, so events and user history agree.
"""
import bisect
import copy
import json
import random
import uuid
from collections import Counter
from datetime import datetime, timezone, timedelta
from pathlib import Path

SEED = None  # set to an int for reproducible output
if SEED is not None:
    random.seed(SEED)

BASE_DIR = Path.cwd()
OUTPUT_DIR = BASE_DIR / "data" / "streaming"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR_USER = BASE_DIR / "data" / "users"
OUTPUT_DIR_USER.mkdir(parents=True, exist_ok=True)

NUM_FILES = 1
MIN_RECORDS_PER_FILE = 1_000
MAX_RECORDS_PER_FILE = 1_500
NUM_USERS = 200               # small enough that each user has several sessions + history
NUM_PRODUCTS = 20
WINDOW_DAYS = 30              # events and user changes are spread over this window
SHARE_USERS_WITH_CHANGES = 0.60
MAX_CHANGES_PER_USER = 3

PRODUCT_CATEGORIES = [
    "pain_relief", "vitamins", "skin_care", "baby_care",
    "cold_flu", "personal_care", "prescription", "fitness",
]
DEVICES = ["mobile", "desktop", "tablet"]
COUNTRIES = ["Germany", "Netherlands", "Belgium", "Austria", "France"]
LOYALTY_TIERS = ["bronze", "silver", "gold", "platinum"]
QUANTITY_EVENTS = {"add_to_cart", "checkout", "purchase"}

# Anomaly rates (fraction of records per file)
ANOMALY_RATES = {
    "null_user_or_product": 0.015,
    "null_device_country_category": 0.03,
    "duplicate_events": 0.03,
    "invalid_quantity": 0.15,   # fraction of quantity-type events only
    "future_event_time": 0.01,
    "dirty_strings": 0.03,
}

NOW = datetime.now(timezone.utc)
WINDOW_START = NOW - timedelta(days=WINDOW_DAYS)

PRODUCTS = {
    f"prod_{i}": {
        "category": random.choice(PRODUCT_CATEGORIES),
        "price": round(random.uniform(3.99, 149.99), 2),
    }
    for i in range(1, NUM_PRODUCTS + 1)
}


# --------------------------------------------------------------------------- #
# Users + SCD2 history
# --------------------------------------------------------------------------- #
def build_user_history():
    """Return {user_id: [version dicts sorted by updated_at]}."""
    history = {}
    for i in range(1, NUM_USERS + 1):
        user_id = f"user_{i}"
        signup = WINDOW_START - timedelta(days=random.randint(30, 365))
        country = random.choice(COUNTRIES)
        tier = "bronze"

        versions = [{
            "user_id": user_id,
            "email": f"{user_id}@example.com",
            "country": country,
            "loyalty_tier": tier,
            "signup_date": signup.date().isoformat(),
            "updated_at": signup,
            "change_type": "insert",
        }]

        if random.random() < SHARE_USERS_WITH_CHANGES:
            n_changes = random.randint(1, MAX_CHANGES_PER_USER)
            change_times = sorted(
                WINDOW_START + timedelta(seconds=random.randint(0, WINDOW_DAYS * 86400 - 1))
                for _ in range(n_changes)
            )
            for ts in change_times:
                # always change at least one tracked attribute
                change_country = random.random() < 0.5
                change_tier = (not change_country) or random.random() < 0.4
                if change_country:
                    country = random.choice([c for c in COUNTRIES if c != country])
                if change_tier:
                    tier = random.choice([t for t in LOYALTY_TIERS if t != tier])
                versions.append({
                    "user_id": user_id,
                    "email": f"{user_id}@example.com",
                    "country": country,
                    "loyalty_tier": tier,
                    "signup_date": signup.date().isoformat(),
                    "updated_at": ts,
                    "change_type": "update",
                })
        history[user_id] = versions
    return history


USER_HISTORY = build_user_history()
USER_IDS = list(USER_HISTORY)
PRODUCT_IDS = list(PRODUCTS)
_VERSION_TIMES = {u: [v["updated_at"] for v in vs] for u, vs in USER_HISTORY.items()}


def user_attrs_at(user_id, ts):
    """Profile version valid at timestamp ts."""
    idx = bisect.bisect_right(_VERSION_TIMES[user_id], ts) - 1
    return USER_HISTORY[user_id][max(idx, 0)]


def write_users_feed(run_id):
    path = OUTPUT_DIR_USER / f"users_{run_id}.json"
    rows = sorted(
        (v for vs in USER_HISTORY.values() for v in vs),
        key=lambda v: v["updated_at"],
    )
    with path.open("w", encoding="utf-8") as f:
        for v in rows:
            f.write(json.dumps({**v, "updated_at": v["updated_at"].isoformat()}) + "\n")
    changed = sum(1 for vs in USER_HISTORY.values() if len(vs) > 1)
    print(f"Generated {len(rows):,} user records ({changed} users with history): {path}")


# --------------------------------------------------------------------------- #
# Clean event generation
# --------------------------------------------------------------------------- #
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
        "country": user_attrs_at(user_id, event_time)["country"],
        "price": product["price"],
        "quantity": quantity if event_type in QUANTITY_EVENTS else None,
    }


def generate_session(session_number):
    user_id = random.choice(USER_IDS)
    session_id = f"session_{session_number}"
    product_id = random.choice(PRODUCT_IDS)
    device = random.choice(DEVICES)
    quantity = random.randint(1, 5)
    t = WINDOW_START + timedelta(seconds=random.randint(0, WINDOW_DAYS * 86400 - 3600))

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


# --------------------------------------------------------------------------- #
# Anomaly injection (6 types, applied to disjoint records)
# --------------------------------------------------------------------------- #
def inject_anomalies(batch):
    n = len(batch)
    used = set()
    counts = Counter()

    def sample(rate, base_pool, base_size=None):
        pool = [i for i in base_pool if i not in used]
        k = min(len(pool), max(1, int((base_size or len(pool)) * rate)))
        chosen = random.sample(pool, k)
        used.update(chosen)
        return chosen

    all_idx = list(range(n))
    qty_pool = [i for i in all_idx if batch[i]["event_type"] in QUANTITY_EVENTS]

    # 1. null user_id / product_id
    for i in sample(ANOMALY_RATES["null_user_or_product"], all_idx, n):
        batch[i][random.choice(["user_id", "product_id"])] = None
        counts["null_user_or_product"] += 1

    # 2. null device / country / category (non-critical, recoverable)
    for i in sample(ANOMALY_RATES["null_device_country_category"], all_idx, n):
        batch[i][random.choice(["device", "country", "category"])] = None
        counts["null_device_country_category"] += 1

    # 4. invalid quantity on quantity-type events (null / 0 / negative)
    for i in sample(ANOMALY_RATES["invalid_quantity"], qty_pool):
        batch[i]["quantity"] = random.choice([None, 0, -1, -3])
        counts["invalid_quantity"] += 1

    # 5. future-dated event_time
    for i in sample(ANOMALY_RATES["future_event_time"], all_idx, n):
        ts = datetime.fromisoformat(batch[i]["event_time"]) + timedelta(days=random.randint(30, 365))
        batch[i]["event_time"] = ts.isoformat()
        counts["future_event_time"] += 1

    # 6. dirty strings (case / whitespace) in event_type and device
    for i in sample(ANOMALY_RATES["dirty_strings"], all_idx, n):
        field = random.choice(["event_type", "device"])
        value = batch[i][field]
        batch[i][field] = random.choice([value.upper(), value.title(), f" {value} ", f"{value.upper()} "])
        counts["dirty_strings"] += 1

    # 3. duplicate events (exact copies of clean, untouched rows)
    clean_pool = [i for i in all_idx if i not in used]
    dup_idx = random.sample(clean_pool, max(1, int(n * ANOMALY_RATES["duplicate_events"])))
    batch.extend(copy.deepcopy(batch[i]) for i in dup_idx)
    counts["duplicate_events"] = len(dup_idx)

    random.shuffle(batch)
    return counts


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def generate_clickstream():
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
    write_users_feed(run_id)

    start = 0
    total_written = 0
    for file_number, size in enumerate(file_sizes, 1):
        batch = all_events[start:start + size]
        start += size

        counts = inject_anomalies(batch)

        output_file = OUTPUT_DIR / f"clickstream_{run_id}_{file_number}.json"
        with output_file.open("w", encoding="utf-8") as f:
            for event in batch:
                f.write(json.dumps(event) + "\n")

        total_written += len(batch)
        print(f"Generated {len(batch):,} events: {output_file}")
        for name, c in counts.items():
            print(f"   anomaly {name}: {c}")

    print(f"Total events written: {total_written:,} across {NUM_FILES} files "
          f"({session_number:,} sessions)")


if __name__ == "__main__":
    generate_clickstream()