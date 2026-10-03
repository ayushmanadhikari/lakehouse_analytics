"""
Bronze-layer test data generator for a (pharmacy-style) e-commerce platform.

  data/streaming/clickstream_<run>_<n>.json   JSON-lines click events
  data/users_cdc/users_cdc_<run>_<n>.json     user change feed (I/U/D) -> source for SCD2 dim_user
  data/reference/products_<run>.csv           product catalog with price history
  data/audit/                                 ground truth of every injected problem

EVENT ANOMALIES (what silver should do)
  null_required_field   session_id / event_time / event_type / product_id / logged-in user_id null -> quarantine
  recoverable_nulls     category / price / device / country / currency null -> backfill from dims
  placeholder_strings   "", "N/A", "null", "unknown" -> normalise to NULL
  case_whitespace       "Page_View", " purchase " -> trim + lower
  timestamp_format      epoch s/ms, "2026-10-03 10:00:00", dd/mm/yyyy, Z, +01:00 -> parse to UTC
  future_timestamp      event_time in the future -> quarantine / flag
  bad_quantity          0, -1, 999, "3", 2.0, "two" -> validate range + cast
  duplicate             same event_id delivered 2+ times -> dedupe on event_id
  late_arrival          ingest_time hours/days after event_time -> watermarks / reprocessing
  bot_traffic           crawler sessions -> filter out

LEGITIMATE NULLS (not errors)
  user_id null when is_logged_in = false; product_id/category/price null on page_view and search;
  quantity / order_id / search_query only on the event types where they apply.

USER CDC (SCD2 source)
  real attribute history (country/city, loyalty tier, email, marketing opt-in), new users inside the
  window, hard deletes (op = D), out-of-order rows, plus:
  noop_update           "update" with no attribute change -> must NOT open a new SCD2 version
  duplicate             same CDC record twice -> dedupe
  null_email            email null on a real change -> don't overwrite good value
  email_format          " John@X.COM " -> trim + lower
  tier_case             "GOLD" / "Gold" -> lower
  timestamp_format      mixed timestamp formats -> parse to UTC

Set SEED to an int for reproducible output.
"""
import csv
import json
import random
import uuid
from bisect import bisect_right
from collections import Counter
from copy import copy
from datetime import datetime, timedelta, timezone
from itertools import accumulate
from pathlib import Path

# ----------------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------------
SEED = None

BASE_DIR = Path.cwd()
EVENTS_DIR = BASE_DIR / "data" / "streaming"
USERS_DIR = BASE_DIR / "data" / "users_cdc"
REF_DIR = BASE_DIR / "data" / "reference"
AUDIT_DIR = BASE_DIR / "data" / "audit"

DAYS_BACK = 30                     # history window (needed so user attributes can change)
NUM_FILES = 1                      # event files (split by ingestion time, like micro-batches)
MIN_RECORDS_PER_FILE = 900
MAX_RECORDS_PER_FILE = 1200
NUM_USER_CDC_FILES = 2
NUM_USERS = 500
NUM_PRODUCTS = 20

# session-level behaviour
ANON_SESSION_RATE = 0.15
BOT_SESSION_RATE = 0.002

# pipeline-level problems
LATE_ARRIVAL_RATE = 0.03
DUPLICATE_RATE = 0.02

# user dimension behaviour
NEW_USER_IN_WINDOW_RATE = 0.15
DELETE_USER_RATE = 0.01
NOOP_UPDATE_RATE = 0.05
CDC_DUPLICATE_RATE = 0.02

# per-record, field-level problems
EVENT_ANOMALY_RATES = {
    "null_required_field": 0.015,
    "recoverable_nulls": 0.03,
    "placeholder_strings": 0.02,
    "case_whitespace": 0.03,
    "timestamp_format": 0.03,
    "future_timestamp": 0.005,
    "bad_quantity": 0.01,
}
CDC_ANOMALY_RATES = {
    "null_email": 0.02,
    "email_format": 0.03,
    "tier_case": 0.03,
    "timestamp_format": 0.03,
}

# ----------------------------------------------------------------------------
# Reference values
# ----------------------------------------------------------------------------
PRODUCT_CATEGORIES = ["pain_relief", "vitamins", "skin_care", "baby_care",
                      "cold_flu", "personal_care", "prescription", "fitness"]
BRANDS = ["Apotheka", "VitaPlus", "DermaCare", "BabySoft", "FluAway", "FitLife"]
DEVICES = ["mobile", "desktop", "tablet"]
COUNTRIES = ["Germany", "Netherlands", "Belgium", "Austria", "France"]
COUNTRY_WEIGHTS = [40, 20, 10, 10, 20]
CITIES = {
    "Germany": ["Berlin", "Munich", "Hamburg", "Cologne"],
    "Netherlands": ["Amsterdam", "Rotterdam", "Utrecht"],
    "Belgium": ["Brussels", "Antwerp", "Ghent"],
    "Austria": ["Vienna", "Graz", "Salzburg"],
    "France": ["Paris", "Lyon", "Marseille"],
}
LOYALTY_TIERS = ["basic", "silver", "gold", "platinum"]
FIRST_NAMES = ["Anna", "Lukas", "Sophie", "Jan", "Marie", "Felix", "Eva", "Tom", "Lea", "Noah"]
LAST_NAMES = ["Müller", "Schmidt", "De Vries", "Peeters", "Dubois", "Bauer", "Jansen", "Weber"]
EMAIL_DOMAINS = ["gmail.com", "outlook.com", "web.de", "gmx.net", "example.com"]
TRAFFIC_SOURCES = ["organic", "paid_search", "email", "direct", "social"]
APP_VERSIONS = ["3.4.1", "3.5.0", "3.6.2"]
SEARCH_TERMS = ["ibuprofen", "vitamin d", "sunscreen", "diapers", "cough syrup", "omega 3", "allergy"]
PLACEHOLDERS = ["", " ", "N/A", "null", "NULL", "None", "unknown", "-"]
USER_AGENTS = {
    "mobile": ["Mozilla/5.0 (iPhone; CPU iPhone OS 17_0)", "Mozilla/5.0 (Linux; Android 14; Pixel 8)"],
    "tablet": ["Mozilla/5.0 (iPad; CPU OS 17_0)", "Mozilla/5.0 (Linux; Android 13; SM-X700)"],
    "desktop": ["Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0)"],
}
BOT_AGENTS = ["Googlebot/2.1 (+http://www.google.com/bot.html)", "bingbot/2.0", "python-requests/2.31"]

PRODUCT_EVENTS = {"product_view", "add_to_cart", "remove_from_cart", "checkout", "purchase"}

# ----------------------------------------------------------------------------
# World state (built once per run in init_world)
# ----------------------------------------------------------------------------
START = END = None
PRODUCTS, PRODUCT_IDS = {}, []
USERS, USER_IDS, USER_CUM_WEIGHTS = {}, [], []
AUDIT = Counter()
GROUND_TRUTH = []          # (entity, record_key, anomaly)


def tag(entity, key, anomaly):
    AUDIT[f"{entity}.{anomaly}"] += 1
    GROUND_TRUTH.append((entity, key, anomaly))


def random_dt(a, b):
    if b <= a:
        return a
    return a + timedelta(seconds=random.uniform(0, (b - a).total_seconds()))


def random_ip():
    return f"{random.randint(11, 223)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"


def ingest_after(ts, normal_max_s, late_rate, late_max_h):
    """Return (ingested_at, was_late). Records older than the window come from the initial load."""
    if ts < START:
        return START, False
    if random.random() < late_rate:
        return min(ts + timedelta(hours=random.uniform(1, late_max_h)), END), True
    return min(ts + timedelta(seconds=random.uniform(1, normal_max_s)), END), False


def ts_variant(value):
    """Re-render an ISO timestamp in one of the formats real systems emit."""
    try:
        dt = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    style = random.choice(["epoch_s", "epoch_ms", "space_naive", "eu_format", "zulu", "local_offset"])
    if style == "epoch_s":
        return int(dt.timestamp())
    if style == "epoch_ms":
        return int(dt.timestamp() * 1000)
    if style == "space_naive":
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    if style == "eu_format":
        return dt.strftime("%d/%m/%Y %H:%M:%S")          # ambiguous with mm/dd
    if style == "zulu":
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return dt.astimezone(timezone(timedelta(hours=1))).isoformat()


# ----------------------------------------------------------------------------
# Products (with price history -> event price is the price AT event time)
# ----------------------------------------------------------------------------
def build_products():
    products = {}
    for i in range(1, NUM_PRODUCTS + 1):
        base = round(random.uniform(3.99, 149.99), 2)
        history = [{"valid_from": START - timedelta(days=365), "price": base}]
        if random.random() < 0.25:
            change_at = random_dt(START + timedelta(days=2), END - timedelta(days=2))
            history.append({"valid_from": change_at, "price": round(base * random.uniform(0.8, 1.2), 2)})
        products[f"prod_{i}"] = {
            "name": f"Product {i}",
            "category": random.choice(PRODUCT_CATEGORIES),
            "brand": random.choice(BRANDS),
            "history": history,
            "valid_froms": [h["valid_from"] for h in history],
        }
    return products


def price_at(product_id, ts):
    p = PRODUCTS[product_id]
    return p["history"][max(bisect_right(p["valid_froms"], ts) - 1, 0)]["price"]


# ----------------------------------------------------------------------------
# Users (each user = list of attribute versions -> SCD2 source of truth)
# ----------------------------------------------------------------------------
def new_email(first, last, uid_num):
    return f"{first}.{last}{uid_num}@{random.choice(EMAIL_DOMAINS)}".lower().replace(" ", "")


def mutate_user(cur, uid_num):
    new = dict(cur)
    kinds = random.sample(["address", "tier", "email", "opt_in"], k=random.choice([1, 1, 2]))
    if "address" in kinds:
        country = cur["country"] if random.random() < 0.7 else random.choices(COUNTRIES, COUNTRY_WEIGHTS)[0]
        city = random.choice([c for c in CITIES[country] if c != cur["city"]])
        new["country"], new["city"] = country, city
    if "tier" in kinds:
        idx = LOYALTY_TIERS.index(cur["loyalty_tier"])
        idx += 1 if idx == 0 else -1 if idx == len(LOYALTY_TIERS) - 1 else random.choice([-1, 1])
        new["loyalty_tier"] = LOYALTY_TIERS[idx]
    if "email" in kinds:
        new["email"] = new_email(cur["first_name"], cur["last_name"], f"{uid_num}{random.randint(1, 99)}")
    if "opt_in" in kinds:
        new["marketing_opt_in"] = not cur["marketing_opt_in"]
    return new


def build_users():
    users = {}
    for i in range(1, NUM_USERS + 1):
        uid = f"user_{i}"
        if random.random() < NEW_USER_IN_WINDOW_RATE:
            signup = random_dt(START, END - timedelta(days=1))
        else:
            signup = START - timedelta(days=random.randint(1, 720), seconds=random.randint(0, 86_399))
        country = random.choices(COUNTRIES, COUNTRY_WEIGHTS)[0]
        first, last = random.choice(FIRST_NAMES), random.choice(LAST_NAMES)
        cur = {
            "email": new_email(first, last, i), "first_name": first, "last_name": last,
            "country": country, "city": random.choice(CITIES[country]),
            "loyalty_tier": random.choices(LOYALTY_TIERS, [60, 25, 10, 5])[0],
            "marketing_opt_in": random.random() < 0.5,
        }
        versions = [{"valid_from": signup, **cur}]
        lo = max(signup, START) + timedelta(hours=1)
        if lo < END:
            n_changes = random.choices([0, 1, 2, 3], [55, 25, 15, 5])[0]
            for ts in sorted(random_dt(lo, END) for _ in range(n_changes)):
                cur = mutate_user(cur, i)
                versions.append({"valid_from": ts, **cur})
        deleted_at = None
        if random.random() < DELETE_USER_RATE:
            d_lo = versions[-1]["valid_from"] + timedelta(hours=1)
            if d_lo < END:
                deleted_at = random_dt(d_lo, END)
        users[uid] = {"signup": signup, "deleted_at": deleted_at, "versions": versions,
                      "valid_froms": [v["valid_from"] for v in versions]}
    return users


def user_at(uid, ts):
    u = USERS[uid]
    return u["versions"][max(bisect_right(u["valid_froms"], ts) - 1, 0)]


def pick_user(t):
    """Activity-skewed user pick, restricted to users that exist (signed up, not deleted) at time t."""
    for _ in range(30):
        uid = random.choices(USER_IDS, cum_weights=USER_CUM_WEIGHTS)[0]
        u = USERS[uid]
        if u["signup"] <= t and (u["deleted_at"] is None or u["deleted_at"] > t):
            return uid
    return None


def init_world():
    global START, END, PRODUCTS, PRODUCT_IDS, USERS, USER_IDS, USER_CUM_WEIGHTS
    END = datetime.now(timezone.utc)
    START = END - timedelta(days=DAYS_BACK)
    AUDIT.clear()
    GROUND_TRUTH.clear()
    PRODUCTS = build_products()
    PRODUCT_IDS = list(PRODUCTS)
    USERS = build_users()
    USER_IDS = list(USERS)
    ranks = list(range(1, len(USER_IDS) + 1))
    random.shuffle(ranks)
    USER_CUM_WEIGHTS = list(accumulate(1 / (r ** 0.7) for r in ranks))   # a few power users, long tail


# ----------------------------------------------------------------------------
# Sessions / events
# ----------------------------------------------------------------------------
def make_event(sess, event_type, t, product_id=None, quantity=None, order_id=None, search_query=None):
    product = PRODUCTS[product_id] if product_id else None
    if sess["logged_in"] and sess["user_id"] in USERS:
        country = user_at(sess["user_id"], t)["country"]   # point-in-time country (validates SCD2 joins)
    else:
        country = sess["country"]
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": sess["user_id"],
        "is_logged_in": sess["logged_in"],
        "session_id": sess["session_id"],
        "event_time": t,
        "event_type": event_type,
        "product_id": product_id,
        "category": product["category"] if product else None,
        "device": sess["device"],
        "country": country,
        "currency": "EUR",
        "price": price_at(product_id, t) if product else None,
        "quantity": quantity,
        "order_id": order_id,
        "search_query": search_query,
        "ip_address": sess["ip"],
        "user_agent": sess["ua"],
        "traffic_source": sess["traffic_source"],
        "app_version": sess["app_version"],
        "ingest_time": None,
    }


def shopper_flow(sess, start):
    t = start
    events = []

    def add(event_type, **kw):
        nonlocal t
        t += timedelta(seconds=random.randint(5, 120))
        events.append(make_event(sess, event_type, t, **kw))

    add("page_view")
    if random.random() < 0.30:
        add("search", search_query=random.choice(SEARCH_TERMS))
    if random.random() < 0.70:
        viewed = random.sample(PRODUCT_IDS, k=random.choices([1, 2, 3], [60, 30, 10])[0])
        cart = {}
        for pid in viewed:
            add("product_view", product_id=pid)
            if random.random() < 0.40:
                cart[pid] = random.randint(1, 5)
                add("add_to_cart", product_id=pid, quantity=cart[pid])
        for pid in list(cart):
            if random.random() < 0.15:
                add("remove_from_cart", product_id=pid, quantity=cart.pop(pid))
        if cart and sess["logged_in"] and random.random() < 0.50:       # anonymous users can't check out
            for pid, q in cart.items():
                add("checkout", product_id=pid, quantity=q)
            if random.random() < 0.60:
                order_id = f"ord_{uuid.uuid4().hex[:10]}"
                for pid, q in cart.items():
                    add("purchase", product_id=pid, quantity=q, order_id=order_id)
    return events


def bot_flow(sess, start):
    t, events = start, []
    for _ in range(random.randint(20, 80)):
        t += timedelta(seconds=random.uniform(0.5, 3))
        et = random.choice(["page_view", "product_view"])
        events.append(make_event(sess, et, t, product_id=random.choice(PRODUCT_IDS) if et == "product_view" else None))
    return events


def generate_session(n):
    r = random.random()
    start = random_dt(START, END - timedelta(hours=1))
    device = random.choice(DEVICES)
    sess = {
        "session_id": f"session_{n}", "user_id": None, "logged_in": False, "tag": None,
        "device": device, "country": random.choices(COUNTRIES, COUNTRY_WEIGHTS)[0],
        "ip": random_ip(), "ua": random.choice(USER_AGENTS[device]),
        "traffic_source": random.choice(TRAFFIC_SOURCES),
        "app_version": random.choice(APP_VERSIONS) if device != "desktop" else None,
    }
    if r < BOT_SESSION_RATE:
        sess.update(tag="bot_traffic", device="desktop", ua=random.choice(BOT_AGENTS), app_version=None)
        events = bot_flow(sess, start)
    else:
        if random.random() >= ANON_SESSION_RATE:
            uid = pick_user(start)
            if uid:
                sess.update(user_id=uid, logged_in=True)
        events = shopper_flow(sess, start)

    if sess["tag"]:
        for e in events:
            tag("event", e["event_id"], sess["tag"])
    return events


# ----------------------------------------------------------------------------
# Field-level event anomalies (applied to the serialised record)
# ----------------------------------------------------------------------------
def a_null_required(rec):
    fields = ["session_id", "event_time", "event_type"]
    if rec["event_type"] in PRODUCT_EVENTS and rec["product_id"]:
        fields.append("product_id")
    if rec["is_logged_in"] and rec.get("user_id"):
        fields.append("user_id")
    rec[random.choice(fields)] = None
    return True


def a_recoverable_nulls(rec):
    fields = ["device", "country", "currency"] + (["category", "price"] if rec["product_id"] else [])
    for f in random.sample(fields, k=random.randint(1, 2)):
        rec[f] = None
    return True


def a_placeholders(rec):
    fields = [f for f in ("device", "country", "category") if isinstance(rec[f], str)]
    if not fields:
        return False
    rec[random.choice(fields)] = random.choice(PLACEHOLDERS)
    return True


def a_case_whitespace(rec):
    fields = [f for f in ("event_type", "device", "category", "product_id") if isinstance(rec[f], str) and rec[f].strip()]
    if not fields:
        return False
    f = random.choice(fields)
    v = rec[f]
    rec[f] = random.choice([v.upper(), v.title(), f"  {v} ", f"{v} "])
    return True


def a_timestamp_format(rec):
    v = ts_variant(rec["event_time"])
    if v is None:
        return False
    rec["event_time"] = v
    return True


def a_future_timestamp(rec):
    rec["event_time"] = (END + timedelta(days=random.randint(1, 365), seconds=random.randint(0, 86_399))).isoformat()
    return True


def a_bad_quantity(rec):
    if rec["quantity"] is None:
        return False
    rec["quantity"] = random.choice([0, -1, -3, 999, "3", 2.0, "two"])
    return True


EVENT_ANOMALIES = [
    ("recoverable_nulls", a_recoverable_nulls), ("placeholder_strings", a_placeholders),
    ("case_whitespace", a_case_whitespace), ("bad_quantity", a_bad_quantity),
    ("timestamp_format", a_timestamp_format), ("future_timestamp", a_future_timestamp),
    ("null_required_field", a_null_required),
]


def apply_anomalies(rec, funcs, rates, entity, key):
    for name, fn in funcs:
        if random.random() < rates[name] and fn(rec):
            tag(entity, key, name)


# ----------------------------------------------------------------------------
# Event pipeline: generate -> ingest metadata -> duplicates -> split into files -> dirty -> write
# ----------------------------------------------------------------------------
def serialize_event(e):
    key = e["event_id"]
    rec = dict(e)
    rec["event_time"] = e["event_time"].isoformat()
    rec["ingest_time"] = e["ingest_time"].isoformat()
    apply_anomalies(rec, EVENT_ANOMALIES, EVENT_ANOMALY_RATES, "event", key)
    return json.dumps(rec, ensure_ascii=False)


def generate_clickstream(run_id):
    file_sizes = [random.randint(MIN_RECORDS_PER_FILE, MAX_RECORDS_PER_FILE) for _ in range(NUM_FILES)]
    total_needed = sum(file_sizes)

    events, session_number = [], 0
    while len(events) < total_needed:
        session_number += 1
        events.extend(generate_session(session_number))

    for e in events:
        e["ingest_time"], late = ingest_after(e["event_time"], 120, LATE_ARRIVAL_RATE, 72)
        if late:
            tag("event", e["event_id"], "late_arrival")

    dups = []
    for e in events:
        if random.random() < DUPLICATE_RATE:
            d = copy(e)
            d["ingest_time"] = min(e["ingest_time"] + timedelta(seconds=random.uniform(1, 7_200)), END)
            dups.append(d)
            tag("event", e["event_id"], "duplicate")
    events.extend(dups)

    # near-ingestion order with a little jitter -> mild out-of-order within and across files
    events.sort(key=lambda e: e["ingest_time"] + timedelta(seconds=random.uniform(0, 300)))

    start = 0
    for n, size in enumerate(file_sizes, 1):
        batch = events[start:] if n == NUM_FILES else events[start:start + size]
        start += size
        out = EVENTS_DIR / f"clickstream_{run_id}_{n}.json"
        with out.open("w", encoding="utf-8") as f:
            for e in batch:
                f.write(serialize_event(e) + "\n")
        print(f"Generated {len(batch):,} events: {out}")
    print(f"Total event rows: {len(events):,} across {NUM_FILES} files ({session_number:,} sessions)")


# ----------------------------------------------------------------------------
# User CDC feed (source for SCD2 dim_user)
# ----------------------------------------------------------------------------
def cdc_row(uid, version, op, updated_at):
    ingested_at, _ = ingest_after(updated_at, 300, 0, 48)
    row = {
        "user_id": uid, "email": version["email"], "first_name": version["first_name"],
        "last_name": version["last_name"], "country": version["country"], "city": version["city"],
        "loyalty_tier": version["loyalty_tier"], "marketing_opt_in": version["marketing_opt_in"],
        "op": op, "updated_at": updated_at, "ingested_at": ingested_at,
    }
    return row


def cdc_key(row):
    return f"{row['user_id']}|{row['updated_at'].isoformat()}|{row['op']}"


def c_null_email(row):
    row["email"] = None
    return True


def c_email_format(row):
    if not row["email"]:
        return False
    row["email"] = random.choice([row["email"].upper(), f"  {row['email']} ", row["email"].title()])
    return True


def c_tier_case(row):
    row["loyalty_tier"] = random.choice([row["loyalty_tier"].upper(), row["loyalty_tier"].title()])
    return True


def c_timestamp_format(row):
    v = ts_variant(row["updated_at"])
    if v is None:
        return False
    row["updated_at"] = v
    return True


CDC_ANOMALIES = [
    ("null_email", c_null_email), ("email_format", c_email_format),
    ("tier_case", c_tier_case), ("timestamp_format", c_timestamp_format),
]


def generate_user_cdc(run_id):
    rows = []
    for uid, u in USERS.items():
        versions = u["versions"]
        for k, v in enumerate(versions):
            rows.append(cdc_row(uid, v, "I" if k == 0 else "U", v["valid_from"]))
        if u["deleted_at"]:
            rows.append(cdc_row(uid, versions[-1], "D", u["deleted_at"]))
        elif random.random() < NOOP_UPDATE_RATE:
            ts = versions[-1]["valid_from"] + timedelta(minutes=random.randint(10, 120))
            if ts < END:
                row = cdc_row(uid, versions[-1], "U", ts)    # same attributes -> must not create an SCD2 version
                rows.append(row)
                tag("user_cdc", cdc_key(row), "noop_update")

    dups = []
    for r in rows:
        if random.random() < CDC_DUPLICATE_RATE:
            d = copy(r)
            d["ingested_at"] = min(r["ingested_at"] + timedelta(seconds=random.uniform(1, 3_600)), END)
            dups.append(d)
            tag("user_cdc", cdc_key(r), "duplicate")
    rows.extend(dups)
    rows.sort(key=lambda r: r["ingested_at"] + timedelta(seconds=random.uniform(0, 600)))

    buckets = [[] for _ in range(NUM_USER_CDC_FILES)]
    for r in rows:
        idx = min(int((r["ingested_at"] - START) / (END - START) * NUM_USER_CDC_FILES), NUM_USER_CDC_FILES - 1)
        buckets[idx].append(r)

    for n, batch in enumerate(buckets, 1):
        out = USERS_DIR / f"users_cdc_{run_id}_{n}.json"
        with out.open("w", encoding="utf-8") as f:
            for r in batch:
                key = cdc_key(r)
                rec = dict(r)
                rec["updated_at"] = r["updated_at"].isoformat()
                rec["ingested_at"] = r["ingested_at"].isoformat()
                apply_anomalies(rec, CDC_ANOMALIES, CDC_ANOMALY_RATES, "user_cdc", key)
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        print(f"Generated {len(batch):,} user CDC rows: {out}")
    n_changed = sum(len(u["versions"]) > 1 for u in USERS.values())
    n_deleted = sum(u["deleted_at"] is not None for u in USERS.values())
    n_versions = sum(len(u["versions"]) for u in USERS.values())
    print(f"Users: {len(USERS)} | with history changes: {n_changed} | deleted: {n_deleted} | "
          f"expected SCD2 rows: {n_versions}")


# ----------------------------------------------------------------------------
# Reference + audit outputs
# ----------------------------------------------------------------------------
def write_products(run_id):
    out = REF_DIR / f"products_{run_id}.csv"
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["product_id", "product_name", "category", "brand", "unit_price", "currency",
                    "valid_from", "valid_to", "is_current"])
        for pid, p in PRODUCTS.items():
            h = p["history"]
            for i, row in enumerate(h):
                last = i == len(h) - 1
                w.writerow([pid, p["name"], p["category"], p["brand"], row["price"], "EUR",
                            row["valid_from"].isoformat(), "" if last else h[i + 1]["valid_from"].isoformat(),
                            str(last).lower()])
    print(f"Wrote product catalog: {out}")


def write_audit(run_id):
    with (AUDIT_DIR / f"anomaly_ground_truth_{run_id}.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["entity", "record_key", "anomaly"])
        w.writerows(GROUND_TRUTH)
    with (AUDIT_DIR / f"anomaly_summary_{run_id}.json").open("w", encoding="utf-8") as f:
        json.dump(dict(sorted(AUDIT.items())), f, indent=2)
    print("\nInjected anomalies (ground truth in data/audit/):")
    for k, v in sorted(AUDIT.items()):
        print(f"  {k:<40} {v:>6,}")


def main():
    if SEED is not None:
        random.seed(SEED)
    for d in (EVENTS_DIR, USERS_DIR, REF_DIR, AUDIT_DIR):
        d.mkdir(parents=True, exist_ok=True)
    init_world()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    write_products(run_id)
    generate_user_cdc(run_id)
    generate_clickstream(run_id)
    write_audit(run_id)


if __name__ == "__main__":
    main()
