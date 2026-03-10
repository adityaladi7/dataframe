"""
seed_database.py
Creates a realistic SQLite database simulating a sales pipeline.
Intentionally injects data quality issues for the framework to catch.
"""

import sqlite3
import random
from datetime import datetime, timedelta
import os

DB_PATH = "data/pipeline.db"

def seed():
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # ── Drop & recreate tables ──────────────────────────────────────────────
    cur.executescript("""
        DROP TABLE IF EXISTS orders;
        DROP TABLE IF EXISTS customers;

        CREATE TABLE customers (
            customer_id   TEXT PRIMARY KEY,
            name          TEXT,
            email         TEXT,
            country       TEXT,
            signup_date   TEXT
        );

        CREATE TABLE orders (
            order_id      TEXT,
            customer_id   TEXT,
            amount        REAL,
            currency      TEXT,
            status        TEXT,
            order_date    TEXT,
            updated_at    TEXT
        );
    """)

    # ── Seed customers (clean) ──────────────────────────────────────────────
    countries = ["IN", "US", "JP", "DE", "GB"]
    customers = []
    for i in range(1, 101):
        cid = f"C{i:04d}"
        signup = (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 600))).strftime("%Y-%m-%d")
        customers.append((cid, f"Customer {i}", f"user{i}@example.com", random.choice(countries), signup))

    cur.executemany("INSERT INTO customers VALUES (?,?,?,?,?)", customers)

    # ── Seed orders ─────────────────────────────────────────────────────────
    statuses = ["completed", "pending", "refunded", "cancelled"]
    orders = []
    now = datetime.now()

    for i in range(1, 501):
        oid = f"ORD{i:05d}"
        cid = f"C{random.randint(1,100):04d}"
        amount = round(random.uniform(10, 5000), 2)
        currency = "USD"
        status = random.choice(statuses)
        order_dt = (now - timedelta(days=random.randint(0, 90))).strftime("%Y-%m-%d %H:%M:%S")
        updated = order_dt
        orders.append((oid, cid, amount, currency, status, order_dt, updated))

    # ── Inject data quality issues ──────────────────────────────────────────

    # 1. DUPLICATES — repeat 15 order rows exactly
    orders += random.sample(orders, 15)

    # 2. NULL amounts — 12 orders with missing amount
    for i in range(12):
        idx = random.randint(0, len(orders)-1)
        o = list(orders[idx])
        o[2] = None
        orders[idx] = tuple(o)

    # 3. WRONG DATA TYPE — amount stored as string in 8 rows
    for i in range(8):
        idx = random.randint(0, len(orders)-1)
        o = list(orders[idx])
        o[2] = "N/A"
        orders[idx] = tuple(o)

    # 4. OUT-OF-RANGE amounts — negative or absurdly large
    for _ in range(10):
        idx = random.randint(0, len(orders)-1)
        o = list(orders[idx])
        o[2] = round(random.choice([-50.0, -200.0, 99999.99, 150000.0]), 2)
        orders[idx] = tuple(o)

    # 5. STALE DATA — 20 orders with updated_at older than 60 days
    stale_dt = (now - timedelta(days=random.randint(61, 200))).strftime("%Y-%m-%d %H:%M:%S")
    for _ in range(20):
        idx = random.randint(0, len(orders)-1)
        o = list(orders[idx])
        o[6] = stale_dt
        orders[idx] = tuple(o)

    # 6. INVALID STATUS — a few garbage values
    for _ in range(6):
        idx = random.randint(0, len(orders)-1)
        o = list(orders[idx])
        o[4] = random.choice(["UNKNOWN", "error", "NULL", ""])
        orders[idx] = tuple(o)

    cur.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?,?)", orders)
    conn.commit()
    conn.close()
    print(f"✅  Database seeded → {DB_PATH}")
    print(f"    customers : 100 rows")
    print(f"    orders    : {len(orders)} rows (with injected issues)")

if __name__ == "__main__":
    seed()
