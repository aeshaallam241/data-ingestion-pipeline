import csv
import functools
import getpass
import json
import logging
import os
import random
import shutil
import sys
import time
import uuid
from datetime import datetime

import psycopg2
import requests

PG = dict(
    host=os.environ.get("PG_HOST", "localhost"),
    port=int(os.environ.get("PG_PORT", "5432")),
    dbname=os.environ.get("PG_DB", "postgres"),
    user=os.environ.get("PG_USER", "postgres"),
)
BRONZE = os.environ.get("BRONZE_DIR", "bronze")
STATE_FILE = "state/watermarks.json"
TABLES = ["customers", "products", "orders"]
API_URL = "https://dummyjson.com/products"
CSV_FILE = "data/sample/shipments.csv"
RUN_ID = uuid.uuid4().hex[:8]
FAILED = []

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s %(levelname)s [{RUN_ID}] %(message)s",
    handlers=[logging.FileHandler("logs/ingestion.log", encoding="utf-8"),
              logging.StreamHandler()],
)
log = logging.getLogger("ingestion")


def retry(times=3, delay=2):
    def deco(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            for attempt in range(1, times + 1):
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    log.warning("%s failed (attempt %s/%s): %s",
                                fn.__name__, attempt, times, e)
                    if attempt == times:
                        raise
                    time.sleep(delay)
        return wrapper
    return deco


def record(source, status, rows=0, seconds=0.0, error=""):
    entry = dict(run_id=RUN_ID, source=source, status=status, rows=rows,
                 seconds=round(seconds, 2), error=error,
                 timestamp=datetime.now().isoformat(timespec="seconds"))
    with open("logs/run_history.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")


def save_bronze(source, name, ext, writer):
    day = datetime.now().strftime("%Y-%m-%d")
    folder = os.path.join(BRONZE, source, name, day)
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, f"{name}_{RUN_ID}.{ext}")
    writer(path)
    return path


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}


def save_state(state):
    os.makedirs("state", exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


@retry()
def ingest_table(table, state):
    last = state.get(table, "1970-01-01 00:00:00")
    conn = psycopg2.connect(**PG)
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table} WHERE updated_at > %s ORDER BY updated_at",
                (last,))
    cols = [c[0] for c in cur.description]
    rows = cur.fetchall()
    conn.close()
    if not rows:
        log.info("%s: no new rows", table)
        return 0

    def write(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(cols)
            w.writerows(rows)

    path = save_bronze("postgres", table, "csv", write)
    state[table] = str(max(r[cols.index("updated_at")] for r in rows))
    log.info("%s: saved %s rows to %s", table, len(rows), path)
    return len(rows)


@retry()
def fetch_page(skip, limit):
    r = requests.get(API_URL, params={"limit": limit, "skip": skip}, timeout=30)
    r.raise_for_status()
    return r.json()


def ingest_api():
    pages, skip, limit = [], 0, 30
    while True:
        data = fetch_page(skip, limit)
        if not data.get("products"):
            break
        pages.append(data)
        skip += limit
        if skip >= data.get("total", 0):
            break

    def write(path):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(pages, f)

    path = save_bronze("rest_api", "products", "json", write)
    count = sum(len(p["products"]) for p in pages)
    log.info("api: saved %s products (%s pages) to %s", count, len(pages), path)
    return count


def make_sample_csv():
    os.makedirs(os.path.dirname(CSV_FILE), exist_ok=True)
    carriers = ["DHL", "Aramex", "FedEx"]
    rows = []
    for i in range(1, 301):
        order_id = 5000 if i % 60 == 0 else i
        carrier = "" if i % 40 == 0 else carriers[i % 3]
        days = -2 if i % 55 == 0 else (i % 7) + 1
        rows.append([i, order_id, carrier, "2026-10-%02d" % ((i % 28) + 1), days])
    rows.append(rows[0])
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["shipment_id", "order_id", "carrier", "shipped_date",
                    "delivery_days"])
        w.writerows(rows)


def ingest_csv():
    if not os.path.exists(CSV_FILE):
        make_sample_csv()
    path = save_bronze("csv_files", "shipments", "csv",
                       lambda p: shutil.copyfile(CSV_FILE, p))
    with open(CSV_FILE, encoding="utf-8") as f:
        count = sum(1 for _ in f) - 1
    log.info("csv: copied %s rows to %s", count, path)
    return count


def add_orders(n=50):
    conn = psycopg2.connect(**PG)
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(MAX(order_id), 0) FROM orders")
    start = cur.fetchone()[0]
    for i in range(1, n + 1):
        cur.execute(
            "INSERT INTO orders (order_id, customer_id, product_id, quantity, "
            "amount, order_date) VALUES (%s,%s,%s,%s,%s,CURRENT_DATE)",
            (start + i, random.randint(1, 200), random.randint(1, 50),
             random.randint(1, 5), round(random.uniform(5, 500), 2)))
    conn.commit()
    conn.close()
    log.info("added %s new orders", n)


def run(source, fn):
    start = time.time()
    try:
        rows = fn()
        record(source, "success", rows, time.time() - start)
    except Exception as e:
        log.error("%s failed: %s", source, e)
        record(source, "failed", 0, time.time() - start, str(e))
        FAILED.append(source)


if __name__ == "__main__":
    PG["password"] = os.environ.get("PG_PASSWORD") or getpass.getpass(
        "Postgres password: ")
    if len(sys.argv) > 1 and sys.argv[1] == "add":
        add_orders(50)
    else:
        state = load_state()
        for t in TABLES:
            run(f"postgres.{t}", lambda t=t: ingest_table(t, state))
        save_state(state)
        run("api.products", ingest_api)
        run("csv.shipments", ingest_csv)
        if FAILED:
            log.error("failed sources: %s", ", ".join(FAILED))
            sys.exit(1)