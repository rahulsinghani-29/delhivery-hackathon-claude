"""
Load production CSV data into commerce_ai.db.

Usage:
    python -m scripts.load_production_data --csv <path> [--db <path>]

The CSV is expected to have these columns (Delhivery real data format):
    waybill_number, hq_client_name, client_type, category_name, line_item_name,
    order_amt, origin_state, destination_city, buyer_id, buyer_rto_history_pct,
    final_status, hudi_payment_method, analytics_payment_method,
    is_rto, was_adfix_corrected, manifest_latency_days
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import logging
import re
import sqlite3
import sys
import time
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BATCH = 50_000


def slugify(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower().strip()).strip("_")[:40]


def merchant_id(name: str) -> str:
    h = hashlib.md5(name.encode()).hexdigest()[:8].upper()
    return f"M-{h}"


def price_band(amt: float) -> str:
    if amt < 300:
        return "low"
    if amt < 1000:
        return "mid"
    if amt < 3000:
        return "high"
    return "premium"


def payment_mode(raw: str) -> str:
    r = raw.strip().lower()
    if "cod" in r:
        return "COD"
    return "Prepaid"


def delivery_outcome(is_rto: str, final_status: str) -> str:
    if is_rto == "1":
        return "rto"
    fs = final_status.strip().lower()
    if "deliver" in fs:
        return "delivered"
    if "cancel" in fs:
        return "cancelled"
    return "pending"


def address_quality(was_adfix: str, rto_pct: str) -> float:
    try:
        rto = float(rto_pct)
    except (ValueError, TypeError):
        rto = 0.0
    # High historical RTO rate → low address quality
    q = max(0.0, 1.0 - rto)
    # Adfix corrected → slightly lower quality (needed correction)
    if was_adfix == "1":
        q = min(q, 0.7)
    return round(q, 3)


def init_schema(db: sqlite3.Connection) -> None:
    """Ensure tables exist — matches the existing schema."""
    db.executescript("""
        CREATE TABLE IF NOT EXISTS merchants (
            merchant_id TEXT PRIMARY KEY,
            name TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS warehouse_nodes (
            node_id TEXT PRIMARY KEY,
            merchant_id TEXT,
            city TEXT,
            state TEXT,
            pincode TEXT,
            is_active BOOLEAN DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS orders (
            order_id TEXT PRIMARY KEY,
            merchant_id TEXT,
            customer_ucid TEXT,
            category TEXT,
            price_band TEXT,
            payment_mode TEXT,
            origin_node TEXT,
            destination_pincode TEXT,
            destination_cluster TEXT,
            address_quality REAL,
            rto_score REAL DEFAULT 0.5,
            delivery_outcome TEXT,
            shipping_mode TEXT DEFAULT 'standard',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS merchant_permissions (
            merchant_id TEXT PRIMARY KEY,
            intervention_type TEXT DEFAULT 'all',
            is_enabled BOOLEAN DEFAULT 1,
            daily_cap INTEGER DEFAULT 5,
            hourly_cap INTEGER DEFAULT 2,
            auto_cancel_enabled BOOLEAN DEFAULT 0,
            auto_cancel_threshold REAL DEFAULT 0.9,
            express_upgrade_enabled BOOLEAN DEFAULT 0,
            impulse_categories TEXT DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_orders_merchant ON orders (merchant_id);
        CREATE INDEX IF NOT EXISTS idx_orders_outcome ON orders (delivery_outcome);
    """)
    db.commit()


def load(csv_path: str, db_path: str) -> None:
    t0 = time.time()
    db = sqlite3.connect(db_path, isolation_level=None)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA cache_size=-131072")  # 128 MB page cache

    init_schema(db)

    # Disable FK enforcement during bulk load — we control referential integrity manually.
    db.execute("PRAGMA foreign_keys=OFF")

    # ── Wipe existing data so we start clean ──
    log.info("Clearing existing data…")
    for table in ("communication_logs", "interventions", "suppressed_recommendations",
                  "orders", "warehouse_nodes", "merchant_permissions", "merchants"):
        try:
            db.execute(f"DELETE FROM {table}")
        except Exception:
            pass  # table may not exist yet
    db.commit()

    # ── Pass 1: collect merchants + origin states (low memory) ──
    merchants: dict[str, str] = {}   # name → merchant_id
    origin_states: set[str] = set()
    merchant_order_counts: dict[str, int] = {}

    log.info("Pass 1: collecting merchants…")
    with open(csv_path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i % 1_000_000 == 0 and i > 0:
                log.info(f"  {i:,} rows | {len(merchants):,} merchants")
            name = row["hq_client_name"].strip() or "UNKNOWN"
            if name not in merchants:
                merchants[name] = merchant_id(name)
            origin_states.add(slugify(row["origin_state"]) or "unknown")

    log.info(f"Pass 1 done — {len(merchants):,} merchants, {len(origin_states)} origin states")

    # ── Insert merchants (FK parents before orders) ──
    log.info(f"Inserting {len(merchants):,} merchants…")
    db.execute("BEGIN")
    db.executemany(
        "INSERT OR IGNORE INTO merchants (merchant_id, name) VALUES (?,?)",
        [(mid, name) for name, mid in merchants.items()],
    )
    db.execute("COMMIT")

    # ── Insert stub warehouse nodes (one per origin state) ──
    log.info(f"Inserting {len(origin_states)} warehouse node stubs…")
    db.execute("BEGIN")
    db.executemany(
        "INSERT OR IGNORE INTO warehouse_nodes (node_id, merchant_id, city, state, pincode) VALUES (?,?,?,?,?)",
        [(s, next(iter(merchants.values())), s, s, "000000") for s in origin_states],
    )
    db.execute("COMMIT")

    # ── Pass 2: stream CSV again, insert orders ──
    log.info("Pass 2: streaming orders…")
    seen_waybills: set[str] = set()
    order_batch: list[tuple] = []
    rows_read = 0

    def flush_orders() -> None:
        db.execute("BEGIN")
        db.executemany(
            """INSERT OR IGNORE INTO orders
               (order_id, merchant_id, customer_ucid, category, price_band,
                payment_mode, origin_node, destination_pincode, destination_cluster,
                address_quality, rto_score, delivery_outcome, shipping_mode, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,0.5,?,'standard',CURRENT_TIMESTAMP)""",
            order_batch,
        )
        db.execute("COMMIT")
        order_batch.clear()

    with open(csv_path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows_read += 1
            if rows_read % 1_000_000 == 0:
                elapsed = time.time() - t0
                log.info(f"  {rows_read:,} rows | {len(seen_waybills):,} orders inserted | {elapsed:.0f}s")

            wbn = row["waybill_number"].strip()
            if not wbn or wbn in seen_waybills:
                continue
            seen_waybills.add(wbn)

            name = row["hq_client_name"].strip() or "UNKNOWN"
            mid = merchants.get(name, list(merchants.values())[0])
            merchant_order_counts[mid] = merchant_order_counts.get(mid, 0) + 1

            origin = slugify(row["origin_state"]) or "unknown"
            dest = slugify(row["destination_city"]) or "unknown"

            try:
                amt = float(row["order_amt"] or 0)
            except ValueError:
                amt = 0.0

            order_batch.append((
                wbn, mid,
                row["buyer_id"].strip() or f"anon-{wbn}",
                row["category_name"].strip() or "general",
                price_band(amt),
                payment_mode(row["hudi_payment_method"]),
                origin, dest, dest,
                address_quality(row["was_adfix_corrected"], row["buyer_rto_history_pct"]),
                delivery_outcome(row["is_rto"], row["final_status"]),
            ))

            if len(order_batch) >= BATCH:
                flush_orders()

    if order_batch:
        flush_orders()

    log.info(f"Pass 2 done — {len(seen_waybills):,} orders inserted")

    # ── Insert merchant_permissions ──
    log.info("Setting up merchant permissions…")
    db.execute("BEGIN")
    db.executemany(
        """INSERT OR IGNORE INTO merchant_permissions
           (merchant_id, is_enabled, daily_cap, hourly_cap)
           VALUES (?,1,10,3)""",
        [(mid,) for mid in merchants.values()],
    )
    db.execute("COMMIT")

    # ── Stats ──
    top10 = sorted(merchant_order_counts.items(), key=lambda x: -x[1])[:10]
    log.info("\n── Top 10 merchants by order count ──")
    for mid, cnt in top10:
        name = next(n for n, m in merchants.items() if m == mid)
        log.info(f"  {mid}  {cnt:>8,}  {name}")

    elapsed = time.time() - t0
    log.info(f"\nDone in {elapsed:.1f}s — {len(seen_waybills):,} orders, {len(merchants):,} merchants")

    # Write top merchant to a file so the frontend can use it
    top_mid, top_cnt = top10[0]
    top_name = next(n for n, m in merchants.items() if m == top_mid)
    out = Path(db_path).parent / "top_merchant.txt"
    out.write_text(f"{top_mid}\t{top_name}\t{top_cnt}\n")
    log.info(f"Top merchant written to {out}")

    db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True, help="Path to production CSV")
    parser.add_argument("--db", default="commerce_ai.db", help="Path to SQLite DB")
    args = parser.parse_args()
    load(args.csv, args.db)
