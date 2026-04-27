"""
ETL: Transform raw Delhivery CSV -> Postgres.

Usage:
    python -m scripts.load_postgres \
        --csv /data/input.csv \
        --dsn "postgresql://commerce:commerce@postgres:5432/commerce_ai"
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import logging
import sys
import time
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# City normalization
# ---------------------------------------------------------------------------
CITY_ALIASES = {}
for _variants, _canonical in [
    (["bangalore", "BANGALORE", "Bangalore", "bengaluru", "BENGALURU"], "Bengaluru"),
    (["mumbai", "MUMBAI"], "Mumbai"),
    (["delhi", "DELHI", "new delhi", "New Delhi", "NEW DELHI"], "Delhi"),
    (["chennai", "CHENNAI"], "Chennai"),
    (["hyderabad", "HYDERABAD"], "Hyderabad"),
    (["kolkata", "KOLKATA"], "Kolkata"),
    (["pune", "PUNE"], "Pune"),
    (["gurgaon", "Gurgaon", "GURGAON", "gurugram"], "Gurugram"),
    (["noida", "NOIDA"], "Noida"),
    (["thane", "THANE"], "Thane"),
    (["ahmedabad", "AHMEDABAD"], "Ahmedabad"),
]:
    for v in _variants:
        CITY_ALIASES[v] = _canonical


def normalize_city(raw: str) -> str:
    s = raw.strip()
    return CITY_ALIASES.get(s, s.title() if s else "Unknown")


# ---------------------------------------------------------------------------
# Category inference from line_item_name
# ---------------------------------------------------------------------------
CATEGORY_KEYWORDS = {
    "fashion": ["shirt", "tshirt", "t-shirt", "dress", "saree", "sari", "kurta",
                "kurti", "jeans", "trouser", "legging", "top", "blouse", "jacket",
                "hoodie", "sweater", "palazzo", "dupatta", "lehnga", "lehenga"],
    "beauty": ["cream", "serum", "facewash", "face wash", "moistur", "sunscreen",
               "lipstick", "mascara", "foundation", "skincare", "shampoo",
               "conditioner", "body lotion", "perfume", "scrub", "toner", "cleanser"],
    "jewellery": ["earring", "necklace", "bracelet", "ring", "pendant", "chain",
                  "bangle", "anklet", "jewel", "mangalsutra"],
    "food": ["achar", "pickle", "spice", "masala", "ghee", "honey", "tea",
             "coffee", "chocolate", "snack", "namkeen", "dry fruit"],
    "electronics": ["charger", "cable", "earphone", "headphone", "speaker",
                    "power bank", "adapter", "led", "trimmer"],
    "health": ["medicine", "ayurved", "capsule", "tablet", "syrup", "vitamin",
               "supplement", "herbal"],
    "home": ["bedsheet", "pillow", "curtain", "towel", "mat", "organizer",
             "candle", "decor", "frame", "lamp"],
    "footwear": ["shoe", "sandal", "slipper", "heel", "boot", "sneaker", "chappal"],
}


def infer_category(item_name: str) -> str:
    lower = item_name.lower()
    for cat, keywords in CATEGORY_KEYWORDS.items():
        for kw in keywords:
            if kw in lower:
                return cat
    return "general"


# ---------------------------------------------------------------------------
# Transform helpers
# ---------------------------------------------------------------------------

def merchant_id(name: str) -> str:
    h = hashlib.md5(name.encode()).hexdigest()[:8].upper()
    return f"M-{h}"


def price_band(amt: float) -> str:
    if amt < 300: return "low"
    if amt < 1000: return "mid"
    if amt < 3000: return "high"
    return "premium"


def payment_mode(hudi_tag: str, raw_method: str) -> str:
    tag = hudi_tag.strip().lower()
    if tag == "cod": return "COD"
    if tag == "pre-paid": return "Prepaid"
    method = raw_method.strip().lower()
    if "cod" in method or "cash" in method: return "COD"
    return "Prepaid"


def delivery_outcome(is_rto: str, final_status: str) -> str:
    if is_rto.strip() == "1": return "rto"
    fs = final_status.strip().lower()
    if "deliver" in fs: return "delivered"
    if "transit" in fs: return "in_transit"
    if "cancel" in fs: return "cancelled"
    return "pending"


def address_quality(was_adfix: str, rto_pct: str) -> float:
    try:
        rto = float(rto_pct)
    except (ValueError, TypeError):
        rto = 0.0
    q = max(0.0, 1.0 - rto)
    if was_adfix.strip() == "1":
        q = min(q, 0.7)
    return round(q, 3)


def make_order_id(merchant_name: str, row_index: int) -> str:
    raw = f"{merchant_name}:{row_index}"
    h = hashlib.sha256(raw.encode()).hexdigest()[:12]
    return f"ORD-{h.upper()}"


AMT_CAP = 50_000.0
BATCH_SIZE = 10_000


# ---------------------------------------------------------------------------
# Main ETL
# ---------------------------------------------------------------------------

def wait_for_postgres(dsn: str, retries: int = 30, delay: float = 2.0) -> None:
    """Wait for Postgres to accept connections."""
    for attempt in range(retries):
        try:
            conn = psycopg2.connect(dsn)
            conn.close()
            log.info("Postgres is ready.")
            return
        except psycopg2.OperationalError:
            if attempt < retries - 1:
                log.info("Waiting for Postgres... (%d/%d)", attempt + 1, retries)
                time.sleep(delay)
    log.error("Postgres not available after %d attempts", retries)
    sys.exit(1)


def apply_schema(cur, conn) -> None:
    schema_path = Path(__file__).resolve().parent.parent / "postgres_schema.sql"
    if schema_path.exists():
        log.info("Applying schema from %s", schema_path)
        cur.execute(schema_path.read_text())
        conn.commit()
    else:
        log.error("Schema file not found at %s", schema_path)
        sys.exit(1)


def create_materialized_views(cur, conn) -> None:
    """Create materialized views after data is loaded."""
    log.info("Creating materialized views...")

    views = {
        "mv_merchant_cohort_stats": """
            SELECT
                merchant_id, category, price_band, payment_mode,
                origin_state, destination_city,
                COUNT(*) AS order_count,
                SUM(CASE WHEN delivery_outcome = 'rto' THEN 1 ELSE 0 END) AS rto_count,
                ROUND(AVG(CASE WHEN delivery_outcome = 'rto' THEN 1.0 ELSE 0.0 END)::numeric, 4) AS rto_rate,
                ROUND(AVG(CASE WHEN delivery_outcome = 'delivered' THEN 1.0 ELSE 0.0 END)::numeric, 4) AS delivery_rate,
                ROUND(AVG(order_amount)::numeric, 2) AS avg_order_value,
                ROUND(AVG(address_quality)::numeric, 4) AS avg_address_quality
            FROM orders
            WHERE delivery_outcome IN ('delivered', 'rto')
            GROUP BY merchant_id, category, price_band, payment_mode, origin_state, destination_city
        """,
        "mv_peer_benchmarks": """
            SELECT
                category, price_band, payment_mode, origin_state, destination_city,
                COUNT(DISTINCT merchant_id) AS merchant_count,
                COUNT(*) AS total_orders,
                ROUND(AVG(CASE WHEN delivery_outcome = 'rto' THEN 1.0 ELSE 0.0 END)::numeric, 4) AS peer_rto_rate,
                ROUND(AVG(CASE WHEN delivery_outcome = 'delivered' THEN 1.0 ELSE 0.0 END)::numeric, 4) AS peer_delivery_rate,
                ROUND(AVG(order_amount)::numeric, 2) AS peer_avg_order_value,
                ROUND(STDDEV(CASE WHEN delivery_outcome = 'rto' THEN 1.0 ELSE 0.0 END)::numeric, 4) AS peer_rto_stddev
            FROM orders
            WHERE delivery_outcome IN ('delivered', 'rto')
            GROUP BY category, price_band, payment_mode, origin_state, destination_city
            HAVING COUNT(*) >= 10
        """,
        "mv_merchant_summary": """
            SELECT
                m.merchant_id, m.name, m.client_type,
                COUNT(o.order_id) AS total_orders,
                SUM(CASE WHEN o.delivery_outcome = 'rto' THEN 1 ELSE 0 END) AS rto_count,
                ROUND(AVG(CASE WHEN o.delivery_outcome = 'rto' THEN 1.0 ELSE 0.0 END)::numeric, 4) AS rto_rate,
                ROUND(AVG(o.order_amount)::numeric, 2) AS avg_order_value,
                COUNT(DISTINCT o.destination_city) AS destination_cities,
                COUNT(DISTINCT o.buyer_id) AS unique_buyers
            FROM merchants m
            LEFT JOIN orders o ON m.merchant_id = o.merchant_id
            GROUP BY m.merchant_id, m.name, m.client_type
        """,
        "mv_demand_map": """
            SELECT
                merchant_id, destination_city,
                COUNT(*) AS order_count,
                SUM(CASE WHEN delivery_outcome = 'rto' THEN 1 ELSE 0 END) AS rto_count,
                ROUND(100.0 * AVG(CASE WHEN delivery_outcome = 'rto' THEN 1.0 ELSE 0.0 END)::numeric, 1) AS rto_rate_pct
            FROM orders
            WHERE delivery_outcome IN ('delivered', 'rto')
            GROUP BY merchant_id, destination_city
        """,
    }

    for view_name, query in views.items():
        cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE")
        cur.execute(f"CREATE MATERIALIZED VIEW {view_name} AS {query}")
        log.info("  Created %s", view_name)

    # Create unique indexes for CONCURRENTLY refresh later
    cur.execute("""CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_cohort
        ON mv_merchant_cohort_stats (merchant_id, category, price_band, payment_mode, origin_state, destination_city)""")
    cur.execute("""CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_peer
        ON mv_peer_benchmarks (category, price_band, payment_mode, origin_state, destination_city)""")
    cur.execute("""CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_merchant_summary
        ON mv_merchant_summary (merchant_id)""")
    cur.execute("""CREATE INDEX IF NOT EXISTS idx_mv_demand_map
        ON mv_demand_map (merchant_id)""")

    conn.commit()
    log.info("Materialized views created.")


def load(csv_path: str, dsn: str) -> None:
    wait_for_postgres(dsn)

    t0 = time.time()
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    cur = conn.cursor()

    apply_schema(cur, conn)

    # ── Pass 1: Collect merchants, classify rows ──
    log.info("Pass 1: Scanning CSV...")
    merchants: dict[str, tuple[str, str | None]] = {}
    total_rows = 0
    order_count_pass1 = 0
    line_item_count_pass1 = 0

    with open(csv_path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_rows += 1
            name = row["hq_client_name"].strip() or "UNKNOWN"
            if name not in merchants:
                ct = row.get("client_type", "").strip() or None
                merchants[name] = (merchant_id(name), ct)
            if row.get("final_status", "").strip():
                order_count_pass1 += 1
            else:
                line_item_count_pass1 += 1

    log.info("Pass 1 done: %d total rows, %d merchants, %d orders, %d line items",
             total_rows, len(merchants), order_count_pass1, line_item_count_pass1)

    # ── Insert merchants ──
    log.info("Inserting %d merchants...", len(merchants))
    execute_values(
        cur,
        "INSERT INTO merchants (merchant_id, name, client_type) VALUES %s "
        "ON CONFLICT (merchant_id) DO NOTHING",
        [(mid, name, ct) for name, (mid, ct) in merchants.items()],
    )
    conn.commit()

    # ── Pass 2: Load orders (rows with final_status) ──
    log.info("Pass 2: Loading orders...")
    order_batch: list[tuple] = []
    orders_loaded = 0

    def flush_orders():
        nonlocal orders_loaded
        if not order_batch:
            return
        execute_values(
            cur,
            """INSERT INTO orders (
                order_id, merchant_id, buyer_id, buyer_rto_history,
                category, price_band, payment_mode,
                origin_city, origin_state, destination_city, destination_pincode,
                address_quality, was_adfix_corrected, manifest_latency,
                order_amount, delivery_outcome, shipping_mode
            ) VALUES %s ON CONFLICT (order_id) DO NOTHING""",
            order_batch,
            page_size=BATCH_SIZE,
        )
        conn.commit()
        orders_loaded += len(order_batch)
        order_batch.clear()

    with open(csv_path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if not row.get("final_status", "").strip():
                continue

            name = row["hq_client_name"].strip() or "UNKNOWN"
            mid = merchants[name][0]

            try:
                amt = min(float(row.get("order_amt", 0) or 0), AMT_CAP)
            except ValueError:
                amt = 0.0

            try:
                rto_hist = float(row.get("buyer_rto_history_pct", 0) or 0)
            except ValueError:
                rto_hist = 0.0

            try:
                manifest = int(float(row.get("manifest_latency_days", 0) or 0))
            except ValueError:
                manifest = 0

            oid = make_order_id(name, i)
            dest_city = normalize_city(row.get("destination_city", "") or "")
            origin_city_val = normalize_city(row.get("origin_city", "") or "")
            cat = row.get("category_name", "").strip()
            if not cat or cat == "Default":
                cat = infer_category(row.get("line_item_name", ""))

            order_batch.append((
                oid, mid,
                row.get("buyer_id", "").strip() or None,
                rto_hist, cat, price_band(amt),
                payment_mode(row.get("hudi_pt_tag", ""), row.get("payment_method", "")),
                origin_city_val if origin_city_val != "Unknown" else None,
                row.get("origin_state", "").strip() or None,
                dest_city if dest_city != "Unknown" else None,
                row.get("original_pincode", "").strip() or None,
                address_quality(row.get("was_adfix_corrected", ""), row.get("buyer_rto_history_pct", "")),
                row.get("was_adfix_corrected", "").strip() == "1",
                manifest, amt,
                delivery_outcome(row.get("is_rto", "0"), row.get("final_status", "")),
                "standard",
            ))

            if len(order_batch) >= BATCH_SIZE:
                flush_orders()
                if orders_loaded % 50_000 == 0:
                    log.info("  %d orders loaded (%.0fs)", orders_loaded, time.time() - t0)

    flush_orders()
    log.info("Orders loaded: %d", orders_loaded)

    # ── Pass 3: Load line items ──
    log.info("Pass 3: Loading line items...")
    li_batch: list[tuple] = []
    li_loaded = 0

    def flush_line_items():
        nonlocal li_loaded
        if not li_batch:
            return
        execute_values(
            cur,
            """INSERT INTO order_line_items (merchant_id, item_name, item_amount, inferred_category)
               VALUES %s""",
            li_batch,
            page_size=BATCH_SIZE,
        )
        conn.commit()
        li_loaded += len(li_batch)
        li_batch.clear()

    with open(csv_path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("final_status", "").strip():
                continue
            name = row["hq_client_name"].strip() or "UNKNOWN"
            mid = merchants[name][0]
            item = row.get("line_item_name", "").strip()
            try:
                amt = float(row.get("order_amt", 0) or 0)
            except ValueError:
                amt = 0.0
            li_batch.append((mid, item, amt, infer_category(item)))
            if len(li_batch) >= BATCH_SIZE:
                flush_line_items()
                if li_loaded % 200_000 == 0:
                    log.info("  %d line items loaded", li_loaded)

    flush_line_items()
    log.info("Line items loaded: %d", li_loaded)

    # ── Merchant permissions ──
    log.info("Setting up merchant permissions...")
    execute_values(
        cur,
        """INSERT INTO merchant_permissions (merchant_id, intervention_type, is_enabled, daily_cap, hourly_cap)
           VALUES %s ON CONFLICT DO NOTHING""",
        [(mid, "all", True, 10, 3) for _, (mid, _) in merchants.items()],
    )
    conn.commit()

    # ── Materialized views ──
    create_materialized_views(cur, conn)

    # ── Final stats ──
    cur.execute("SELECT COUNT(*) FROM orders")
    final_orders = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM merchants")
    final_merchants = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM order_line_items")
    final_li = cur.fetchone()[0]

    elapsed = time.time() - t0
    log.info("\n=== LOAD COMPLETE (%.1fs) ===", elapsed)
    log.info("  Orders:     %d", final_orders)
    log.info("  Merchants:  %d", final_merchants)
    log.info("  Line items: %d", final_li)

    cur.execute(
        "SELECT merchant_id, name, total_orders, rto_rate "
        "FROM mv_merchant_summary ORDER BY total_orders DESC LIMIT 10"
    )
    log.info("\n  Top 10 merchants:")
    for mid, name, total, rto in cur.fetchall():
        log.info("    %-12s  %8d orders  RTO=%.1f%%  %s", mid, total, (rto or 0) * 100, name)

    cur.close()
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load Delhivery CSV into Postgres")
    parser.add_argument("--csv", required=True, help="Path to the CSV file")
    parser.add_argument("--dsn", default="postgresql://commerce:commerce@postgres:5432/commerce_ai",
                        help="Postgres connection string")
    args = parser.parse_args()
    load(args.csv, args.dsn)
