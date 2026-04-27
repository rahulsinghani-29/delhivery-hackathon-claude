"""
Validate the Postgres data load — run after load_postgres.py.

Checks:
    1. Row counts match expectations
    2. No NULL values in required columns
    3. Enum values are valid (payment_mode, delivery_outcome, price_band)
    4. RTO rates match known benchmarks from CSV analysis
    5. Materialized views are populated and consistent
    6. Top merchant matches expected (ZIBBRI B2C)
    7. Referential integrity (all order merchant_ids exist in merchants)
    8. Amount distribution sanity check

Usage:
    python -m scripts.validate_postgres \
        --dsn "postgresql://commerce:commerce@postgres:5432/commerce_ai"
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


class ValidationResult:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.warnings = 0
        self.details: list[str] = []

    def check(self, name: str, condition: bool, msg: str = ""):
        if condition:
            self.passed += 1
            log.info("  PASS  %s", name)
        else:
            self.failed += 1
            detail = f"FAIL  {name}: {msg}"
            self.details.append(detail)
            log.error("  %s", detail)

    def warn(self, name: str, msg: str):
        self.warnings += 1
        log.warning("  WARN  %s: %s", name, msg)

    def summary(self) -> bool:
        log.info("\n" + "=" * 60)
        log.info("VALIDATION SUMMARY")
        log.info("  Passed:   %d", self.passed)
        log.info("  Failed:   %d", self.failed)
        log.info("  Warnings: %d", self.warnings)
        if self.details:
            log.info("\nFailures:")
            for d in self.details:
                log.info("  - %s", d)
        log.info("=" * 60)
        return self.failed == 0


def validate(dsn: str) -> bool:
    log.info("Connecting to Postgres...")
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()
    r = ValidationResult()

    # ── 1. Row counts ──
    log.info("\n--- 1. ROW COUNTS ---")

    cur.execute("SELECT COUNT(*) FROM merchants")
    merchant_count = cur.fetchone()[0]
    r.check("merchants > 0", merchant_count > 0, f"got {merchant_count}")
    r.check("merchants ~ 2000-3000", 1500 <= merchant_count <= 4000,
            f"got {merchant_count}, expected ~2100-2900")

    cur.execute("SELECT COUNT(*) FROM orders")
    order_count = cur.fetchone()[0]
    r.check("orders > 0", order_count > 0, f"got {order_count}")
    r.check("orders ~ 470k-480k", 400_000 <= order_count <= 500_000,
            f"got {order_count}, expected ~474k")

    cur.execute("SELECT COUNT(*) FROM order_line_items")
    li_count = cur.fetchone()[0]
    r.check("line_items > 0", li_count > 0, f"got {li_count}")
    r.check("line_items ~ 1.4M", 1_200_000 <= li_count <= 1_600_000,
            f"got {li_count}, expected ~1.43M")

    cur.execute("SELECT COUNT(*) FROM merchant_permissions")
    perm_count = cur.fetchone()[0]
    r.check("permissions exist", perm_count > 0, f"got {perm_count}")

    # ── 2. NULL checks on required columns ──
    log.info("\n--- 2. NULL CHECKS ---")

    for col in ["merchant_id", "price_band", "payment_mode", "delivery_outcome", "order_amount"]:
        cur.execute(f"SELECT COUNT(*) FROM orders WHERE {col} IS NULL")
        null_count = cur.fetchone()[0]
        r.check(f"orders.{col} no NULLs", null_count == 0, f"{null_count} NULLs found")

    cur.execute("SELECT COUNT(*) FROM merchants WHERE merchant_id IS NULL OR name IS NULL")
    r.check("merchants no NULL ids/names", cur.fetchone()[0] == 0)

    # ── 3. Enum value validation ──
    log.info("\n--- 3. ENUM VALUES ---")

    cur.execute("SELECT DISTINCT payment_mode FROM orders")
    payment_modes = {row[0] for row in cur.fetchall()}
    r.check("payment_mode values valid",
            payment_modes.issubset({"COD", "Prepaid"}),
            f"got {payment_modes}")

    cur.execute("SELECT DISTINCT delivery_outcome FROM orders")
    outcomes = {row[0] for row in cur.fetchall()}
    valid_outcomes = {"delivered", "rto", "in_transit", "cancelled", "pending"}
    r.check("delivery_outcome values valid",
            outcomes.issubset(valid_outcomes),
            f"got {outcomes}")

    cur.execute("SELECT DISTINCT price_band FROM orders")
    bands = {row[0] for row in cur.fetchall()}
    r.check("price_band values valid",
            bands.issubset({"low", "mid", "high", "premium"}),
            f"got {bands}")

    # ── 4. RTO rate benchmarks ──
    log.info("\n--- 4. RTO RATE BENCHMARKS ---")

    cur.execute("""
        SELECT
            COUNT(*) FILTER (WHERE delivery_outcome = 'rto') AS rto,
            COUNT(*) FILTER (WHERE delivery_outcome IN ('delivered', 'rto')) AS total
        FROM orders
    """)
    rto, total = cur.fetchone()
    overall_rto = rto / total if total > 0 else 0
    r.check("overall RTO rate ~ 15-20%",
            0.12 <= overall_rto <= 0.25,
            f"got {overall_rto:.1%}")

    # COD vs Prepaid RTO
    cur.execute("""
        SELECT payment_mode,
               COUNT(*) FILTER (WHERE delivery_outcome = 'rto') AS rto,
               COUNT(*) FILTER (WHERE delivery_outcome IN ('delivered', 'rto')) AS total
        FROM orders
        GROUP BY payment_mode
    """)
    for pm, rto_cnt, total_cnt in cur.fetchall():
        rate = rto_cnt / total_cnt if total_cnt > 0 else 0
        if pm == "COD":
            r.check("COD RTO rate ~ 30-40%", 0.25 <= rate <= 0.45,
                    f"got {rate:.1%}")
        elif pm == "Prepaid":
            r.check("Prepaid RTO rate ~ 0-5%", rate <= 0.08,
                    f"got {rate:.1%}")

    # ── 5. Materialized views ──
    log.info("\n--- 5. MATERIALIZED VIEWS ---")

    for view in ["mv_merchant_cohort_stats", "mv_peer_benchmarks",
                 "mv_merchant_summary", "mv_demand_map"]:
        cur.execute(f"SELECT COUNT(*) FROM {view}")
        cnt = cur.fetchone()[0]
        r.check(f"{view} populated", cnt > 0, f"got {cnt} rows")

    # Cross-check: mv_merchant_summary total should match orders
    cur.execute("SELECT SUM(total_orders) FROM mv_merchant_summary")
    mv_total = cur.fetchone()[0] or 0
    r.check("mv_merchant_summary total ~ order count",
            abs(mv_total - order_count) <= order_count * 0.01,
            f"mv_total={mv_total}, orders={order_count}")

    # ── 6. Top merchant check ──
    log.info("\n--- 6. TOP MERCHANT ---")

    cur.execute("""
        SELECT merchant_id, name, total_orders
        FROM mv_merchant_summary
        ORDER BY total_orders DESC
        LIMIT 1
    """)
    top = cur.fetchone()
    if top:
        log.info("  Top merchant: %s (%s) with %d orders", top[0], top[1], top[2])
        r.check("top merchant is ZIBBRI B2C",
                "ZIBBRI" in (top[1] or "").upper(),
                f"got {top[1]}")
        r.check("top merchant has ~60k+ orders",
                top[2] >= 50_000,
                f"got {top[2]}")

    # ── 7. Referential integrity ──
    log.info("\n--- 7. REFERENTIAL INTEGRITY ---")

    cur.execute("""
        SELECT COUNT(*) FROM orders o
        LEFT JOIN merchants m ON o.merchant_id = m.merchant_id
        WHERE m.merchant_id IS NULL
    """)
    orphans = cur.fetchone()[0]
    r.check("no orphan orders (all merchant_ids exist)", orphans == 0,
            f"{orphans} orders with missing merchant")

    # ── 8. Amount distribution ──
    log.info("\n--- 8. AMOUNT DISTRIBUTION ---")

    cur.execute("""
        SELECT
            MIN(order_amount), MAX(order_amount),
            AVG(order_amount),
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY order_amount),
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY order_amount)
        FROM orders
    """)
    mn, mx, avg, median, p99 = cur.fetchone()
    log.info("  min=%.2f  max=%.2f  avg=%.2f  median=%.2f  P99=%.2f", mn, mx, avg, median, p99)
    r.check("max amount <= 50000 (capped)", float(mx) <= 50_001,
            f"got {mx}")
    r.check("median amount ~ 500-800", 300 <= float(median) <= 1200,
            f"got {median}")

    # ── 9. Category inference coverage ──
    log.info("\n--- 9. CATEGORY COVERAGE ---")

    cur.execute("""
        SELECT category, COUNT(*) AS cnt
        FROM orders
        GROUP BY category
        ORDER BY cnt DESC
        LIMIT 10
    """)
    log.info("  Top categories:")
    general_count = 0
    for cat, cnt in cur.fetchall():
        pct = 100 * cnt / order_count if order_count > 0 else 0
        log.info("    %-20s %8d (%.1f%%)", cat, cnt, pct)
        if cat == "general":
            general_count = cnt

    if general_count > 0:
        general_pct = general_count / order_count
        if general_pct > 0.8:
            r.warn("category inference", f"{general_pct:.0%} orders are 'general' — category inference has limited coverage")
        else:
            r.check("category inference < 80% general", True)

    # ── 10. Destination city normalization ──
    log.info("\n--- 10. CITY NORMALIZATION ---")

    cur.execute("""
        SELECT destination_city, COUNT(*) AS cnt
        FROM orders
        WHERE destination_city IS NOT NULL
        GROUP BY destination_city
        ORDER BY cnt DESC
        LIMIT 10
    """)
    log.info("  Top destination cities:")
    for city, cnt in cur.fetchall():
        log.info("    %-25s %8d", city, cnt)

    # Check no Bangalore/BANGALORE duplicates
    cur.execute("""
        SELECT COUNT(DISTINCT destination_city)
        FROM orders
        WHERE LOWER(destination_city) IN ('bangalore', 'bengaluru')
    """)
    bangalore_variants = cur.fetchone()[0]
    r.check("Bangalore normalized (1 variant)", bangalore_variants <= 1,
            f"got {bangalore_variants} variants")

    # ── Done ──
    cur.close()
    conn.close()
    return r.summary()


def main():
    parser = argparse.ArgumentParser(description="Validate Postgres data load")
    parser.add_argument("--dsn", default="postgresql://commerce:commerce@postgres:5432/commerce_ai")
    args = parser.parse_args()

    # Wait for postgres
    for attempt in range(15):
        try:
            conn = psycopg2.connect(args.dsn)
            conn.close()
            break
        except psycopg2.OperationalError:
            log.info("Waiting for Postgres... (%d/15)", attempt + 1)
            time.sleep(2)

    success = validate(args.dsn)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
