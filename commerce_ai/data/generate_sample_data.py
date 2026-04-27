"""Generate realistic sample CSV data files for Delhivery Commerce AI.

Usage:
    python -m data.generate_sample_data

Generates files in data/sample/:
    - merchants.csv
    - warehouse_nodes.csv
    - orders.csv
    - interventions.csv
"""

from __future__ import annotations

import csv
import math
import os
import random
from datetime import datetime, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SEED = 42
NUM_MERCHANTS = 120
NUM_ORDERS = 5_000
DATE_START = datetime(2025, 10, 1)
DATE_END = datetime(2026, 3, 31)

CITIES = [
    ("Delhi", "Delhi", "110001"),
    ("Mumbai", "Maharashtra", "400001"),
    ("Bangalore", "Karnataka", "560001"),
    ("Hyderabad", "Telangana", "500001"),
    ("Chennai", "Tamil Nadu", "600001"),
    ("Kolkata", "West Bengal", "700001"),
    ("Pune", "Maharashtra", "411001"),
    ("Ahmedabad", "Gujarat", "380001"),
    ("Jaipur", "Rajasthan", "302001"),
    ("Lucknow", "Uttar Pradesh", "226001"),
]

CATEGORIES = ["electronics", "fashion", "beauty", "home", "grocery"]
CATEGORY_WEIGHTS = [0.20, 0.30, 0.15, 0.20, 0.15]

PRICE_BANDS = ["0-500", "500-1000", "1000-2000", "2000+"]
PRICE_BAND_WEIGHTS = [0.35, 0.30, 0.20, 0.15]

PAYMENT_MODES = ["COD", "prepaid"]
PAYMENT_MODE_WEIGHTS = [0.60, 0.40]

DESTINATION_CLUSTERS = ["north", "south", "east", "west", "central", "northeast"]
CLUSTER_WEIGHTS = [0.25, 0.22, 0.15, 0.18, 0.12, 0.08]

SHIPPING_MODES = ["surface", "express"]
SHIPPING_MODE_WEIGHTS = [0.95, 0.05]

DELIVERY_OUTCOMES = ["delivered", "rto", "pending"]

INTERVENTION_TYPES = [
    "verification",
    "cancellation",
    "masked_calling",
    "cod_to_prepaid",
    "premium_courier",
    "merchant_confirmation",
    "address_enrichment_outreach",
    "cod_to_prepaid_outreach",
    "auto_cancel",
    "express_upgrade",
]

INTERVENTION_OWNERS = {
    "verification": "delhivery",
    "cancellation": "delhivery",
    "masked_calling": "delhivery",
    "cod_to_prepaid": "merchant",
    "premium_courier": "delhivery",
    "merchant_confirmation": "merchant",
    "address_enrichment_outreach": "delhivery",
    "cod_to_prepaid_outreach": "delhivery",
    "auto_cancel": "delhivery",
    "express_upgrade": "delhivery",
}

# Sale periods with higher order volume
SALE_PERIODS = [
    (datetime(2025, 10, 15), datetime(2025, 10, 20)),  # Diwali sale
    (datetime(2025, 11, 22), datetime(2025, 11, 30)),   # Black Friday / end-of-month
    (datetime(2025, 12, 25), datetime(2025, 12, 31)),   # Year-end sale
    (datetime(2026, 1, 14), datetime(2026, 1, 20)),     # Makar Sankranti / Pongal
    (datetime(2026, 3, 8), datetime(2026, 3, 15)),      # Holi sale
]

OUTPUT_DIR = Path(__file__).parent / "sample"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_sale_period(dt: datetime) -> bool:
    """Check if a datetime falls within a sale period."""
    for start, end in SALE_PERIODS:
        if start <= dt <= end:
            return True
    return False


def _random_phone() -> str:
    """Generate an Indian mobile number like 9XXXXXXXXX."""
    return "9" + "".join(str(random.randint(0, 9)) for _ in range(9))


def _clamp(val: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, val))


def _random_datetime_in_range(start: datetime, end: datetime) -> datetime:
    """Return a random datetime between start and end with realistic hour distribution."""
    delta = (end - start).total_seconds()
    offset = random.random() * delta
    dt = start + timedelta(seconds=offset)
    # Bias hours: more orders during 10-22, fewer at night
    hour = dt.hour
    if hour < 6:
        # 30% chance to keep late-night orders
        if random.random() > 0.30:
            dt = dt.replace(hour=random.randint(10, 21))
    return dt


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------


def generate_merchants() -> list[dict]:
    """Generate 120 merchants with IDs M001-M120."""
    merchants = []
    for i in range(1, NUM_MERCHANTS + 1):
        merchants.append({
            "merchant_id": f"M{i:03d}",
            "name": f"Merchant_{i:03d}",
        })
    return merchants


def generate_warehouse_nodes(merchants: list[dict]) -> list[dict]:
    """Generate 2-3 warehouse nodes per merchant."""
    nodes = []
    node_counter = 0
    for m in merchants:
        num_nodes = random.choice([2, 2, 3])  # Slightly more 2-node merchants
        chosen_cities = random.sample(CITIES, num_nodes)
        for city, state, base_pin in chosen_cities:
            node_counter += 1
            # Vary pincode slightly
            pin = str(int(base_pin) + random.randint(0, 99))
            nodes.append({
                "node_id": f"WH{node_counter:04d}",
                "merchant_id": m["merchant_id"],
                "city": city,
                "state": state,
                "pincode": pin,
                "is_active": "true",
            })
    return nodes


def generate_orders(
    merchants: list[dict],
    warehouse_nodes: list[dict],
) -> list[dict]:
    """Generate ~50,000 orders across 6 months with realistic distributions."""
    # Build merchant -> nodes mapping
    merchant_nodes: dict[str, list[dict]] = {}
    for node in warehouse_nodes:
        mid = node["merchant_id"]
        merchant_nodes.setdefault(mid, []).append(node)

    # Build a pool of customer UCIDs with some repeat customers
    # ~30,000 unique customers, so some will have multiple orders
    num_unique_customers = 30_000
    customer_pool = [_random_phone() for _ in range(num_unique_customers)]

    # Destination pincodes pool (realistic Indian pincodes)
    dest_pincodes = [str(random.randint(110001, 855999)) for _ in range(2000)]

    orders = []
    for i in range(1, NUM_ORDERS + 1):
        merchant = random.choice(merchants)
        mid = merchant["merchant_id"]
        nodes = merchant_nodes[mid]
        origin_node = random.choice(nodes)

        # Pick a date with sale-period bias
        dt = _random_datetime_in_range(DATE_START, DATE_END)
        # During sale periods, generate extra orders (handled by oversampling)
        if _is_sale_period(dt):
            # 40% more likely to keep sale-period dates
            pass
        else:
            # Small chance to re-roll into a sale period
            if random.random() < 0.10:
                sale = random.choice(SALE_PERIODS)
                dt = _random_datetime_in_range(sale[0], sale[1])

        category = random.choices(CATEGORIES, weights=CATEGORY_WEIGHTS, k=1)[0]
        price_band = random.choices(PRICE_BANDS, weights=PRICE_BAND_WEIGHTS, k=1)[0]
        payment_mode = random.choices(PAYMENT_MODES, weights=PAYMENT_MODE_WEIGHTS, k=1)[0]
        dest_cluster = random.choices(DESTINATION_CLUSTERS, weights=CLUSTER_WEIGHTS, k=1)[0]
        shipping_mode = random.choices(SHIPPING_MODES, weights=SHIPPING_MODE_WEIGHTS, k=1)[0]

        # Customer UCID — pick from pool (allows repeats)
        customer_ucid = random.choice(customer_pool)

        # Address quality: normally distributed around 0.7, with outliers
        addr_quality = _clamp(random.gauss(0.70, 0.15))
        # Inject some low-quality outliers (~5%)
        if random.random() < 0.05:
            addr_quality = _clamp(random.uniform(0.0, 0.3))

        # RTO score: skewed toward low, with tail of high-risk
        # Use beta distribution: alpha=2, beta=5 gives mean ~0.28, skewed low
        rto_score = _clamp(random.betavariate(2, 5))
        # COD orders tend to have higher RTO scores
        if payment_mode == "COD":
            rto_score = _clamp(rto_score + random.uniform(0.05, 0.15))
        # Low address quality increases RTO
        if addr_quality < 0.4:
            rto_score = _clamp(rto_score + random.uniform(0.1, 0.25))

        # Delivery outcome correlated with rto_score
        if rto_score > 0.7:
            outcome = random.choices(
                DELIVERY_OUTCOMES, weights=[0.25, 0.60, 0.15], k=1
            )[0]
        elif rto_score > 0.4:
            outcome = random.choices(
                DELIVERY_OUTCOMES, weights=[0.55, 0.30, 0.15], k=1
            )[0]
        else:
            outcome = random.choices(
                DELIVERY_OUTCOMES, weights=[0.80, 0.10, 0.10], k=1
            )[0]

        dest_pin = random.choice(dest_pincodes)

        orders.append({
            "order_id": f"ORD{i:06d}",
            "merchant_id": mid,
            "customer_ucid": customer_ucid,
            "category": category,
            "price_band": price_band,
            "payment_mode": payment_mode,
            "origin_node": origin_node["node_id"],
            "destination_pincode": dest_pin,
            "destination_cluster": dest_cluster,
            "address_quality": f"{addr_quality:.4f}",
            "rto_score": f"{rto_score:.4f}",
            "delivery_outcome": outcome,
            "shipping_mode": shipping_mode,
            "created_at": dt.strftime("%Y-%m-%d %H:%M:%S"),
        })

    return orders


def generate_interventions(orders: list[dict]) -> list[dict]:
    """Generate ~5,000 intervention records linked to high-RTO orders."""
    # Filter to high-RTO orders (rto_score > 0.4)
    high_rto_orders = [o for o in orders if float(o["rto_score"]) > 0.4]
    random.shuffle(high_rto_orders)

    # Pick ~5000 orders for interventions (some orders may get multiple)
    target_count = 5000
    interventions = []
    idx = 0

    for o in high_rto_orders:
        if idx >= target_count:
            break

        rto = float(o["rto_score"])
        order_dt = datetime.strptime(o["created_at"], "%Y-%m-%d %H:%M:%S")

        # Pick intervention type based on RTO score
        if rto > 0.85:
            itype = random.choices(
                ["auto_cancel", "cancellation", "verification"],
                weights=[0.5, 0.3, 0.2],
                k=1,
            )[0]
        elif rto > 0.6:
            itype = random.choices(
                [
                    "verification",
                    "masked_calling",
                    "address_enrichment_outreach",
                    "cod_to_prepaid_outreach",
                    "express_upgrade",
                ],
                weights=[0.25, 0.20, 0.20, 0.20, 0.15],
                k=1,
            )[0]
        else:
            itype = random.choices(
                [
                    "merchant_confirmation",
                    "cod_to_prepaid",
                    "premium_courier",
                    "verification",
                ],
                weights=[0.30, 0.25, 0.25, 0.20],
                k=1,
            )[0]

        action_owner = INTERVENTION_OWNERS[itype]
        initiated_by = "system" if action_owner == "delhivery" else random.choice(["system", "merchant"])

        # Confidence score
        confidence = _clamp(random.gauss(0.72, 0.15))

        # Outcome correlated with intervention type
        if itype in ("auto_cancel", "cancellation"):
            outcome = random.choices(
                ["successful_delivery", "rto", "pending"],
                weights=[0.10, 0.70, 0.20],
                k=1,
            )[0]
        elif itype == "express_upgrade":
            outcome = random.choices(
                ["successful_delivery", "rto", "pending"],
                weights=[0.65, 0.20, 0.15],
                k=1,
            )[0]
        else:
            outcome = random.choices(
                ["successful_delivery", "rto", "pending"],
                weights=[0.50, 0.30, 0.20],
                k=1,
            )[0]

        # Executed shortly after order creation
        exec_offset = timedelta(minutes=random.randint(1, 120))
        executed_at = order_dt + exec_offset

        completed_at = ""
        if outcome != "pending":
            completed_at = (executed_at + timedelta(hours=random.randint(1, 72))).strftime(
                "%Y-%m-%d %H:%M:%S"
            )

        idx += 1
        interventions.append({
            "intervention_id": f"INT{idx:05d}",
            "order_id": o["order_id"],
            "merchant_id": o["merchant_id"],
            "intervention_type": itype,
            "action_owner": action_owner,
            "initiated_by": initiated_by,
            "confidence_score": f"{confidence:.4f}",
            "outcome": outcome,
            "executed_at": executed_at.strftime("%Y-%m-%d %H:%M:%S"),
            "completed_at": completed_at,
        })

    return interventions


# ---------------------------------------------------------------------------
# CSV writing
# ---------------------------------------------------------------------------


def _write_csv(filepath: Path, rows: list[dict]) -> None:
    """Write a list of dicts to a CSV file."""
    if not rows:
        return
    filepath.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Written {len(rows):,} rows → {filepath}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Generate all sample CSV files."""
    random.seed(SEED)

    print("Generating sample data...")

    merchants = generate_merchants()
    _write_csv(OUTPUT_DIR / "merchants.csv", merchants)

    nodes = generate_warehouse_nodes(merchants)
    _write_csv(OUTPUT_DIR / "warehouse_nodes.csv", nodes)

    orders = generate_orders(merchants, nodes)
    _write_csv(OUTPUT_DIR / "orders.csv", orders)

    interventions = generate_interventions(orders)
    _write_csv(OUTPUT_DIR / "interventions.csv", interventions)

    print(f"\nDone! Files in {OUTPUT_DIR}/")
    print(f"  Merchants:       {len(merchants):,}")
    print(f"  Warehouse nodes: {len(nodes):,}")
    print(f"  Orders:          {len(orders):,}")
    print(f"  Interventions:   {len(interventions):,}")


if __name__ == "__main__":
    main()
