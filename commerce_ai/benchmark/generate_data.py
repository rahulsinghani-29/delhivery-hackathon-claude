"""Load real Delhivery data from CSV into SQLite for benchmarking.

Usage:
    python -m benchmark.generate_data

Reads the Delhivery CSV (17.7M rows) in chunks, deduplicates by waybill,
samples 300K unique waybills, and creates benchmark.db.
"""

from __future__ import annotations

import os
import re
import sqlite3
from pathlib import Path

import pandas as pd

CSV_PATH = (
    "/Users/rahul.singhani/Documents/Product Team - Kiro/"
    "D1 UX changes/delhivery-one-frontend/Hackathon_RTO/claude/"
    "e019c113-850e-48c8-8741-65b6dde2ca3d.csv"
)
DB_PATH = Path(__file__).parent / "benchmark.db"

SAMPLE_SIZE = 300_000
CHUNK_SIZE = 50_000
# Only scan first ~3M rows to keep sampling fast
MAX_ROWS_SCAN = 3_000_000

# ---------------------------------------------------------------------------
# Category classification from line_item_name
# ---------------------------------------------------------------------------

CATEGORY_PATTERNS: list[tuple[str, re.Pattern]] = [
    ("Perfume & Beauty", re.compile(
        r"perfume|fragrance|deodorant|attar|cologne|body\s*spray|"
        r"serum|cream|lotion|moistur|sunscreen|face\s*wash|shampoo|"
        r"conditioner|hair\s*oil|lipstick|mascara|foundation|"
        r"nail\s*polish|makeup|cosmetic|beauty|skincare|soap|"
        r"body\s*wash|cleanser|toner|eye\s*liner|kajal|"
        r"essential\s*oil|aroma", re.IGNORECASE)),
    ("Electronics", re.compile(
        r"phone|mobile|laptop|charger|earphone|headphone|earbud|"
        r"cable|adapter|speaker|watch|smartwatch|camera|tablet|"
        r"electronic|power\s*bank|led|bulb|light|fan|heater|"
        r"iron|trimmer|shaver|dryer|bluetooth|usb|hdmi|"
        r"remote|battery|inverter|router|wifi|printer|"
        r"keyboard|mouse|monitor|tv|television|projector", re.IGNORECASE)),
    ("Home & Kitchen", re.compile(
        r"kitchen|utensil|cookware|mixer|grinder|pan\b|pot\b|plate|"
        r"glass\b|bottle|container|home|decor|curtain|bedsheet|"
        r"pillow|towel|mat\b|rug|carpet|furniture|shelf|rack|"
        r"organizer|storage|basket|bin|hook|hanger|"
        r"candle|vase|frame|mirror|clock|lamp|"
        r"mop|broom|brush|cleaner|detergent|"
        r"garden|plant|seed|fertilizer|pot\b|planter|"
        r"stainless|steel|ceramic|wooden|bamboo", re.IGNORECASE)),
    ("Apparel & Fashion", re.compile(
        r"shirt|tshirt|t-shirt|jeans|trouser|dress|saree|sari|"
        r"kurti|kurta|legging|top\b|jacket|sweater|hoodie|pant|"
        r"skirt|blouse|apparel|cloth|wear|fashion|"
        r"shoe|sandal|slipper|sneaker|boot|heel|"
        r"bag|purse|wallet|belt|scarf|stole|dupatta|"
        r"jewel|necklace|earring|bracelet|ring|bangle|"
        r"sunglasses|cap|hat|sock|underwear|bra|"
        r"tracksuit|shorts|bermuda|palazzo|salwar|"
        r"suit|blazer|coat|sherwani|lehenga|gown", re.IGNORECASE)),
    ("Health & Wellness", re.compile(
        r"vitamin|supplement|protein|ayurved|herbal|medicine|"
        r"tablet|capsule|syrup|health|wellness|organic|"
        r"yoga|fitness|gym|exercise|weight|slim|"
        r"mask|sanitizer|thermometer|oximeter|"
        r"honey|ghee|oil\b|powder\b|tea\b|green\s*tea", re.IGNORECASE)),
    ("Books & Stationery", re.compile(
        r"book|notebook|diary|pen\b|pencil|eraser|ruler|"
        r"stationery|paper|card|envelope|stamp|"
        r"novel|textbook|guide|magazine|comic", re.IGNORECASE)),
    ("Toys & Kids", re.compile(
        r"toy|game|puzzle|doll|action\s*figure|lego|"
        r"baby|infant|diaper|feeding|stroller|"
        r"kid|child|school\s*bag", re.IGNORECASE)),
    ("Food & Grocery", re.compile(
        r"food|snack|chocolate|biscuit|cookie|chips|"
        r"rice|flour|dal|spice|masala|pickle|"
        r"dry\s*fruit|nut|almond|cashew|"
        r"sauce|ketchup|jam|noodle|pasta|"
        r"coffee|milk|juice|drink|water", re.IGNORECASE)),
]


def classify_category(line_item: str | None, cat_name: str | None) -> str:
    """Classify product category from line_item_name using keyword matching.

    Also uses category_name column as a hint if line_item_name doesn't match.
    """
    # Try line_item_name first
    text = str(line_item or "").strip()
    if text:
        for cat, pattern in CATEGORY_PATTERNS:
            if pattern.search(text):
                return cat

    # Fallback to category_name column if available
    fallback = str(cat_name or "").strip()
    if fallback:
        for cat, pattern in CATEGORY_PATTERNS:
            if pattern.search(fallback):
                return cat

    # If category_name has a non-empty value that didn't match patterns,
    # use it directly as a hint (capitalize nicely)
    if fallback and fallback.lower() not in ("", "nan", "none", "null", "others"):
        return fallback.strip().title()

    return "Others"


def classify_price_range(amt: float) -> str:
    """Classify order amount into price range bucket."""
    if amt <= 500:
        return "0-500"
    elif amt <= 1500:
        return "500-1500"
    elif amt <= 3000:
        return "1500-3000"
    else:
        return "3000+"


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS clients (
    client_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    avg_price_range TEXT NOT NULL,
    client_type TEXT
);

CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    client_id TEXT NOT NULL,
    category TEXT NOT NULL,
    price REAL NOT NULL,
    payment_mode TEXT NOT NULL,
    origin_state TEXT,
    destination_city TEXT,
    destination_pincode TEXT,
    buyer_id TEXT,
    buyer_rto_history_pct REAL,
    is_rto INTEGER NOT NULL,
    delivery_outcome TEXT NOT NULL,
    was_adfix_corrected INTEGER,
    manifest_latency_days REAL
);

CREATE INDEX IF NOT EXISTS idx_orders_client_id ON orders(client_id);
CREATE INDEX IF NOT EXISTS idx_orders_category ON orders(category);
CREATE INDEX IF NOT EXISTS idx_orders_delivery_outcome ON orders(delivery_outcome);
CREATE INDEX IF NOT EXISTS idx_orders_destination_city ON orders(destination_city);
CREATE INDEX IF NOT EXISTS idx_orders_destination_pincode ON orders(destination_pincode);
CREATE INDEX IF NOT EXISTS idx_orders_buyer_id ON orders(buyer_id);
CREATE INDEX IF NOT EXISTS idx_orders_payment_mode ON orders(payment_mode);
"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if not Path(CSV_PATH).exists():
        raise FileNotFoundError(
            f"Delhivery CSV not found at {CSV_PATH}. "
            "Update CSV_PATH in generate_data.py."
        )

    # Remove old DB
    if DB_PATH.exists():
        os.remove(DB_PATH)

    print(f"Reading CSV in chunks of {CHUNK_SIZE:,} from {CSV_PATH}...")

    # ── Phase 1: Collect unique waybills from first ~3M rows ──
    seen_waybills: set[str] = set()
    unique_rows: list[pd.DataFrame] = []
    rows_scanned = 0

    reader = pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE, dtype=str, low_memory=False)
    for chunk_num, chunk in enumerate(reader):
        rows_scanned += len(chunk)

        # Deduplicate within chunk
        chunk = chunk.drop_duplicates(subset=["waybill_number"], keep="first")

        # Remove waybills we've already seen
        new_mask = ~chunk["waybill_number"].isin(seen_waybills)
        new_rows = chunk[new_mask]
        seen_waybills.update(new_rows["waybill_number"].tolist())
        unique_rows.append(new_rows)

        print(
            f"  Chunk {chunk_num + 1}: scanned {rows_scanned:,} rows, "
            f"{len(seen_waybills):,} unique waybills so far"
        )

        if len(seen_waybills) >= SAMPLE_SIZE or rows_scanned >= MAX_ROWS_SCAN:
            break

    # Combine and sample
    df = pd.concat(unique_rows, ignore_index=True)
    del unique_rows  # free memory

    if len(df) > SAMPLE_SIZE:
        df = df.sample(n=SAMPLE_SIZE, random_state=42)
    print(f"  Sampled {len(df):,} unique waybills")

    # ── Phase 2: Filter out In-Transit ──
    df = df[df["final_status"].isin(["Delivered", "RTO"])].copy()
    print(f"  After filtering In-Transit: {len(df):,} orders")

    # ── Phase 3: Clean and transform ──
    # Numeric columns
    df["order_amt"] = pd.to_numeric(df["order_amt"], errors="coerce").fillna(0)
    df["buyer_rto_history_pct"] = pd.to_numeric(
        df["buyer_rto_history_pct"], errors="coerce"
    ).fillna(0)
    df["is_rto"] = pd.to_numeric(df["is_rto"], errors="coerce").fillna(0).astype(int)
    df["was_adfix_corrected"] = (
        pd.to_numeric(df["was_adfix_corrected"], errors="coerce").fillna(0).astype(int)
    )
    df["manifest_latency_days"] = pd.to_numeric(
        df["manifest_latency_days"], errors="coerce"
    ).fillna(0)

    # Classify category
    df["category"] = df.apply(
        lambda r: classify_category(r.get("line_item_name"), r.get("category_name")),
        axis=1,
    )

    # Classify price range
    df["price_range"] = df["order_amt"].apply(classify_price_range)

    # Delivery outcome
    df["delivery_outcome"] = df["final_status"].map(
        {"Delivered": "Delivered", "RTO": "RTO"}
    )

    # Payment mode normalization
    df["payment_mode"] = df["hudi_payment_method"].fillna("Pre-paid").str.strip()
    df.loc[~df["payment_mode"].isin(["COD", "Pre-paid"]), "payment_mode"] = "Pre-paid"

    # Destination pincode — use if available, else empty string
    if "destination_pincode" in df.columns:
        df["destination_pincode"] = df["destination_pincode"].fillna("").astype(str).str.strip()
    elif "dest_pincode" in df.columns:
        df["destination_pincode"] = df["dest_pincode"].fillna("").astype(str).str.strip()
    else:
        df["destination_pincode"] = ""

    print(f"  Categories: {df['category'].value_counts().to_dict()}")
    print(f"  Payment modes: {df['payment_mode'].value_counts().to_dict()}")
    print(f"  Outcomes: {df['delivery_outcome'].value_counts().to_dict()}")

    # ── Phase 4: Build client table ──
    client_agg = df.groupby("hq_client_name").agg(
        category=("category", lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else "Others"),
        avg_price_range=("price_range", lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else "0-500"),
        client_type=("client_type", "first"),
    ).reset_index()
    client_agg.rename(columns={"hq_client_name": "client_id"}, inplace=True)
    client_agg["name"] = client_agg["client_id"]

    print(f"\n{len(client_agg):,} unique clients")

    # ── Phase 5: Write to SQLite ──
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA_SQL)

    # Insert clients
    client_records = client_agg[
        ["client_id", "name", "category", "avg_price_range", "client_type"]
    ].values.tolist()
    conn.executemany(
        "INSERT INTO clients (client_id, name, category, avg_price_range, client_type) "
        "VALUES (?, ?, ?, ?, ?)",
        client_records,
    )
    conn.commit()
    print(f"  Inserted {len(client_records):,} clients")

    # Insert orders in chunks
    order_cols = [
        "waybill_number", "hq_client_name", "category", "order_amt",
        "payment_mode", "origin_state", "destination_city", "destination_pincode",
        "buyer_id", "buyer_rto_history_pct", "is_rto", "delivery_outcome",
        "was_adfix_corrected", "manifest_latency_days",
    ]
    order_df = df[order_cols].copy()
    order_df.columns = [
        "order_id", "client_id", "category", "price",
        "payment_mode", "origin_state", "destination_city", "destination_pincode",
        "buyer_id", "buyer_rto_history_pct", "is_rto", "delivery_outcome",
        "was_adfix_corrected", "manifest_latency_days",
    ]

    total_inserted = 0
    insert_chunk_size = 10_000
    records = order_df.values.tolist()
    for i in range(0, len(records), insert_chunk_size):
        batch = records[i : i + insert_chunk_size]
        conn.executemany(
            """INSERT OR IGNORE INTO orders (
                order_id, client_id, category, price, payment_mode,
                origin_state, destination_city, destination_pincode, buyer_id,
                buyer_rto_history_pct, is_rto, delivery_outcome,
                was_adfix_corrected, manifest_latency_days
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            batch,
        )
        conn.commit()
        total_inserted += len(batch)
        print(f"  Orders: {total_inserted:,} / {len(records):,}")

    conn.close()
    print(f"\nDone! {total_inserted:,} orders across {len(client_records):,} clients in {DB_PATH}")


if __name__ == "__main__":
    main()
