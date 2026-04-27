"""Benchmark API — compare client COD RTO performance against peers.

Run:
    uvicorn benchmark.app:app --port 8001 --reload
"""

from __future__ import annotations

import sqlite3
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

DB_PATH = Path(__file__).parent / "benchmark.db"

# ---------------------------------------------------------------------------
# Database — uses main data.db module if Postgres is available, else local SQLite
# ---------------------------------------------------------------------------

_conn = None


def get_db():
    global _conn
    if _conn is not None:
        return _conn
    try:
        from data.db import get_db as main_get_db, is_postgres
        if is_postgres():
            _conn = main_get_db(str(DB_PATH))
            print("Benchmark: using Postgres from main db module")
            return _conn
    except Exception:
        pass
    # Fallback to local SQLite
    import sqlite3
    _conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    _conn.row_factory = sqlite3.Row
    _conn.execute("PRAGMA journal_mode=WAL")
    return _conn


def _check_and_generate_data() -> None:
    """Generate data if DB doesn't exist or is empty."""
    try:
        from data.db import is_postgres
        if is_postgres():
            print("Benchmark: Postgres mode — skipping local data generation")
            return
    except Exception:
        pass
    if not DB_PATH.exists():
        print("benchmark.db not found — generating data...")
        from benchmark.generate_data import main as gen_main
        gen_main()
        return
    conn = get_db()
    try:
        count = conn.execute("SELECT COUNT(*) FROM clients").fetchone()[0]
        if count == 0:
            print("DB empty — generating data...")
            from benchmark.generate_data import main as gen_main
            gen_main()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    _check_and_generate_data()
    yield
    if _conn:
        _conn.close()


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(title="Benchmark API", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _df_from_query(sql: str, params: tuple = ()) -> pd.DataFrame:
    conn = get_db()
    return pd.read_sql_query(sql, conn, params=params)


def _round(v: float, n: int = 2) -> float:
    return round(float(v), n)


def _pct(num: float, den: float, decimals: int = 1) -> float:
    return round(float(num / den * 100) if den > 0 else 0.0, decimals)


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    conn = get_db()
    clients = conn.execute("SELECT COUNT(*) FROM clients").fetchone()[0]
    orders = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    return {"status": "ok", "clients": clients, "orders": orders}


# ---------------------------------------------------------------------------
# GET /clients
# ---------------------------------------------------------------------------

@app.get("/clients")
def list_clients():
    df = _df_from_query("""
        SELECT
            c.client_id, c.name, c.category, c.avg_price_range, c.client_type,
            COUNT(o.order_id) AS order_count,
            SUM(CASE WHEN o.payment_mode = 'COD' AND o.is_rto = 1 THEN 1 ELSE 0 END) AS cod_rto_count,
            SUM(CASE WHEN o.payment_mode = 'COD' THEN 1 ELSE 0 END) AS cod_count
        FROM clients c
        LEFT JOIN orders o ON c.client_id = o.client_id
        GROUP BY c.client_id
        HAVING order_count > 0
        ORDER BY order_count DESC
    """)
    df["rto_rate"] = (df["cod_rto_count"] / df["cod_count"].replace(0, 1) * 100).round(1)
    return df[
        ["client_id", "name", "category", "avg_price_range", "client_type", "order_count", "rto_rate"]
    ].to_dict(orient="records")


# ---------------------------------------------------------------------------
# GET /clients/{client_id}/benchmark
# ---------------------------------------------------------------------------

@app.get("/clients/{client_id}/benchmark")
def benchmark_client(client_id: str):
    conn = get_db()

    # 1. Get client info
    row = conn.execute(
        "SELECT * FROM clients WHERE client_id = ?", (client_id,)
    ).fetchone()
    if not row:
        raise HTTPException(404, f"Client {client_id} not found")

    client_info = dict(row)
    category = client_info["category"]

    # 2. Find peers: same category, ranked by AOV closeness then volume
    # First get client's AOV
    client_aov_row = conn.execute(
        "SELECT AVG(price) as aov FROM orders WHERE client_id = ?", (client_id,)
    ).fetchone()
    client_aov = float(client_aov_row["aov"]) if client_aov_row and client_aov_row["aov"] else 0

    peers_df = _df_from_query("""
        SELECT c.client_id, c.name, c.category, c.avg_price_range, c.client_type,
               COUNT(o.order_id) AS order_count,
               AVG(o.price) AS aov
        FROM clients c
        JOIN orders o ON c.client_id = o.client_id
        WHERE c.category = ? AND c.client_id != ?
        GROUP BY c.client_id
        HAVING order_count >= 10
    """, (category, client_id))

    if peers_df.empty:
        raise HTTPException(404, "No peers found for this category")

    # Sort by AOV closeness to target client
    peers_df["aov_diff"] = (peers_df["aov"] - client_aov).abs()
    peers_df = peers_df.sort_values("aov_diff")

    # Among top 20 closest AOV, pick top 5 by order_count
    top_aov_peers = peers_df.head(20)
    top_peers = top_aov_peers.nlargest(5, "order_count")

    peer_ids = top_peers["client_id"].tolist()

    # 3. Load all orders for client + peers
    all_ids = [client_id] + peer_ids
    placeholders = ",".join(["?"] * len(all_ids))
    orders_df = _df_from_query(
        f"SELECT * FROM orders WHERE client_id IN ({placeholders})",
        tuple(all_ids),
    )

    client_orders = orders_df[orders_df["client_id"] == client_id]
    peer_orders = orders_df[orders_df["client_id"].isin(peer_ids)]

    # 4. Compute metrics
    client_metrics = _compute_metrics(client_orders)
    peer_avg_metrics = _compute_metrics(peer_orders)

    # Per-peer metrics
    per_peer = []
    for pid in peer_ids:
        po = orders_df[orders_df["client_id"] == pid]
        pm = _compute_metrics(po)
        peer_info = top_peers[top_peers["client_id"] == pid].iloc[0]
        per_peer.append({
            "client_id": pid,
            "name": peer_info["name"],
            "order_count": int(peer_info["order_count"]),
            **pm,
        })

    # 5. Pincode-level insight for top 3 destination cities
    city_pincode_rto = _compute_city_pincode_rto(client_orders, peer_orders)
    client_metrics["city_pincode_rto"] = city_pincode_rto

    # 6. Diagnosis — consulting report style
    diagnosis = _diagnose(
        client_orders, peer_orders, client_metrics, peer_avg_metrics,
        client_info, per_peer,
    )

    return {
        "client": {
            **client_info,
            "order_count": len(client_orders),
            **client_metrics,
        },
        "peers": per_peer,
        "peer_average": peer_avg_metrics,
        **diagnosis,
    }


# ---------------------------------------------------------------------------
# Metric computation — ALL RTO rates are COD-only
# ---------------------------------------------------------------------------

def _compute_metrics(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {}

    total = len(df)

    # COD / Prepaid split
    cod_df = df[df["payment_mode"] == "COD"]
    prepaid_df = df[df["payment_mode"] == "Pre-paid"]
    cod_pct = _pct(len(cod_df), total)

    # RTO rate = COD RTO rate (primary metric)
    cod_rto_rate = _pct(cod_df["is_rto"].sum(), len(cod_df)) if len(cod_df) > 0 else 0.0
    prepaid_rto_rate = _pct(prepaid_df["is_rto"].sum(), len(prepaid_df)) if len(prepaid_df) > 0 else 0.0

    # rto_rate IS cod_rto_rate
    rto_rate = cod_rto_rate

    # Avg order value
    avg_order_value = _round(df["price"].mean())

    # Address corrected %
    addr_corrected_pct = _pct(df["was_adfix_corrected"].sum(), total)

    # Avg manifest latency days
    avg_manifest_latency = _round(df["manifest_latency_days"].mean())

    # Repeat buyer % (buyer_id appears in >1 order)
    buyer_counts = df["buyer_id"].value_counts()
    repeat_buyers = buyer_counts[buyer_counts > 1].index
    repeat_orders = len(df[df["buyer_id"].isin(repeat_buyers)])
    repeat_buyer_pct = _pct(repeat_orders, total)

    # RTO rate by payment mode (COD-only RTO for each)
    payment_rto = {
        "COD": cod_rto_rate,
        "Pre-paid": prepaid_rto_rate,
    }

    # Top 5 destination cities by COD volume with COD RTO rates
    cod_city_stats = cod_df.groupby("destination_city").agg(
        total=("order_id", "count"),
        rto=("is_rto", "sum"),
    ).nlargest(5, "total")
    city_rto = {
        row.Index: _pct(row.rto, row.total)
        for row in cod_city_stats.itertuples()
    }

    # COD RTO rate by origin state
    cod_state_stats = cod_df.groupby("origin_state").agg(
        total=("order_id", "count"),
        rto=("is_rto", "sum"),
    ).nlargest(5, "total")
    state_rto = {
        row.Index: _pct(row.rto, row.total)
        for row in cod_state_stats.itertuples()
    }

    return {
        "rto_rate": rto_rate,
        "cod_pct": cod_pct,
        "cod_rto_rate": cod_rto_rate,
        "prepaid_rto_rate": prepaid_rto_rate,
        "avg_order_value": avg_order_value,
        "addr_corrected_pct": addr_corrected_pct,
        "avg_manifest_latency": avg_manifest_latency,
        "repeat_buyer_pct": repeat_buyer_pct,
        "payment_rto": payment_rto,
        "city_rto": city_rto,
        "state_rto": state_rto,
    }


# ---------------------------------------------------------------------------
# Pincode-level insight within top 3 destination cities
# ---------------------------------------------------------------------------

def _compute_city_pincode_rto(
    client_df: pd.DataFrame, peer_df: pd.DataFrame,
) -> dict[str, dict[str, float]]:
    """For top 3 destination cities, show pincode-level COD RTO for client vs peers."""
    result: dict[str, dict[str, float]] = {}

    # Check if destination_pincode column exists and has data
    if "destination_pincode" not in client_df.columns:
        return result

    client_cod = client_df[client_df["payment_mode"] == "COD"]
    if client_cod.empty:
        return result

    # Top 3 cities by COD volume for this client
    top_cities = (
        client_cod.groupby("destination_city")["order_id"]
        .count()
        .nlargest(3)
        .index.tolist()
    )

    peer_cod = peer_df[peer_df["payment_mode"] == "COD"]

    for city in top_cities:
        city_client = client_cod[client_cod["destination_city"] == city]
        city_peer = peer_cod[peer_cod["destination_city"] == city]

        # Get pincodes with enough volume
        pin_stats = city_client.groupby("destination_pincode").agg(
            total=("order_id", "count"),
            rto=("is_rto", "sum"),
        )
        pin_stats = pin_stats[pin_stats["total"] >= 3]  # min 3 orders
        if pin_stats.empty:
            continue

        top_pins = pin_stats.nlargest(5, "total")
        pin_data: dict[str, float] = {}
        for pin_row in top_pins.itertuples():
            pin = str(pin_row.Index)
            if not pin or pin in ("", "nan"):
                continue
            client_pin_rto = _pct(pin_row.rto, pin_row.total)

            # Peer RTO for same pincode
            peer_pin = city_peer[city_peer["destination_pincode"] == pin]
            peer_pin_rto = _pct(peer_pin["is_rto"].sum(), len(peer_pin)) if len(peer_pin) >= 3 else -1

            pin_data[pin] = {
                "client_rto": client_pin_rto,
                "peer_rto": peer_pin_rto,
                "client_orders": int(pin_row.total),
            }

        if pin_data:
            result[city] = pin_data

    return result


# ---------------------------------------------------------------------------
# Diagnosis engine — consulting report style
# ---------------------------------------------------------------------------

def _diagnose(
    client_df: pd.DataFrame,
    peer_df: pd.DataFrame,
    cm: dict,
    pm: dict,
    client_info: dict,
    per_peer: list[dict],
) -> dict[str, Any]:
    """Generate consulting-style diagnosis with strengths, improvements, and peer learnings."""
    if not cm or not pm:
        return {
            "overall_assessment": "Insufficient data to generate assessment.",
            "strengths": [],
            "improvement_areas": [],
            "peer_learnings": [],
        }

    client_name = client_info.get("name", "This client")
    rto_gap = cm.get("rto_rate", 0) - pm.get("rto_rate", 0)
    rto_gap_rounded = round(rto_gap, 1)

    # ── Overall Assessment ──
    if rto_gap > 3:
        assessment = (
            f"{client_name} has a COD RTO rate of {cm['rto_rate']}%, which is "
            f"{abs(rto_gap_rounded)}pp above the peer average of {pm['rto_rate']}%. "
            f"This indicates significant room for improvement in COD order management."
        )
    elif rto_gap > 0:
        assessment = (
            f"{client_name} has a COD RTO rate of {cm['rto_rate']}%, which is "
            f"{abs(rto_gap_rounded)}pp above the peer average of {pm['rto_rate']}%. "
            f"While not significantly worse, there are specific areas where improvement is possible."
        )
    elif rto_gap > -2:
        assessment = (
            f"{client_name} has a COD RTO rate of {cm['rto_rate']}%, roughly in line with "
            f"the peer average of {pm['rto_rate']}%. Performance is competitive, but "
            f"targeted optimizations could still yield gains."
        )
    else:
        assessment = (
            f"{client_name} has a COD RTO rate of {cm['rto_rate']}%, which is "
            f"{abs(rto_gap_rounded)}pp below the peer average of {pm['rto_rate']}%. "
            f"This is strong performance. Below are areas of strength and minor opportunities."
        )

    # ── Strengths ──
    strengths: list[dict] = []

    if cm.get("avg_manifest_latency", 99) < pm.get("avg_manifest_latency", 0):
        strengths.append({
            "area": "Manifest Speed",
            "detail": (
                f"Your avg manifest latency of {cm['avg_manifest_latency']} days is faster "
                f"than peer avg of {pm['avg_manifest_latency']} days."
            ),
        })

    if cm.get("rto_rate", 100) < pm.get("rto_rate", 0):
        strengths.append({
            "area": "COD RTO Rate",
            "detail": (
                f"Your COD RTO rate of {cm['rto_rate']}% is lower than "
                f"peer avg of {pm['rto_rate']}%."
            ),
        })

    if cm.get("repeat_buyer_pct", 0) > pm.get("repeat_buyer_pct", 100):
        strengths.append({
            "area": "Repeat Buyers",
            "detail": (
                f"Your repeat buyer rate of {cm['repeat_buyer_pct']}% exceeds "
                f"peer avg of {pm['repeat_buyer_pct']}%. Repeat buyers have lower RTO risk."
            ),
        })

    if cm.get("addr_corrected_pct", 100) < pm.get("addr_corrected_pct", 0):
        strengths.append({
            "area": "Address Quality",
            "detail": (
                f"Only {cm['addr_corrected_pct']}% of your orders needed address correction "
                f"vs {pm['addr_corrected_pct']}% for peers — your address data is cleaner."
            ),
        })

    cod_pct_client = cm.get("cod_pct", 100)
    cod_pct_peer = pm.get("cod_pct", 0)
    if cod_pct_client < cod_pct_peer - 5:
        strengths.append({
            "area": "Prepaid Mix",
            "detail": (
                f"Your COD share is {cod_pct_client}% vs {cod_pct_peer}% for peers — "
                f"a healthier prepaid mix reduces RTO exposure."
            ),
        })

    # ── Improvement Areas ──
    improvements: list[dict] = []

    # COD dependency
    cod_gap = cod_pct_client - cod_pct_peer
    if cod_gap > 10 and cm.get("cod_rto_rate", 0) > 15:
        improvements.append({
            "area": "COD Dependency",
            "detail": (
                f"{cod_pct_client}% of your orders are COD vs {cod_pct_peer}% peer avg — "
                f"and your COD RTO is {cm['cod_rto_rate']}% vs {pm.get('cod_rto_rate', 0)}% for peers. "
                f"Consider nudging high-risk COD orders to prepaid."
            ),
            "priority": "high",
        })
    elif cod_gap > 5:
        improvements.append({
            "area": "COD Dependency",
            "detail": (
                f"{cod_pct_client}% of your orders are COD vs {cod_pct_peer}% peer avg. "
                f"Reducing COD share could lower RTO exposure."
            ),
            "priority": "medium",
        })

    # Address issues
    addr_gap = cm.get("addr_corrected_pct", 0) - pm.get("addr_corrected_pct", 0)
    if addr_gap > 3:
        improvements.append({
            "area": "Address Quality",
            "detail": (
                f"{cm['addr_corrected_pct']}% of orders needed address correction "
                f"vs {pm['addr_corrected_pct']}% for peers. Consider adding address "
                f"validation at checkout."
            ),
            "priority": "high" if addr_gap > 8 else "medium",
        })

    # Slow manifest
    manifest_gap = cm.get("avg_manifest_latency", 0) - pm.get("avg_manifest_latency", 0)
    if manifest_gap > 0.5:
        improvements.append({
            "area": "Manifest Speed",
            "detail": (
                f"Avg manifest-to-pickup is {cm['avg_manifest_latency']} days "
                f"vs {pm['avg_manifest_latency']} days for peers. Faster manifesting "
                f"reduces buyer cancellations."
            ),
            "priority": "high" if manifest_gap > 1 else "medium",
        })

    # City problems
    client_city_rto = cm.get("city_rto", {})
    peer_city_rto = pm.get("city_rto", {})
    problem_cities = []
    for city, rto in client_city_rto.items():
        peer_rto = peer_city_rto.get(city, 0)
        if rto > peer_rto + 5:
            problem_cities.append((city, rto, peer_rto))
    if problem_cities:
        problem_cities.sort(key=lambda x: x[1] - x[2], reverse=True)
        top = problem_cities[0]
        improvements.append({
            "area": "Destination Focus",
            "detail": (
                f"Your orders to {top[0]} have {top[1]}% COD RTO vs {top[2]}% for peers. "
                f"Consider reviewing COD policies for this destination."
            ),
            "priority": "medium",
        })

    # Low repeat
    repeat_gap = pm.get("repeat_buyer_pct", 0) - cm.get("repeat_buyer_pct", 0)
    if repeat_gap > 5:
        improvements.append({
            "area": "Repeat Buyers",
            "detail": (
                f"Repeat buyer rate is {cm['repeat_buyer_pct']}% vs {pm['repeat_buyer_pct']}% "
                f"for peers. New buyers have higher RTO risk — loyalty programs could help."
            ),
            "priority": "low",
        })

    # ── Peer Learnings ──
    peer_learnings: list[str] = []

    # Sort peers by RTO rate (best first)
    sorted_peers = sorted(per_peer, key=lambda p: p.get("rto_rate", 100))

    for peer in sorted_peers[:3]:
        p_name = peer.get("name", "Unknown")
        p_rto = peer.get("rto_rate", 0)
        p_cod_pct = peer.get("cod_pct", 0)
        p_orders = peer.get("order_count", 0)

        # Find what makes this peer different
        if p_rto < cm.get("rto_rate", 0):
            if p_cod_pct < cod_pct_client - 10:
                peer_learnings.append(
                    f"{p_name} achieves {p_rto}% COD RTO with only {p_cod_pct}% COD "
                    f"(vs your {cod_pct_client}%) across {p_orders} orders — "
                    f"their lower COD share is a key differentiator."
                )
            elif peer.get("avg_manifest_latency", 99) < cm.get("avg_manifest_latency", 0) - 0.3:
                peer_learnings.append(
                    f"{p_name} has {p_rto}% COD RTO with manifest latency of "
                    f"{peer['avg_manifest_latency']} days (vs your {cm['avg_manifest_latency']} days) — "
                    f"faster processing correlates with lower RTO."
                )
            else:
                peer_learnings.append(
                    f"{p_name} achieves {p_rto}% COD RTO across {p_orders} orders "
                    f"with {p_cod_pct}% COD share."
                )

    # If no learnings yet, add a generic one
    if not peer_learnings and sorted_peers:
        best = sorted_peers[0]
        peer_learnings.append(
            f"Top peer {best.get('name', 'Unknown')} has {best.get('rto_rate', 0)}% COD RTO "
            f"across {best.get('order_count', 0)} orders."
        )

    return {
        "overall_assessment": assessment,
        "strengths": strengths,
        "improvement_areas": improvements,
        "peer_learnings": peer_learnings,
    }
