"""Query functions for the Delhivery Commerce AI data layer.

All functions accept a database connection (sqlite3.Connection or
PgConnectionWrapper) and return plain dicts.

When DATABASE_URL is set (Postgres mode), several queries use materialized
views for performance instead of scanning the full orders table.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import config as cfg
from data.db import is_postgres

# ---------------------------------------------------------------------------
# Simple in-process TTL cache
# ---------------------------------------------------------------------------

_cache: dict[str, tuple[float, Any]] = {}


def _cache_get(key: str) -> Any:
    entry = _cache.get(key)
    if entry and time.monotonic() < entry[0]:
        return entry[1]
    return None


def _cache_set(key: str, value: Any, ttl: int = cfg.QUERY_CACHE_TTL) -> None:
    _cache[key] = (time.monotonic() + ttl, value)


def invalidate_merchant_cache(merchant_id: str) -> None:
    keys_to_drop = [k for k in _cache if merchant_id in k]
    for k in keys_to_drop:
        _cache.pop(k, None)


def _rows_to_dicts(cursor) -> List[dict]:
    """Convert all rows from a cursor into a list of plain dicts."""
    rows = cursor.fetchall()
    if not rows:
        return []
    # PgCursorWrapper already returns dicts; sqlite3.Row needs dict()
    if isinstance(rows[0], dict):
        return rows
    return [dict(row) for row in rows]


def _row_to_dict(cursor) -> Optional[dict]:
    """Fetch one row and return it as a dict, or None."""
    row = cursor.fetchone()
    if row is None:
        return None
    if isinstance(row, dict):
        return row
    return dict(row)



# ---------------------------------------------------------------------------
# Merchant snapshot
# ---------------------------------------------------------------------------

def get_merchant_snapshot(db, merchant_id: str) -> dict:
    """Return merchant info, warehouse nodes, distributions, and benchmark gaps."""
    cache_key = f"snapshot:{merchant_id}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    merchant = _row_to_dict(
        db.execute("SELECT * FROM merchants WHERE merchant_id = ?", (merchant_id,))
    )
    if merchant is None:
        return {}

    # Warehouse nodes — Postgres schema may not have this table populated
    try:
        warehouse_nodes = _rows_to_dicts(
            db.execute(
                "SELECT * FROM warehouse_nodes WHERE merchant_id = ? AND is_active = 1",
                (merchant_id,),
            )
        )
    except Exception:
        warehouse_nodes = []

    category_dist = _rows_to_dicts(
        db.execute(
            "SELECT category, COUNT(*) AS order_count FROM orders WHERE merchant_id = ? GROUP BY category",
            (merchant_id,),
        )
    )

    price_band_dist = _rows_to_dicts(
        db.execute(
            "SELECT price_band, COUNT(*) AS order_count FROM orders WHERE merchant_id = ? GROUP BY price_band",
            (merchant_id,),
        )
    )

    payment_mode_dist = _rows_to_dicts(
        db.execute(
            "SELECT payment_mode, COUNT(*) AS order_count FROM orders WHERE merchant_id = ? GROUP BY payment_mode",
            (merchant_id,),
        )
    )

    # Benchmark gaps — use materialized views on Postgres, sampling on SQLite
    if is_postgres():
        benchmark_gaps = _rows_to_dicts(
            db.execute(
                """
                SELECT
                    mc.category, mc.price_band, mc.payment_mode,
                    mc.order_count,
                    ROUND((mc.rto_rate * 100)::numeric, 1) AS merchant_rto_rate,
                    ROUND((pb.peer_rto_rate * 100)::numeric, 1) AS peer_rto_rate,
                    pb.total_orders AS peer_total_orders,
                    ROUND(((mc.rto_rate - pb.peer_rto_rate) * 100)::numeric, 1) AS rto_gap
                FROM mv_merchant_cohort_stats mc
                JOIN mv_peer_benchmarks pb
                    ON mc.category = pb.category
                    AND mc.price_band = pb.price_band
                    AND mc.payment_mode = pb.payment_mode
                    AND mc.origin_state IS NOT DISTINCT FROM pb.origin_state
                    AND mc.destination_city IS NOT DISTINCT FROM pb.destination_city
                WHERE mc.merchant_id = ?
                ORDER BY ABS(mc.rto_rate - pb.peer_rto_rate) DESC
                """,
                (merchant_id,),
            )
        )
    else:
        SAMPLE = "SELECT * FROM orders ORDER BY ROWID LIMIT 1000000"
        benchmark_gaps = _rows_to_dicts(
            db.execute(
                f"""
                WITH sample AS ({SAMPLE}),
                merchant_stats AS (
                    SELECT category, price_band, payment_mode,
                        COUNT(*) AS order_count,
                        ROUND(100.0 * AVG(CASE WHEN delivery_outcome = 'rto' THEN 1.0 ELSE 0.0 END), 1) AS merchant_rto_rate
                    FROM orders WHERE merchant_id = ?
                    GROUP BY category, price_band, payment_mode
                ),
                peer_stats AS (
                    SELECT category, price_band, payment_mode,
                        COUNT(*) AS peer_total_orders,
                        ROUND(100.0 * AVG(CASE WHEN delivery_outcome = 'rto' THEN 1.0 ELSE 0.0 END), 1) AS peer_rto_rate
                    FROM sample WHERE merchant_id != ?
                    GROUP BY category, price_band, payment_mode
                    HAVING COUNT(*) >= 10
                )
                SELECT m.category, m.price_band, m.payment_mode, m.order_count,
                    m.merchant_rto_rate, p.peer_rto_rate, p.peer_total_orders,
                    ROUND(m.merchant_rto_rate - p.peer_rto_rate, 1) AS rto_gap
                FROM merchant_stats m
                INNER JOIN peer_stats p ON m.category = p.category AND m.price_band = p.price_band AND m.payment_mode = p.payment_mode
                ORDER BY ABS(ROUND(m.merchant_rto_rate - p.peer_rto_rate, 1)) DESC
                """,
                (merchant_id, merchant_id),
            )
        )

    result = {
        "merchant_id": merchant["merchant_id"],
        "name": merchant.get("name"),
        "warehouse_nodes": warehouse_nodes,
        "category_distribution": {r["category"]: r["order_count"] for r in category_dist},
        "price_band_distribution": {r["price_band"]: r["order_count"] for r in price_band_dist},
        "payment_mode_distribution": {r["payment_mode"]: r["order_count"] for r in payment_mode_dist},
        "benchmark_gaps": benchmark_gaps,
    }
    _cache_set(cache_key, result)
    return result



# ---------------------------------------------------------------------------
# Cohort benchmarks
# ---------------------------------------------------------------------------

def get_cohort_benchmarks(db, merchant_id: str) -> list[dict]:
    if is_postgres():
        return _rows_to_dicts(
            db.execute(
                """
                SELECT category, price_band, payment_mode,
                    origin_state AS origin_node, destination_city AS destination_cluster,
                    order_count, delivery_rate
                FROM mv_merchant_cohort_stats
                WHERE merchant_id = ?
                """,
                (merchant_id,),
            )
        )
    return _rows_to_dicts(
        db.execute(
            """
            SELECT category, price_band, payment_mode, origin_node, destination_cluster,
                COUNT(*) AS order_count,
                AVG(CASE WHEN delivery_outcome = 'delivered' THEN 1.0 ELSE 0.0 END) AS delivery_rate
            FROM orders WHERE merchant_id = ?
            GROUP BY category, price_band, payment_mode, origin_node, destination_cluster
            """,
            (merchant_id,),
        )
    )


# ---------------------------------------------------------------------------
# Peer benchmarks
# ---------------------------------------------------------------------------

def get_peer_benchmarks(db, merchant_id: str, category: str, price_band: str) -> list[dict]:
    if is_postgres():
        return _rows_to_dicts(
            db.execute(
                """
                SELECT
                    mc.category || '|' || mc.price_band || '|' || mc.payment_mode
                        || '|' || COALESCE(mc.origin_state,'') || '|' || COALESCE(mc.destination_city,'') AS cohort_key,
                    mc.delivery_rate AS merchant_score,
                    COALESCE(pb.peer_delivery_rate, 0.0) AS peer_avg_score,
                    COALESCE(pb.total_orders, 0) AS peer_sample_size,
                    (mc.delivery_rate - COALESCE(pb.peer_delivery_rate, 0.0)) AS gap
                FROM mv_merchant_cohort_stats mc
                LEFT JOIN mv_peer_benchmarks pb
                    ON mc.category = pb.category AND mc.price_band = pb.price_band
                    AND mc.payment_mode = pb.payment_mode
                WHERE mc.merchant_id = ? AND mc.category = ? AND mc.price_band = ?
                """,
                (merchant_id, category, price_band),
            )
        )
    return _rows_to_dicts(
        db.execute(
            """
            WITH merchant_cohorts AS (
                SELECT
                    category || '|' || price_band || '|' || payment_mode
                        || '|' || origin_node || '|' || destination_cluster AS cohort_key,
                    AVG(CASE WHEN delivery_outcome = 'delivered' THEN 1.0 ELSE 0.0 END) AS merchant_score
                FROM orders
                WHERE merchant_id = ? AND category = ? AND price_band = ?
                GROUP BY cohort_key
            ),
            peer_cohorts AS (
                SELECT
                    category || '|' || price_band || '|' || payment_mode
                        || '|' || origin_node || '|' || destination_cluster AS cohort_key,
                    AVG(CASE WHEN delivery_outcome = 'delivered' THEN 1.0 ELSE 0.0 END) AS peer_avg_score,
                    COUNT(*) AS peer_sample_size
                FROM orders
                WHERE merchant_id != ? AND category = ? AND price_band = ?
                GROUP BY cohort_key
            )
            SELECT m.cohort_key, m.merchant_score,
                COALESCE(p.peer_avg_score, 0.0) AS peer_avg_score,
                COALESCE(p.peer_sample_size, 0) AS peer_sample_size,
                (m.merchant_score - COALESCE(p.peer_avg_score, 0.0)) AS gap
            FROM merchant_cohorts m
            LEFT JOIN peer_cohorts p ON m.cohort_key = p.cohort_key
            """,
            (merchant_id, category, price_band, merchant_id, category, price_band),
        )
    )


# ---------------------------------------------------------------------------
# Recent orders
# ---------------------------------------------------------------------------

def get_recent_orders(db, merchant_id: str, limit: int = 50) -> list[dict]:
    return _rows_to_dicts(
        db.execute(
            "SELECT * FROM orders WHERE merchant_id = ? ORDER BY created_at DESC LIMIT ?",
            (merchant_id, limit),
        )
    )


# ---------------------------------------------------------------------------
# Historical analogs
# ---------------------------------------------------------------------------

def get_historical_analogs(
    db, category: str, price_band: str, payment_mode: str,
    origin_node: str, destination_cluster: str, min_orders: int = 50,
) -> dict:
    # Postgres uses origin_state/destination_city; SQLite uses origin_node/destination_cluster
    origin_col = "origin_state" if is_postgres() else "origin_node"
    dest_col = "destination_city" if is_postgres() else "destination_cluster"
    row = _row_to_dict(
        db.execute(
            f"""
            SELECT COUNT(*) AS sample_size,
                AVG(CASE WHEN delivery_outcome != 'delivered' THEN 1.0 ELSE 0.0 END) AS rto_rate
            FROM orders
            WHERE category = ? AND price_band = ? AND payment_mode = ?
              AND {origin_col} = ? AND {dest_col} = ?
            """,
            (category, price_band, payment_mode, origin_node, destination_cluster),
        )
    )
    peer_row = _row_to_dict(
        db.execute(
            """
            SELECT AVG(CASE WHEN delivery_outcome != 'delivered' THEN 1.0 ELSE 0.0 END) AS peer_avg_rto_rate
            FROM orders WHERE category = ? AND price_band = ?
            """,
            (category, price_band),
        )
    )
    sample_size = (row or {}).get("sample_size", 0) or 0
    rto_rate = (row or {}).get("rto_rate", 0.0) or 0.0
    peer_avg_rto_rate = (peer_row or {}).get("peer_avg_rto_rate", 0.0) or 0.0
    return {
        "rto_rate": rto_rate if sample_size >= min_orders else None,
        "sample_size": sample_size,
        "peer_avg_rto_rate": peer_avg_rto_rate,
    }



# ---------------------------------------------------------------------------
# Intervention history & counts
# ---------------------------------------------------------------------------

def get_intervention_history(db, merchant_id: str, period_days: int = 30) -> list[dict]:
    cutoff = (datetime.utcnow() - timedelta(days=period_days)).isoformat()
    return _rows_to_dicts(
        db.execute(
            "SELECT * FROM interventions WHERE merchant_id = ? AND executed_at >= ? ORDER BY executed_at DESC",
            (merchant_id, cutoff),
        )
    )


def get_intervention_counts(db, merchant_id: str, period_days: int = 30) -> dict:
    cache_key = f"intervention_counts:{merchant_id}:{period_days}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    cutoff = (datetime.utcnow() - timedelta(days=period_days)).isoformat()

    by_type = _rows_to_dicts(
        db.execute(
            "SELECT intervention_type, COUNT(*) AS count FROM interventions WHERE merchant_id = ? AND executed_at >= ? GROUP BY intervention_type",
            (merchant_id, cutoff),
        )
    )
    by_outcome = _rows_to_dicts(
        db.execute(
            "SELECT outcome, COUNT(*) AS count FROM interventions WHERE merchant_id = ? AND executed_at >= ? AND outcome IS NOT NULL GROUP BY outcome",
            (merchant_id, cutoff),
        )
    )
    total = sum(r["count"] for r in by_type)
    result = {
        "by_type": {r["intervention_type"]: r["count"] for r in by_type},
        "by_outcome": {r["outcome"]: r["count"] for r in by_outcome},
        "total": total,
    }
    _cache_set(cache_key, result)
    return result


# ---------------------------------------------------------------------------
# Rate limits
# ---------------------------------------------------------------------------

def check_rate_limits(db, merchant_id: str) -> dict:
    now = datetime.utcnow()
    daily_cutoff = (now - timedelta(hours=24)).isoformat()
    hourly_cutoff = (now - timedelta(hours=1)).isoformat()

    daily_row = _row_to_dict(
        db.execute("SELECT COUNT(*) AS cnt FROM interventions WHERE merchant_id = ? AND executed_at >= ?", (merchant_id, daily_cutoff))
    )
    hourly_row = _row_to_dict(
        db.execute("SELECT COUNT(*) AS cnt FROM interventions WHERE merchant_id = ? AND executed_at >= ?", (merchant_id, hourly_cutoff))
    )
    caps_row = _row_to_dict(
        db.execute("SELECT MAX(daily_cap) AS daily_cap, MAX(hourly_cap) AS hourly_cap FROM merchant_permissions WHERE merchant_id = ?", (merchant_id,))
    )

    daily_used = (daily_row or {}).get("cnt", 0) or 0
    hourly_used = (hourly_row or {}).get("cnt", 0) or 0
    daily_cap = (caps_row or {}).get("daily_cap", 500) or 500
    hourly_cap = (caps_row or {}).get("hourly_cap", 100) or 100

    return {
        "daily_used": daily_used,
        "daily_cap": daily_cap,
        "hourly_used": hourly_used,
        "hourly_cap": hourly_cap,
        "is_within_limits": daily_used <= daily_cap and hourly_used <= hourly_cap,
    }


# ---------------------------------------------------------------------------
# Log intervention
# ---------------------------------------------------------------------------

def log_intervention(db, intervention: dict) -> None:
    db.execute(
        """
        INSERT INTO interventions (
            intervention_id, order_id, merchant_id, intervention_type,
            action_owner, initiated_by, confidence_score, outcome,
            executed_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            intervention["intervention_id"],
            intervention["order_id"],
            intervention["merchant_id"],
            intervention["intervention_type"],
            intervention["action_owner"],
            intervention["initiated_by"],
            intervention.get("confidence_score"),
            intervention.get("outcome"),
            intervention["executed_at"],
            intervention.get("completed_at"),
        ),
    )
    db.commit()


# ---------------------------------------------------------------------------
# Merchant permissions
# ---------------------------------------------------------------------------

def get_merchant_permissions(db, merchant_id: str) -> dict:
    rows = _rows_to_dicts(
        db.execute("SELECT * FROM merchant_permissions WHERE merchant_id = ?", (merchant_id,))
    )
    if not rows:
        return {
            "merchant_id": merchant_id,
            "permissions": {},
            "daily_cap": 500,
            "hourly_cap": 100,
            "auto_cancel_enabled": False,
            "auto_cancel_threshold": 0.9,
            "express_upgrade_enabled": False,
            "impulse_categories": ["fashion", "beauty"],
        }

    permissions: dict[str, bool] = {}
    daily_cap = 500
    hourly_cap = 100
    auto_cancel_enabled = False
    auto_cancel_threshold = 0.9
    express_upgrade_enabled = False
    impulse_categories: list[str] = ["fashion", "beauty"]

    for row in rows:
        itype = row["intervention_type"]
        permissions[itype] = bool(row["is_enabled"])
        daily_cap = max(daily_cap, row.get("daily_cap") or 500)
        hourly_cap = max(hourly_cap, row.get("hourly_cap") or 100)
        if row.get("auto_cancel_enabled"):
            auto_cancel_enabled = True
        if row.get("auto_cancel_threshold") is not None:
            auto_cancel_threshold = row["auto_cancel_threshold"]
        if row.get("express_upgrade_enabled"):
            express_upgrade_enabled = True
        if row.get("impulse_categories"):
            impulse_categories = [c.strip() for c in row["impulse_categories"].split(",") if c.strip()]

    return {
        "merchant_id": merchant_id,
        "permissions": permissions,
        "daily_cap": daily_cap,
        "hourly_cap": hourly_cap,
        "auto_cancel_enabled": auto_cancel_enabled,
        "auto_cancel_threshold": auto_cancel_threshold,
        "express_upgrade_enabled": express_upgrade_enabled,
        "impulse_categories": impulse_categories,
    }



# ---------------------------------------------------------------------------
# Customer delivered orders (first-time buyer check)
# ---------------------------------------------------------------------------

def get_customer_delivered_orders(db, customer_ucid: str, merchant_id: str) -> list[dict]:
    # Postgres uses buyer_id; SQLite uses customer_ucid
    col = "buyer_id" if is_postgres() else "customer_ucid"
    return _rows_to_dicts(
        db.execute(
            f"""
            SELECT * FROM orders
            WHERE {col} = ? AND merchant_id = ? AND delivery_outcome = 'delivered'
            ORDER BY created_at DESC
            """,
            (customer_ucid, merchant_id),
        )
    )


# ---------------------------------------------------------------------------
# Cluster RTO rate
# ---------------------------------------------------------------------------

def get_cluster_rto_rate(db, destination_cluster: str, payment_mode: str = "COD") -> float:
    # Postgres uses destination_city; SQLite uses destination_cluster
    col = "destination_city" if is_postgres() else "destination_cluster"
    row = _row_to_dict(
        db.execute(
            f"""
            SELECT AVG(CASE WHEN delivery_outcome != 'delivered' THEN 1.0 ELSE 0.0 END) AS rto_rate
            FROM orders WHERE {col} = ? AND payment_mode = ?
            """,
            (destination_cluster, payment_mode),
        )
    )
    return (row or {}).get("rto_rate", 0.0) or 0.0


# ---------------------------------------------------------------------------
# Merchant list (for client dropdown)
# ---------------------------------------------------------------------------

def get_all_merchants(db) -> list[dict]:
    cache_key = "all_merchants"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    if is_postgres():
        rows = _rows_to_dicts(
            db.execute(
                """
                SELECT merchant_id, name, total_orders AS order_count
                FROM mv_merchant_summary
                ORDER BY total_orders DESC
                LIMIT 200
                """
            )
        )
    else:
        rows = _rows_to_dicts(
            db.execute(
                """
                SELECT o.merchant_id, m.name, COUNT(*) AS order_count
                FROM (SELECT merchant_id FROM orders ORDER BY ROWID LIMIT 2000000) o
                JOIN merchants m ON m.merchant_id = o.merchant_id
                GROUP BY o.merchant_id
                ORDER BY order_count DESC
                LIMIT 200
                """
            )
        )
    _cache_set(cache_key, rows, ttl=300)
    return rows


# ---------------------------------------------------------------------------
# Demand map (destination cluster order + RTO counts)
# ---------------------------------------------------------------------------

def get_demand_map(db, merchant_id: str) -> list[dict]:
    cache_key = f"demand_map:{merchant_id}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    if is_postgres():
        rows = _rows_to_dicts(
            db.execute(
                """
                SELECT destination_city AS city, order_count, rto_count, rto_rate_pct AS rto_rate
                FROM mv_demand_map
                WHERE merchant_id = ?
                ORDER BY order_count DESC
                """,
                (merchant_id,),
            )
        )
    else:
        rows = _rows_to_dicts(
            db.execute(
                """
                SELECT destination_cluster AS city, COUNT(*) AS order_count,
                    SUM(CASE WHEN delivery_outcome = 'rto' THEN 1 ELSE 0 END) AS rto_count,
                    ROUND(100.0 * AVG(CASE WHEN delivery_outcome = 'rto' THEN 1.0 ELSE 0.0 END), 1) AS rto_rate
                FROM orders WHERE merchant_id = ?
                GROUP BY destination_cluster
                ORDER BY order_count DESC
                """,
                (merchant_id,),
            )
        )
    _cache_set(cache_key, rows, ttl=120)
    return rows


def get_demand_map_filtered(
    db, merchant_id: str,
    category: str | None = None,
    price_band: str | None = None,
    payment_mode: str | None = None,
) -> list[dict]:
    """Demand map filtered by cohort dimensions. Queries orders table directly."""
    dest_col = "destination_city" if is_postgres() else "destination_cluster"

    conditions = [f"merchant_id = ?", f"delivery_outcome IN ('delivered', 'rto')"]
    params: list = [merchant_id]

    if category:
        conditions.append("category = ?")
        params.append(category)
    if price_band:
        conditions.append("price_band = ?")
        params.append(price_band)
    if payment_mode:
        conditions.append("payment_mode = ?")
        params.append(payment_mode)

    where = " AND ".join(conditions)

    return _rows_to_dicts(
        db.execute(
            f"""
            SELECT {dest_col} AS city, COUNT(*) AS order_count,
                SUM(CASE WHEN delivery_outcome = 'rto' THEN 1 ELSE 0 END) AS rto_count,
                ROUND(100.0 * AVG(CASE WHEN delivery_outcome = 'rto' THEN 1.0 ELSE 0.0 END){'::numeric' if is_postgres() else ''}, 1) AS rto_rate
            FROM orders
            WHERE {where}
            GROUP BY {dest_col}
            ORDER BY order_count DESC
            """,
            tuple(params),
        )
    )
