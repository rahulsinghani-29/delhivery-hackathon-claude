"""Query functions for the Delhivery Commerce AI data layer.

All functions accept a sqlite3.Connection (with row_factory=sqlite3.Row)
and return plain dicts.  Pydantic models are layered on top in Task 2.
"""

from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import config as cfg

# ---------------------------------------------------------------------------
# Simple in-process TTL cache
# ---------------------------------------------------------------------------

_cache: dict[str, tuple[float, Any]] = {}  # key → (expires_at, value)


def _cache_get(key: str) -> Any:
    """Return cached value if still valid, otherwise None."""
    entry = _cache.get(key)
    if entry and time.monotonic() < entry[0]:
        return entry[1]
    return None


def _cache_set(key: str, value: Any, ttl: int = cfg.QUERY_CACHE_TTL) -> None:
    _cache[key] = (time.monotonic() + ttl, value)


def invalidate_merchant_cache(merchant_id: str) -> None:
    """Call after any write that changes a merchant's aggregate data."""
    keys_to_drop = [k for k in _cache if merchant_id in k]
    for k in keys_to_drop:
        _cache.pop(k, None)


def _rows_to_dicts(cursor: sqlite3.Cursor) -> List[dict]:
    """Convert all rows from a cursor into a list of plain dicts."""
    return [dict(row) for row in cursor.fetchall()]


def _row_to_dict(cursor: sqlite3.Cursor) -> Optional[dict]:
    """Fetch one row and return it as a dict, or None."""
    row = cursor.fetchone()
    return dict(row) if row else None


# ---------------------------------------------------------------------------
# Merchant snapshot
# ---------------------------------------------------------------------------

def get_merchant_snapshot(db: sqlite3.Connection, merchant_id: str) -> dict:
    """Return merchant info, warehouse nodes, distributions, and benchmark gaps."""
    cache_key = f"snapshot:{merchant_id}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    # Merchant info
    merchant = _row_to_dict(
        db.execute("SELECT * FROM merchants WHERE merchant_id = ?", (merchant_id,))
    )
    if merchant is None:
        return {}

    # Warehouse nodes
    warehouse_nodes = _rows_to_dicts(
        db.execute(
            "SELECT * FROM warehouse_nodes WHERE merchant_id = ? AND is_active = 1",
            (merchant_id,),
        )
    )

    # Category distribution
    category_dist = _rows_to_dicts(
        db.execute(
            """
            SELECT category, COUNT(*) AS order_count
            FROM orders
            WHERE merchant_id = ?
            GROUP BY category
            """,
            (merchant_id,),
        )
    )

    # Price band distribution
    price_band_dist = _rows_to_dicts(
        db.execute(
            """
            SELECT price_band, COUNT(*) AS order_count
            FROM orders
            WHERE merchant_id = ?
            GROUP BY price_band
            """,
            (merchant_id,),
        )
    )

    # Payment mode distribution
    payment_mode_dist = _rows_to_dicts(
        db.execute(
            """
            SELECT payment_mode, COUNT(*) AS order_count
            FROM orders
            WHERE merchant_id = ?
            GROUP BY payment_mode
            """,
            (merchant_id,),
        )
    )

    # Benchmark gaps: merchant RTO rate vs peer avg (grouped by category/price_band/payment_mode)
    # Uses a 1M-row ROWID sample for the peer query to keep response times under ~5s.
    # Peers with < 10 orders in the sample cohort are excluded to avoid unreliable comparisons.
    SAMPLE = "SELECT * FROM orders ORDER BY ROWID LIMIT 1000000"
    benchmark_gaps = _rows_to_dicts(
        db.execute(
            f"""
            WITH sample AS ({SAMPLE}),
            merchant_stats AS (
                SELECT
                    category,
                    price_band,
                    payment_mode,
                    COUNT(*) AS order_count,
                    ROUND(100.0 * AVG(CASE WHEN delivery_outcome = 'rto' THEN 1.0 ELSE 0.0 END), 1)
                        AS merchant_rto_rate
                FROM orders
                WHERE merchant_id = ?
                GROUP BY category, price_band, payment_mode
            ),
            peer_stats AS (
                SELECT
                    category,
                    price_band,
                    payment_mode,
                    COUNT(*) AS peer_total_orders,
                    ROUND(100.0 * AVG(CASE WHEN delivery_outcome = 'rto' THEN 1.0 ELSE 0.0 END), 1)
                        AS peer_rto_rate
                FROM sample
                WHERE merchant_id != ?
                GROUP BY category, price_band, payment_mode
                HAVING COUNT(*) >= 10
            )
            SELECT
                m.category,
                m.price_band,
                m.payment_mode,
                m.order_count,
                m.merchant_rto_rate,
                p.peer_rto_rate,
                p.peer_total_orders,
                ROUND(m.merchant_rto_rate - p.peer_rto_rate, 1) AS rto_gap
            FROM merchant_stats m
            INNER JOIN peer_stats p
                ON m.category = p.category
                AND m.price_band = p.price_band
                AND m.payment_mode = p.payment_mode
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
        "benchmark_gaps": benchmark_gaps,  # now uses rto_gap / merchant_rto_rate / peer_rto_rate
    }
    _cache_set(cache_key, result)
    return result


# ---------------------------------------------------------------------------
# Cohort benchmarks
# ---------------------------------------------------------------------------

def get_cohort_benchmarks(db: sqlite3.Connection, merchant_id: str) -> list[dict]:
    """Return cohort-level stats for a merchant grouped by cohort dimensions."""
    return _rows_to_dicts(
        db.execute(
            """
            SELECT
                category,
                price_band,
                payment_mode,
                origin_node,
                destination_cluster,
                COUNT(*) AS order_count,
                AVG(CASE WHEN delivery_outcome = 'delivered' THEN 1.0 ELSE 0.0 END) AS delivery_rate
            FROM orders
            WHERE merchant_id = ?
            GROUP BY category, price_band, payment_mode, origin_node, destination_cluster
            """,
            (merchant_id,),
        )
    )


# ---------------------------------------------------------------------------
# Peer benchmarks
# ---------------------------------------------------------------------------

def get_peer_benchmarks(
    db: sqlite3.Connection,
    merchant_id: str,
    category: str,
    price_band: str,
) -> list[dict]:
    """Compare merchant's cohort performance against peers in same category/price_band."""
    return _rows_to_dicts(
        db.execute(
            """
            WITH merchant_cohorts AS (
                SELECT
                    category || '|' || price_band || '|' || payment_mode
                        || '|' || origin_node || '|' || destination_cluster AS cohort_key,
                    AVG(CASE WHEN delivery_outcome = 'delivered' THEN 1.0 ELSE 0.0 END) AS merchant_score
                FROM orders
                WHERE merchant_id = ?
                  AND category = ?
                  AND price_band = ?
                GROUP BY cohort_key
            ),
            peer_cohorts AS (
                SELECT
                    category || '|' || price_band || '|' || payment_mode
                        || '|' || origin_node || '|' || destination_cluster AS cohort_key,
                    AVG(CASE WHEN delivery_outcome = 'delivered' THEN 1.0 ELSE 0.0 END) AS peer_avg_score,
                    COUNT(*) AS peer_sample_size
                FROM orders
                WHERE merchant_id != ?
                  AND category = ?
                  AND price_band = ?
                GROUP BY cohort_key
            )
            SELECT
                m.cohort_key,
                m.merchant_score,
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

def get_recent_orders(
    db: sqlite3.Connection, merchant_id: str, limit: int = 50
) -> list[dict]:
    """Return most recent orders for a merchant, sorted by created_at desc."""
    return _rows_to_dicts(
        db.execute(
            """
            SELECT *
            FROM orders
            WHERE merchant_id = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (merchant_id, limit),
        )
    )


# ---------------------------------------------------------------------------
# Historical analogs
# ---------------------------------------------------------------------------

def get_historical_analogs(
    db: sqlite3.Connection,
    category: str,
    price_band: str,
    payment_mode: str,
    origin_node: str,
    destination_cluster: str,
    min_orders: int = 50,
) -> dict:
    """Return historical stats for orders matching the given cohort dimensions."""
    row = _row_to_dict(
        db.execute(
            """
            SELECT
                COUNT(*) AS sample_size,
                AVG(CASE WHEN delivery_outcome != 'delivered' THEN 1.0 ELSE 0.0 END) AS rto_rate
            FROM orders
            WHERE category = ?
              AND price_band = ?
              AND payment_mode = ?
              AND origin_node = ?
              AND destination_cluster = ?
            """,
            (category, price_band, payment_mode, origin_node, destination_cluster),
        )
    )

    # Peer average RTO rate across all merchants for same category + price_band
    peer_row = _row_to_dict(
        db.execute(
            """
            SELECT
                AVG(CASE WHEN delivery_outcome != 'delivered' THEN 1.0 ELSE 0.0 END) AS peer_avg_rto_rate
            FROM orders
            WHERE category = ?
              AND price_band = ?
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

def get_intervention_history(
    db: sqlite3.Connection, merchant_id: str, period_days: int = 30
) -> list[dict]:
    """Return intervention logs for a merchant within the given period."""
    cutoff = (datetime.utcnow() - timedelta(days=period_days)).isoformat()
    return _rows_to_dicts(
        db.execute(
            """
            SELECT *
            FROM interventions
            WHERE merchant_id = ?
              AND executed_at >= ?
            ORDER BY executed_at DESC
            """,
            (merchant_id, cutoff),
        )
    )


def get_intervention_counts(
    db: sqlite3.Connection, merchant_id: str, period_days: int = 30
) -> dict:
    """Return intervention counts grouped by type and outcome."""
    cache_key = f"intervention_counts:{merchant_id}:{period_days}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    cutoff = (datetime.utcnow() - timedelta(days=period_days)).isoformat()

    by_type = _rows_to_dicts(
        db.execute(
            """
            SELECT intervention_type, COUNT(*) AS count
            FROM interventions
            WHERE merchant_id = ?
              AND executed_at >= ?
            GROUP BY intervention_type
            """,
            (merchant_id, cutoff),
        )
    )

    by_outcome = _rows_to_dicts(
        db.execute(
            """
            SELECT outcome, COUNT(*) AS count
            FROM interventions
            WHERE merchant_id = ?
              AND executed_at >= ?
              AND outcome IS NOT NULL
            GROUP BY outcome
            """,
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

def check_rate_limits(db: sqlite3.Connection, merchant_id: str) -> dict:
    """Check current intervention usage against daily and hourly caps."""
    now = datetime.utcnow()
    daily_cutoff = (now - timedelta(hours=24)).isoformat()
    hourly_cutoff = (now - timedelta(hours=1)).isoformat()

    daily_row = _row_to_dict(
        db.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM interventions
            WHERE merchant_id = ?
              AND executed_at >= ?
            """,
            (merchant_id, daily_cutoff),
        )
    )

    hourly_row = _row_to_dict(
        db.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM interventions
            WHERE merchant_id = ?
              AND executed_at >= ?
            """,
            (merchant_id, hourly_cutoff),
        )
    )

    # Get caps from merchant_permissions (use max across intervention types)
    caps_row = _row_to_dict(
        db.execute(
            """
            SELECT
                MAX(daily_cap) AS daily_cap,
                MAX(hourly_cap) AS hourly_cap
            FROM merchant_permissions
            WHERE merchant_id = ?
            """,
            (merchant_id,),
        )
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

def log_intervention(db: sqlite3.Connection, intervention: dict) -> None:
    """Insert an intervention log entry."""
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

def get_merchant_permissions(db: sqlite3.Connection, merchant_id: str) -> dict:
    """Return merchant permissions including auto_cancel and express_upgrade config."""
    rows = _rows_to_dicts(
        db.execute(
            """
            SELECT *
            FROM merchant_permissions
            WHERE merchant_id = ?
            """,
            (merchant_id,),
        )
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
        # Take the max caps across all permission rows
        daily_cap = max(daily_cap, row.get("daily_cap") or 500)
        hourly_cap = max(hourly_cap, row.get("hourly_cap") or 100)
        # Auto-cancel config
        if row.get("auto_cancel_enabled"):
            auto_cancel_enabled = True
        if row.get("auto_cancel_threshold") is not None:
            auto_cancel_threshold = row["auto_cancel_threshold"]
        # Express upgrade config
        if row.get("express_upgrade_enabled"):
            express_upgrade_enabled = True
        # Impulse categories (comma-separated string)
        if row.get("impulse_categories"):
            impulse_categories = [
                c.strip() for c in row["impulse_categories"].split(",") if c.strip()
            ]

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

def get_customer_delivered_orders(
    db: sqlite3.Connection, customer_ucid: str, merchant_id: str
) -> list[dict]:
    """Return all delivered orders for a customer with a specific merchant."""
    return _rows_to_dicts(
        db.execute(
            """
            SELECT *
            FROM orders
            WHERE customer_ucid = ?
              AND merchant_id = ?
              AND delivery_outcome = 'delivered'
            ORDER BY created_at DESC
            """,
            (customer_ucid, merchant_id),
        )
    )


# ---------------------------------------------------------------------------
# Cluster RTO rate
# ---------------------------------------------------------------------------

def get_cluster_rto_rate(
    db: sqlite3.Connection, destination_cluster: str, payment_mode: str = "COD"
) -> float:
    """Return the RTO rate for a destination cluster filtered by payment mode."""
    row = _row_to_dict(
        db.execute(
            """
            SELECT
                AVG(CASE WHEN delivery_outcome != 'delivered' THEN 1.0 ELSE 0.0 END) AS rto_rate
            FROM orders
            WHERE destination_cluster = ?
              AND payment_mode = ?
            """,
            (destination_cluster, payment_mode),
        )
    )
    return (row or {}).get("rto_rate", 0.0) or 0.0


# ---------------------------------------------------------------------------
# Merchant list (for client dropdown)
# ---------------------------------------------------------------------------

def get_all_merchants(db: sqlite3.Connection) -> list[dict]:
    """Return all merchants with their order counts, sorted by order count desc."""
    cache_key = "all_merchants"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    # Sample 2M rows by ROWID for approximate counts — fast enough for dropdown ordering
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

def get_demand_map(db: sqlite3.Connection, merchant_id: str) -> list[dict]:
    """Return order volume and RTO rate per destination city for a merchant."""
    cache_key = f"demand_map:{merchant_id}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    rows = _rows_to_dicts(
        db.execute(
            """
            SELECT
                destination_cluster AS city,
                COUNT(*) AS order_count,
                SUM(CASE WHEN delivery_outcome = 'rto' THEN 1 ELSE 0 END) AS rto_count,
                ROUND(100.0 * AVG(CASE WHEN delivery_outcome = 'rto' THEN 1.0 ELSE 0.0 END), 1)
                    AS rto_rate
            FROM orders
            WHERE merchant_id = ?
            GROUP BY destination_cluster
            ORDER BY order_count DESC
            """,
            (merchant_id,),
        )
    )
    _cache_set(cache_key, rows, ttl=120)
    return rows
