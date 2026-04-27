"""Unit tests for commerce_ai.data.queries."""

import sqlite3
from datetime import datetime, timedelta

import pytest

from data.db import init_db, get_db
from data import queries


@pytest.fixture()
def db(tmp_path):
    """Create an in-memory-like temp DB with schema initialised."""
    db_path = str(tmp_path / "test.db")
    init_db(db_path)
    conn = get_db(db_path)
    yield conn
    conn.close()


def _insert_merchant(db, merchant_id="M1", name="Test Merchant"):
    db.execute(
        "INSERT INTO merchants (merchant_id, name) VALUES (?, ?)",
        (merchant_id, name),
    )
    db.commit()


def _insert_node(db, node_id, merchant_id="M1", city="Delhi", state="DL", pincode="110001"):
    db.execute(
        "INSERT INTO warehouse_nodes (node_id, merchant_id, city, state, pincode) VALUES (?, ?, ?, ?, ?)",
        (node_id, merchant_id, city, state, pincode),
    )
    db.commit()


def _insert_order(
    db,
    order_id,
    merchant_id="M1",
    customer_ucid="C1",
    category="electronics",
    price_band="mid",
    payment_mode="COD",
    origin_node="N1",
    destination_pincode="400001",
    destination_cluster="west",
    address_quality=0.8,
    rto_score=0.3,
    delivery_outcome="delivered",
    shipping_mode="surface",
    created_at=None,
):
    if created_at is None:
        created_at = datetime.utcnow().isoformat()
    db.execute(
        """INSERT INTO orders (
            order_id, merchant_id, customer_ucid, category, price_band,
            payment_mode, origin_node, destination_pincode, destination_cluster,
            address_quality, rto_score, delivery_outcome, shipping_mode, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            order_id, merchant_id, customer_ucid, category, price_band,
            payment_mode, origin_node, destination_pincode, destination_cluster,
            address_quality, rto_score, delivery_outcome, shipping_mode, created_at,
        ),
    )
    db.commit()


def _insert_intervention(
    db,
    intervention_id,
    order_id="O1",
    merchant_id="M1",
    intervention_type="verification",
    action_owner="delhivery",
    initiated_by="system",
    confidence_score=0.8,
    outcome="successful_delivery",
    executed_at=None,
    completed_at=None,
):
    if executed_at is None:
        executed_at = datetime.utcnow().isoformat()
    db.execute(
        """INSERT INTO interventions (
            intervention_id, order_id, merchant_id, intervention_type,
            action_owner, initiated_by, confidence_score, outcome,
            executed_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            intervention_id, order_id, merchant_id, intervention_type,
            action_owner, initiated_by, confidence_score, outcome,
            executed_at, completed_at,
        ),
    )
    db.commit()


def _insert_permission(
    db,
    merchant_id="M1",
    intervention_type="verification",
    is_enabled=True,
    daily_cap=500,
    hourly_cap=100,
    auto_cancel_enabled=False,
    auto_cancel_threshold=0.9,
    express_upgrade_enabled=False,
    impulse_categories="fashion,beauty",
):
    db.execute(
        """INSERT INTO merchant_permissions (
            merchant_id, intervention_type, is_enabled, daily_cap, hourly_cap,
            auto_cancel_enabled, auto_cancel_threshold, express_upgrade_enabled,
            impulse_categories
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            merchant_id, intervention_type, is_enabled, daily_cap, hourly_cap,
            auto_cancel_enabled, auto_cancel_threshold, express_upgrade_enabled,
            impulse_categories,
        ),
    )
    db.commit()


# ---- get_merchant_snapshot ----

class TestGetMerchantSnapshot:
    def test_returns_empty_for_unknown_merchant(self, db):
        result = queries.get_merchant_snapshot(db, "UNKNOWN")
        assert result == {}

    def test_returns_merchant_info_and_nodes(self, db):
        _insert_merchant(db, "M1")
        _insert_node(db, "N1", "M1")
        _insert_node(db, "N2", "M1", city="Mumbai", state="MH", pincode="400001")
        result = queries.get_merchant_snapshot(db, "M1")
        assert result["merchant_id"] == "M1"
        assert len(result["warehouse_nodes"]) == 2

    def test_distributions_and_benchmark_gaps(self, db):
        _insert_merchant(db, "M1")
        _insert_merchant(db, "M2", name="Peer")
        _insert_node(db, "N1", "M1")
        _insert_order(db, "O1", merchant_id="M1", category="electronics", delivery_outcome="delivered")
        _insert_order(db, "O2", merchant_id="M1", category="fashion", delivery_outcome="RTO")
        _insert_order(db, "O3", merchant_id="M2", category="electronics", delivery_outcome="RTO")

        result = queries.get_merchant_snapshot(db, "M1")
        assert result["category_distribution"]["electronics"] == 1
        assert result["category_distribution"]["fashion"] == 1
        assert len(result["benchmark_gaps"]) >= 1


# ---- get_cohort_benchmarks ----

class TestGetCohortBenchmarks:
    def test_groups_by_cohort_dimensions(self, db):
        _insert_merchant(db, "M1")
        _insert_node(db, "N1", "M1")
        _insert_order(db, "O1", category="electronics", delivery_outcome="delivered")
        _insert_order(db, "O2", category="electronics", delivery_outcome="RTO")
        _insert_order(db, "O3", category="fashion", delivery_outcome="delivered")

        result = queries.get_cohort_benchmarks(db, "M1")
        assert len(result) == 2
        elec = next(r for r in result if r["category"] == "electronics")
        assert elec["order_count"] == 2
        assert elec["delivery_rate"] == pytest.approx(0.5)


# ---- get_peer_benchmarks ----

class TestGetPeerBenchmarks:
    def test_compares_merchant_vs_peers(self, db):
        _insert_merchant(db, "M1")
        _insert_merchant(db, "M2", name="Peer")
        _insert_node(db, "N1", "M1")
        _insert_node(db, "N2", "M2")
        # M1: 1 delivered
        _insert_order(db, "O1", merchant_id="M1", category="electronics", price_band="mid", delivery_outcome="delivered")
        # M2: 1 RTO
        _insert_order(db, "O2", merchant_id="M2", category="electronics", price_band="mid", delivery_outcome="RTO")

        result = queries.get_peer_benchmarks(db, "M1", "electronics", "mid")
        assert len(result) >= 1
        row = result[0]
        assert row["merchant_score"] == pytest.approx(1.0)
        assert row["peer_avg_score"] == pytest.approx(0.0)
        assert row["gap"] == pytest.approx(1.0)


# ---- get_recent_orders ----

class TestGetRecentOrders:
    def test_returns_orders_sorted_desc(self, db):
        _insert_merchant(db, "M1")
        _insert_node(db, "N1", "M1")
        t1 = "2024-01-01T00:00:00"
        t2 = "2024-01-02T00:00:00"
        _insert_order(db, "O1", created_at=t1)
        _insert_order(db, "O2", created_at=t2)

        result = queries.get_recent_orders(db, "M1", limit=10)
        assert len(result) == 2
        assert result[0]["order_id"] == "O2"
        assert result[1]["order_id"] == "O1"

    def test_respects_limit(self, db):
        _insert_merchant(db, "M1")
        _insert_node(db, "N1", "M1")
        for i in range(5):
            _insert_order(db, f"O{i}", created_at=f"2024-01-0{i+1}T00:00:00")
        result = queries.get_recent_orders(db, "M1", limit=3)
        assert len(result) == 3


# ---- get_historical_analogs ----

class TestGetHistoricalAnalogs:
    def test_returns_stats_for_matching_cohort(self, db):
        _insert_merchant(db, "M1")
        _insert_node(db, "N1", "M1")
        for i in range(60):
            outcome = "delivered" if i < 40 else "RTO"
            _insert_order(db, f"O{i}", delivery_outcome=outcome)

        result = queries.get_historical_analogs(
            db, "electronics", "mid", "COD", "N1", "west", min_orders=50
        )
        assert result["sample_size"] == 60
        assert result["rto_rate"] == pytest.approx(20 / 60, abs=0.01)

    def test_returns_none_rto_rate_below_min_orders(self, db):
        _insert_merchant(db, "M1")
        _insert_node(db, "N1", "M1")
        _insert_order(db, "O1")
        result = queries.get_historical_analogs(
            db, "electronics", "mid", "COD", "N1", "west", min_orders=50
        )
        assert result["rto_rate"] is None
        assert result["sample_size"] == 1


# ---- get_intervention_history ----

class TestGetInterventionHistory:
    def test_returns_recent_interventions(self, db):
        _insert_merchant(db, "M1")
        _insert_node(db, "N1", "M1")
        _insert_order(db, "O1")
        now = datetime.utcnow()
        _insert_intervention(db, "I1", executed_at=now.isoformat())
        _insert_intervention(
            db, "I2",
            executed_at=(now - timedelta(days=60)).isoformat(),
        )

        result = queries.get_intervention_history(db, "M1", period_days=30)
        assert len(result) == 1
        assert result[0]["intervention_id"] == "I1"


# ---- get_intervention_counts ----

class TestGetInterventionCounts:
    def test_counts_by_type_and_outcome(self, db):
        _insert_merchant(db, "M1")
        _insert_node(db, "N1", "M1")
        _insert_order(db, "O1")
        _insert_order(db, "O2")
        now = datetime.utcnow().isoformat()
        _insert_intervention(db, "I1", order_id="O1", outcome="successful_delivery", executed_at=now)
        _insert_intervention(db, "I2", order_id="O2", intervention_type="cancellation", outcome="RTO", executed_at=now)

        result = queries.get_intervention_counts(db, "M1", period_days=30)
        assert result["total"] == 2
        assert result["by_type"]["verification"] == 1
        assert result["by_type"]["cancellation"] == 1
        assert result["by_outcome"]["successful_delivery"] == 1


# ---- check_rate_limits ----

class TestCheckRateLimits:
    def test_within_limits(self, db):
        _insert_merchant(db, "M1")
        _insert_permission(db, "M1", daily_cap=500, hourly_cap=100)
        result = queries.check_rate_limits(db, "M1")
        assert result["is_within_limits"] is True
        assert result["daily_used"] == 0
        assert result["daily_cap"] == 500

    def test_exceeds_daily_cap(self, db):
        _insert_merchant(db, "M1")
        _insert_node(db, "N1", "M1")
        _insert_order(db, "O1")
        _insert_permission(db, "M1", daily_cap=2, hourly_cap=100)
        now = datetime.utcnow().isoformat()
        _insert_intervention(db, "I1", executed_at=now)
        _insert_intervention(db, "I2_extra", order_id="O1", executed_at=now)

        result = queries.check_rate_limits(db, "M1")
        assert result["daily_used"] == 2
        assert result["daily_cap"] == 2
        assert result["is_within_limits"] is False


# ---- log_intervention ----

class TestLogIntervention:
    def test_inserts_intervention(self, db):
        _insert_merchant(db, "M1")
        _insert_node(db, "N1", "M1")
        _insert_order(db, "O1")
        now = datetime.utcnow().isoformat()
        queries.log_intervention(db, {
            "intervention_id": "I_NEW",
            "order_id": "O1",
            "merchant_id": "M1",
            "intervention_type": "verification",
            "action_owner": "delhivery",
            "initiated_by": "system",
            "confidence_score": 0.85,
            "outcome": None,
            "executed_at": now,
            "completed_at": None,
        })
        row = db.execute(
            "SELECT * FROM interventions WHERE intervention_id = ?", ("I_NEW",)
        ).fetchone()
        assert row is not None
        assert dict(row)["intervention_type"] == "verification"


# ---- get_merchant_permissions ----

class TestGetMerchantPermissions:
    def test_defaults_when_no_rows(self, db):
        result = queries.get_merchant_permissions(db, "UNKNOWN")
        assert result["merchant_id"] == "UNKNOWN"
        assert result["permissions"] == {}
        assert result["auto_cancel_enabled"] is False
        assert result["express_upgrade_enabled"] is False

    def test_reads_permissions(self, db):
        _insert_merchant(db, "M1")
        _insert_permission(db, "M1", "verification", is_enabled=True, auto_cancel_enabled=True, auto_cancel_threshold=0.85)
        _insert_permission(db, "M1", "cancellation", is_enabled=False, express_upgrade_enabled=True)

        result = queries.get_merchant_permissions(db, "M1")
        assert result["permissions"]["verification"] is True
        assert result["permissions"]["cancellation"] is False
        assert result["auto_cancel_enabled"] is True
        assert result["auto_cancel_threshold"] == 0.85
        assert result["express_upgrade_enabled"] is True


# ---- get_customer_delivered_orders ----

class TestGetCustomerDeliveredOrders:
    def test_returns_only_delivered(self, db):
        _insert_merchant(db, "M1")
        _insert_node(db, "N1", "M1")
        _insert_order(db, "O1", customer_ucid="C1", delivery_outcome="delivered")
        _insert_order(db, "O2", customer_ucid="C1", delivery_outcome="RTO")
        _insert_order(db, "O3", customer_ucid="C2", delivery_outcome="delivered")

        result = queries.get_customer_delivered_orders(db, "C1", "M1")
        assert len(result) == 1
        assert result[0]["order_id"] == "O1"


# ---- get_cluster_rto_rate ----

class TestGetClusterRtoRate:
    def test_computes_rto_rate(self, db):
        _insert_merchant(db, "M1")
        _insert_node(db, "N1", "M1")
        _insert_order(db, "O1", destination_cluster="west", payment_mode="COD", delivery_outcome="delivered")
        _insert_order(db, "O2", destination_cluster="west", payment_mode="COD", delivery_outcome="RTO")
        _insert_order(db, "O3", destination_cluster="west", payment_mode="COD", delivery_outcome="delivered")

        rate = queries.get_cluster_rto_rate(db, "west", "COD")
        assert rate == pytest.approx(1 / 3, abs=0.01)

    def test_returns_zero_for_no_orders(self, db):
        rate = queries.get_cluster_rto_rate(db, "nonexistent", "COD")
        assert rate == 0.0
