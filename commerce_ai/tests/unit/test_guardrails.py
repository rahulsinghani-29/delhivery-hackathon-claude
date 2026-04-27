"""Unit tests for services/guardrails.py — GuardrailsService."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta

import pytest

from data.db import init_db, get_db
from services.guardrails import GuardrailsService


@pytest.fixture
def db(tmp_path):
    """Create an in-memory SQLite DB with schema."""
    db_path = str(tmp_path / "test.db")
    init_db(db_path)
    conn = get_db(db_path)
    yield conn
    conn.close()


@pytest.fixture
def service(db):
    return GuardrailsService(db)


def _seed_merchant(db, merchant_id="M001"):
    db.execute("INSERT OR IGNORE INTO merchants (merchant_id, name) VALUES (?, ?)", (merchant_id, "Test"))
    db.commit()


def _seed_permissions(db, merchant_id="M001", intervention_type="verification", enabled=True,
                      daily_cap=500, hourly_cap=100):
    _seed_merchant(db, merchant_id)
    db.execute(
        """INSERT OR REPLACE INTO merchant_permissions
           (merchant_id, intervention_type, is_enabled, daily_cap, hourly_cap)
           VALUES (?, ?, ?, ?, ?)""",
        (merchant_id, intervention_type, enabled, daily_cap, hourly_cap),
    )
    db.commit()


class TestCheckRateLimit:
    def test_within_limits(self, db, service):
        _seed_permissions(db, daily_cap=500, hourly_cap=100)
        assert service.check_rate_limit("M001") is True

    def test_exceeds_daily_limit(self, db, service):
        _seed_permissions(db, daily_cap=2, hourly_cap=100)
        _seed_merchant(db)
        # Seed warehouse + orders for FK constraints
        db.execute(
            "INSERT OR IGNORE INTO warehouse_nodes (node_id, merchant_id, city, state, pincode) "
            "VALUES ('WH001', 'M001', 'Delhi', 'DL', '110001')"
        )
        now = datetime.utcnow()
        for i in range(3):
            db.execute(
                """INSERT INTO orders (order_id, merchant_id, customer_ucid, category, price_band,
                   payment_mode, origin_node, destination_pincode, destination_cluster,
                   address_quality, rto_score, delivery_outcome, created_at)
                   VALUES (?, 'M001', 'CUST001', 'fashion', '0-500', 'COD', 'WH001',
                           '110001', 'north', 0.8, 0.5, 'pending', ?)""",
                (f"ord_{i}", now.isoformat()),
            )
            db.execute(
                """INSERT INTO interventions
                   (intervention_id, order_id, merchant_id, intervention_type,
                    action_owner, initiated_by, executed_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (f"int_{i}", f"ord_{i}", "M001", "verification", "delhivery", "system",
                 (now - timedelta(minutes=i)).isoformat()),
            )
        db.commit()
        assert service.check_rate_limit("M001") is False


class TestCheckPermission:
    def test_permission_granted(self, db, service):
        _seed_permissions(db, intervention_type="verification", enabled=True)
        assert service.check_permission("M001", "verification") is True

    def test_permission_denied(self, db, service):
        _seed_permissions(db, intervention_type="verification", enabled=False)
        assert service.check_permission("M001", "verification") is False

    def test_no_permissions_at_all(self, db, service):
        _seed_merchant(db)
        assert service.check_permission("M001", "verification") is False


class TestApplyConfidenceGate:
    def test_demand_gate_passes(self, service):
        rec = {"peer_sample_size": 300, "confidence_interval_width": 0.10}
        assert service.apply_confidence_gate(rec, "demand") is True

    def test_demand_gate_fails_low_sample(self, service):
        rec = {"peer_sample_size": 100, "confidence_interval_width": 0.10}
        assert service.apply_confidence_gate(rec, "demand") is False

    def test_demand_gate_fails_wide_ci(self, service):
        rec = {"peer_sample_size": 300, "confidence_interval_width": 0.20}
        assert service.apply_confidence_gate(rec, "demand") is False

    def test_demand_gate_boundary_sample(self, service):
        rec = {"peer_sample_size": 200, "confidence_interval_width": 0.15}
        assert service.apply_confidence_gate(rec, "demand") is True

    def test_action_gate_passes(self, service):
        rec = {"confidence_score": 0.8}
        assert service.apply_confidence_gate(rec, "action") is True

    def test_action_gate_fails(self, service):
        rec = {"confidence_score": 0.5}
        assert service.apply_confidence_gate(rec, "action") is False

    def test_action_gate_boundary(self, service):
        rec = {"confidence_score": 0.6}
        assert service.apply_confidence_gate(rec, "action") is True

    def test_unknown_gate_type(self, service):
        rec = {"confidence_score": 0.9}
        assert service.apply_confidence_gate(rec, "unknown") is False


class TestLogSuppression:
    def test_logs_to_db(self, db, service):
        _seed_merchant(db)
        rec = {"type": "demand_suggestion", "cohort": "fashion/0-500"}
        service.log_suppression("M001", rec, "insufficient_sample_size")

        rows = db.execute("SELECT * FROM suppressed_recommendations").fetchall()
        assert len(rows) == 1
        row = dict(rows[0])
        assert row["merchant_id"] == "M001"
        assert row["suppression_reason"] == "insufficient_sample_size"
        assert "fashion" in row["recommendation_data"]
