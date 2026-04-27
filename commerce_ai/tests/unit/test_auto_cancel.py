"""Unit tests for services/auto_cancel.py — AutoCancelService."""

from __future__ import annotations

import sqlite3

import pytest

from data.db import init_db, get_db
from services.auto_cancel import AutoCancelService


@pytest.fixture
def db(tmp_path):
    db_path = str(tmp_path / "test.db")
    init_db(db_path)
    conn = get_db(db_path)
    # Seed minimal data for FK constraints
    conn.execute("INSERT INTO merchants (merchant_id, name) VALUES ('M001', 'Test')")
    conn.execute(
        "INSERT INTO warehouse_nodes (node_id, merchant_id, city, state, pincode) VALUES ('WH001', 'M001', 'Delhi', 'DL', '110001')"
    )
    conn.execute(
        """INSERT INTO orders (order_id, merchant_id, customer_ucid, category, price_band,
           payment_mode, origin_node, destination_pincode, destination_cluster,
           address_quality, rto_score, delivery_outcome, created_at)
           VALUES ('ORD001', 'M001', 'CUST001', 'fashion', '0-500', 'COD', 'WH001',
                   '110001', 'north', 0.8, 0.95, 'pending', '2024-01-01T10:00:00')"""
    )
    conn.commit()
    yield conn
    conn.close()


@pytest.fixture
def service(db):
    return AutoCancelService(db)


def _order(rto_score=0.95):
    return {
        "order_id": "ORD001",
        "merchant_id": "M001",
        "rto_score": rto_score,
    }


def _config(enabled=True, threshold=0.9):
    return {
        "auto_cancel_enabled": enabled,
        "auto_cancel_threshold": threshold,
    }


class TestCheckAndCancel:
    def test_disabled_returns_not_cancelled(self, service):
        result = service.check_and_cancel(_order(0.99), _config(enabled=False))
        assert result["cancelled"] is False
        assert result["reason"] == "auto_cancel_disabled"

    def test_below_threshold_returns_not_cancelled(self, service):
        result = service.check_and_cancel(_order(0.5), _config(enabled=True, threshold=0.9))
        assert result["cancelled"] is False
        assert result["reason"] == "below_threshold"

    def test_at_threshold_returns_not_cancelled(self, service):
        """Score exactly at threshold should NOT cancel (must be strictly greater)."""
        result = service.check_and_cancel(_order(0.9), _config(enabled=True, threshold=0.9))
        assert result["cancelled"] is False
        assert result["reason"] == "below_threshold"

    def test_above_threshold_cancels(self, service):
        result = service.check_and_cancel(_order(0.95), _config(enabled=True, threshold=0.9))
        assert result["cancelled"] is True
        assert result["reason"] == "rto_score_exceeded_threshold"
        assert result["cancelled_at"] is not None

    def test_just_above_threshold_cancels(self, service):
        result = service.check_and_cancel(_order(0.901), _config(enabled=True, threshold=0.9))
        assert result["cancelled"] is True

    def test_disabled_with_extreme_score(self, service):
        """Even with rto_score=1.0, disabled config should not cancel."""
        result = service.check_and_cancel(_order(1.0), _config(enabled=False))
        assert result["cancelled"] is False

    def test_result_contains_all_fields(self, service):
        result = service.check_and_cancel(_order(0.95), _config(enabled=True))
        assert "cancelled" in result
        assert "reason" in result
        assert "order_id" in result
        assert "merchant_id" in result
        assert "rto_score" in result
        assert "threshold" in result

    def test_logs_intervention_on_cancel(self, db, service):
        service.check_and_cancel(_order(0.95), _config(enabled=True, threshold=0.9))
        rows = db.execute(
            "SELECT * FROM interventions WHERE intervention_type = 'auto_cancel'"
        ).fetchall()
        assert len(rows) == 1
        row = dict(rows[0])
        assert row["order_id"] == "ORD001"
        assert row["action_owner"] == "delhivery"
