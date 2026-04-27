"""Unit tests for services/impulse_detector.py — ImpulseDetector."""

from __future__ import annotations

import pytest

from data.db import init_db, get_db
from services.impulse_detector import ImpulseDetector


@pytest.fixture
def db(tmp_path):
    db_path = str(tmp_path / "test.db")
    init_db(db_path)
    conn = get_db(db_path)
    conn.execute("INSERT INTO merchants (merchant_id, name) VALUES ('M001', 'Test')")
    conn.execute(
        "INSERT INTO warehouse_nodes (node_id, merchant_id, city, state, pincode) "
        "VALUES ('WH001', 'M001', 'Delhi', 'DL', '110001')"
    )
    conn.commit()
    yield conn
    conn.close()


@pytest.fixture
def detector(db):
    return ImpulseDetector(db)


def _order(
    hour=23, payment_mode="COD", category="fashion", customer_ucid="CUST001",
    rto_score=0.6, shipping_mode="surface",
):
    return {
        "order_id": "ORD001",
        "merchant_id": "M001",
        "customer_ucid": customer_ucid,
        "category": category,
        "price_band": "0-500",
        "payment_mode": payment_mode,
        "origin_node": "WH001",
        "destination_pincode": "110001",
        "destination_cluster": "north",
        "address_quality": 0.8,
        "rto_score": rto_score,
        "delivery_outcome": "pending",
        "shipping_mode": shipping_mode,
        "created_at": f"2024-01-01T{hour:02d}:30:00",
    }


def _config(express_enabled=True, auto_cancel_threshold=0.9):
    return {
        "express_upgrade_enabled": express_enabled,
        "auto_cancel_threshold": auto_cancel_threshold,
    }


class TestDetect:
    def test_all_four_signals(self, detector):
        """Late-night + COD + first-time buyer + fashion → 4 signals, impulsive."""
        result = detector.detect(_order(hour=23, payment_mode="COD", category="fashion"))
        assert result["is_impulsive"] is True
        assert result["signal_count"] == 4
        assert "late_night" in result["matched_signals"]
        assert "cod_payment" in result["matched_signals"]
        assert "first_time_buyer" in result["matched_signals"]
        assert "high_impulse_category" in result["matched_signals"]

    def test_three_signals_is_impulsive(self, detector):
        """3 signals → impulsive."""
        result = detector.detect(_order(hour=23, payment_mode="COD", category="electronics"))
        assert result["is_impulsive"] is True
        assert result["signal_count"] == 3

    def test_two_signals_not_impulsive(self, detector):
        """2 signals → not impulsive."""
        result = detector.detect(_order(hour=10, payment_mode="COD", category="fashion"))
        # COD + first-time + fashion = 3 actually, let's use prepaid
        result = detector.detect(_order(hour=23, payment_mode="prepaid", category="electronics"))
        # late_night + first_time = 2
        assert result["is_impulsive"] is False
        assert result["signal_count"] == 2

    def test_late_night_hours(self, detector):
        """Hours 23, 0, 1, 2, 3 are late-night."""
        for h in [23, 0, 1, 2, 3]:
            result = detector.detect(_order(hour=h, payment_mode="prepaid", category="electronics"))
            assert "late_night" in result["matched_signals"], f"Hour {h} should be late-night"

    def test_not_late_night_hours(self, detector):
        """Hours 4-22 are NOT late-night."""
        for h in [4, 10, 15, 22]:
            result = detector.detect(_order(hour=h, payment_mode="prepaid", category="electronics"))
            assert "late_night" not in result["matched_signals"], f"Hour {h} should not be late-night"

    def test_first_time_buyer_with_prior_delivered(self, db, detector):
        """Customer with prior delivered order is NOT first-time."""
        db.execute(
            """INSERT INTO orders (order_id, merchant_id, customer_ucid, category, price_band,
               payment_mode, origin_node, destination_pincode, destination_cluster,
               address_quality, rto_score, delivery_outcome, created_at)
               VALUES ('ORD_PREV', 'M001', 'CUST001', 'fashion', '0-500', 'COD', 'WH001',
                       '110001', 'north', 0.8, 0.3, 'delivered', '2023-12-01T10:00:00')"""
        )
        db.commit()
        result = detector.detect(_order())
        assert "first_time_buyer" not in result["matched_signals"]

    def test_first_time_buyer_with_only_rto_prior(self, db, detector):
        """Customer with only RTO'd prior orders is still first-time for delivered."""
        db.execute(
            """INSERT INTO orders (order_id, merchant_id, customer_ucid, category, price_band,
               payment_mode, origin_node, destination_pincode, destination_cluster,
               address_quality, rto_score, delivery_outcome, created_at)
               VALUES ('ORD_RTO', 'M001', 'CUST001', 'fashion', '0-500', 'COD', 'WH001',
                       '110001', 'north', 0.8, 0.7, 'rto', '2023-12-01T10:00:00')"""
        )
        db.commit()
        result = detector.detect(_order())
        assert "first_time_buyer" in result["matched_signals"]

    def test_custom_impulse_categories(self, db):
        """Custom impulse categories should be respected."""
        det = ImpulseDetector(db, impulse_categories=["electronics", "toys"])
        result = det.detect(_order(category="electronics"))
        assert "high_impulse_category" in result["matched_signals"]
        result2 = det.detect(_order(category="fashion"))
        assert "high_impulse_category" not in result2["matched_signals"]


class TestUpgradeToExpress:
    def test_all_conditions_met(self, detector):
        order = _order(rto_score=0.6)
        impulse = {"is_impulsive": True, "matched_signals": ["late_night", "cod_payment", "first_time_buyer"]}
        result = detector.upgrade_to_express(order, _config(), impulse, 0.4, 0.9)
        assert result["upgraded"] is True
        assert result["reason"] == "impulsive_and_enabled"
        assert result["new_shipping_mode"] == "express"

    def test_not_impulsive(self, detector):
        order = _order(rto_score=0.6)
        impulse = {"is_impulsive": False, "matched_signals": ["late_night"]}
        result = detector.upgrade_to_express(order, _config(), impulse, 0.4, 0.9)
        assert result["upgraded"] is False
        assert result["reason"] == "not_impulsive"

    def test_below_risk_threshold(self, detector):
        order = _order(rto_score=0.3)
        impulse = {"is_impulsive": True, "matched_signals": ["late_night", "cod_payment", "first_time_buyer"]}
        result = detector.upgrade_to_express(order, _config(), impulse, 0.4, 0.9)
        assert result["upgraded"] is False
        assert result["reason"] == "below_risk_threshold"

    def test_above_auto_cancel_threshold(self, detector):
        order = _order(rto_score=0.95)
        impulse = {"is_impulsive": True, "matched_signals": ["late_night", "cod_payment", "first_time_buyer"]}
        result = detector.upgrade_to_express(order, _config(auto_cancel_threshold=0.9), impulse, 0.4, 0.9)
        assert result["upgraded"] is False
        assert result["reason"] == "above_auto_cancel_threshold"

    def test_express_disabled(self, detector):
        order = _order(rto_score=0.6)
        impulse = {"is_impulsive": True, "matched_signals": ["late_night", "cod_payment", "first_time_buyer"]}
        result = detector.upgrade_to_express(order, _config(express_enabled=False), impulse, 0.4, 0.9)
        assert result["upgraded"] is False
        assert result["reason"] == "express_upgrade_disabled"

    def test_at_exact_risk_threshold(self, detector):
        """rto_score == risk_threshold → below_risk_threshold (must be strictly greater)."""
        order = _order(rto_score=0.4)
        impulse = {"is_impulsive": True, "matched_signals": ["late_night", "cod_payment", "first_time_buyer"]}
        result = detector.upgrade_to_express(order, _config(), impulse, 0.4, 0.9)
        assert result["upgraded"] is False
        assert result["reason"] == "below_risk_threshold"
