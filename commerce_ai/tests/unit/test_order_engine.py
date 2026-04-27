"""Unit tests for services/order_engine.py — OrderActionEngineService."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from ai.knowledge_graph import RiskFactor, RiskPath
from data.db import init_db, get_db
from models import RiskTag
from services.auto_cancel import AutoCancelService
from services.impulse_detector import ImpulseDetector
from services.order_engine import OrderActionEngineService


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
    # Seed some orders for enrichment and impulse detection
    for i in range(5):
        conn.execute(
            """INSERT INTO orders (order_id, merchant_id, customer_ucid, category, price_band,
               payment_mode, origin_node, destination_pincode, destination_cluster,
               address_quality, rto_score, delivery_outcome, created_at)
               VALUES (?, 'M001', 'CUST001', 'fashion', '0-500', 'COD', 'WH001',
                       '110001', 'north', 0.8, ?, 'pending', '2024-01-01T10:00:00')""",
            (f"ORD00{i}", 0.3 + i * 0.15),
        )
    # Seed merchant permissions
    conn.execute(
        """INSERT INTO merchant_permissions (merchant_id, intervention_type, is_enabled,
           auto_cancel_enabled, auto_cancel_threshold, express_upgrade_enabled)
           VALUES ('M001', 'verification', 1, 0, 0.9, 0)"""
    )
    conn.commit()
    yield conn
    conn.close()


def _make_service(db):
    risk_reasoner = MagicMock()
    risk_reasoner.generate_risk_tag.return_value = RiskTag(
        tag_label="High RTO Risk", explanation="Test explanation"
    )

    nba_policy = MagicMock()
    nba_policy.recommend.return_value = {
        "intervention_type": "verification",
        "confidence_score": 0.8,
    }

    insight_gen = MagicMock()
    insight_gen.generate_action_insight.return_value = "NL explanation for action."

    knowledge_graph = MagicMock()
    knowledge_graph.get_risk_path.return_value = RiskPath(
        order_id="ORD001",
        rto_score=0.7,
        factors=[
            RiskFactor(
                factor_type="customer_history",
                description="Customer has 2 prior RTOs",
                weight=0.3,
            )
        ],
        total_risk_weight=0.3,
    )

    auto_cancel = AutoCancelService(db)
    impulse_detector = ImpulseDetector(db)

    return OrderActionEngineService(
        db=db,
        risk_reasoner=risk_reasoner,
        nba_policy=nba_policy,
        insight_gen=insight_gen,
        auto_cancel_service=auto_cancel,
        impulse_detector=impulse_detector,
        knowledge_graph=knowledge_graph,
    )


def _order(rto_score=0.7, payment_mode="COD", hour=10):
    return {
        "order_id": "ORD001",
        "merchant_id": "M001",
        "customer_ucid": "CUST001",
        "category": "fashion",
        "price_band": "0-500",
        "payment_mode": payment_mode,
        "origin_node": "WH001",
        "destination_pincode": "110001",
        "destination_cluster": "north",
        "address_quality": 0.8,
        "rto_score": rto_score,
        "delivery_outcome": "pending",
        "shipping_mode": "surface",
        "created_at": f"2024-01-01T{hour:02d}:30:00",
    }


def _config(auto_cancel_enabled=False, auto_cancel_threshold=0.9, express_enabled=False):
    return {
        "auto_cancel_enabled": auto_cancel_enabled,
        "auto_cancel_threshold": auto_cancel_threshold,
        "express_upgrade_enabled": express_enabled,
    }


class TestProcessOrder:
    def test_standard_flow(self, db):
        service = _make_service(db)
        result = service.process_order(_order(rto_score=0.7), _config())
        assert result["auto_cancel_result"]["cancelled"] is False
        assert result["risk_tag"] is not None
        assert result["next_best_action"] is not None
        assert result["nl_explanation"] is not None

    def test_auto_cancel_stops_processing(self, db):
        service = _make_service(db)
        config = _config(auto_cancel_enabled=True, auto_cancel_threshold=0.5)
        result = service.process_order(_order(rto_score=0.7), config)
        assert result["auto_cancel_result"]["cancelled"] is True
        # Standard flow should NOT have run
        assert result["enrichment"] is None
        assert result["risk_tag"] is None

    def test_low_rto_no_nba(self, db):
        service = _make_service(db)
        result = service.process_order(_order(rto_score=0.2), _config())
        # Below risk threshold → no_action
        assert result["next_best_action"]["intervention_type"] == "no_action"

    def test_low_confidence_nba_defaults_to_no_action(self, db):
        service = _make_service(db)
        service.nba_policy.recommend.return_value = {
            "intervention_type": "verification",
            "confidence_score": 0.4,
        }
        result = service.process_order(_order(rto_score=0.7), _config())
        assert result["next_best_action"]["intervention_type"] == "no_action"

    def test_impulse_detection_runs(self, db):
        service = _make_service(db)
        result = service.process_order(_order(rto_score=0.7), _config())
        assert result["impulse_result"] is not None
        assert "is_impulsive" in result["impulse_result"]

    def test_express_upgrade_when_conditions_met(self, db):
        service = _make_service(db)
        config = _config(express_enabled=True, auto_cancel_threshold=0.9)
        # Late-night + COD + fashion + first-time buyer → impulsive
        order = _order(rto_score=0.6, hour=23)
        result = service.process_order(order, config)
        # Should detect impulse and attempt upgrade
        assert result["impulse_result"] is not None
        assert result["express_upgrade_result"] is not None

    def test_result_contains_all_keys(self, db):
        service = _make_service(db)
        result = service.process_order(_order(), _config())
        expected_keys = [
            "order", "enrichment", "risk_tag", "next_best_action",
            "nl_explanation", "auto_cancel_result", "impulse_result",
            "express_upgrade_result",
        ]
        for key in expected_keys:
            assert key in result, f"Missing key: {key}"


class TestGetLiveFeed:
    def test_returns_sorted_by_rto_desc(self, db):
        service = _make_service(db)
        feed = service.get_live_feed("M001")
        assert len(feed) > 0
        scores = [p["order"]["rto_score"] for p in feed if "rto_score" in p.get("order", {})]
        assert scores == sorted(scores, reverse=True)

    def test_empty_merchant(self, db):
        service = _make_service(db)
        feed = service.get_live_feed("NONEXISTENT")
        assert feed == []
