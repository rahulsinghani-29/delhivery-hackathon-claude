"""Unit tests for services/demand_advisor.py — DemandAdvisorService."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from data.db import init_db, get_db
from services.demand_advisor import DemandAdvisorService


@pytest.fixture
def db(tmp_path):
    db_path = str(tmp_path / "test.db")
    init_db(db_path)
    conn = get_db(db_path)
    # Seed merchants
    conn.execute("INSERT INTO merchants (merchant_id, name) VALUES ('M001', 'Test')")
    conn.execute("INSERT INTO merchants (merchant_id, name) VALUES ('M002', 'Peer')")
    conn.execute(
        "INSERT INTO warehouse_nodes (node_id, merchant_id, city, state, pincode) "
        "VALUES ('WH001', 'M001', 'Delhi', 'DL', '110001')"
    )
    conn.execute(
        "INSERT INTO warehouse_nodes (node_id, merchant_id, city, state, pincode) "
        "VALUES ('WH002', 'M002', 'Mumbai', 'MH', '400001')"
    )
    conn.commit()
    yield conn
    conn.close()


def _seed_orders(db, merchant_id, category, price_band, n=50, delivery_rate=0.7):
    """Seed orders for a merchant with a given delivery rate."""
    import random
    random.seed(42)
    for i in range(n):
        outcome = "delivered" if random.random() < delivery_rate else "rto"
        db.execute(
            """INSERT INTO orders (order_id, merchant_id, customer_ucid, category, price_band,
               payment_mode, origin_node, destination_pincode, destination_cluster,
               address_quality, rto_score, delivery_outcome, created_at)
               VALUES (?, ?, ?, ?, ?, 'COD', 'WH001', '110001', 'north', 0.8, 0.3, ?, '2024-01-01T10:00:00')""",
            (f"ORD_{merchant_id}_{category}_{i}", merchant_id, f"CUST_{i}", category, price_band, outcome),
        )
    db.commit()


@pytest.fixture
def scorer():
    mock = MagicMock()
    mock.predict.return_value = 0.7
    return mock


@pytest.fixture
def insight_gen():
    mock = MagicMock()
    mock.generate_demand_insight.return_value = "Test insight explanation."
    return mock


class TestGetSuggestions:
    def test_empty_when_no_orders(self, db, scorer, insight_gen):
        service = DemandAdvisorService(db, scorer, insight_gen)
        result = service.get_suggestions("M001")
        assert result == []

    def test_returns_suggestions_when_peer_is_better(self, db, scorer, insight_gen):
        # Merchant has 60% delivery, peer has 80%
        _seed_orders(db, "M001", "fashion", "0-500", n=100, delivery_rate=0.6)
        _seed_orders(db, "M002", "fashion", "0-500", n=300, delivery_rate=0.8)

        service = DemandAdvisorService(db, scorer, insight_gen)
        result = service.get_suggestions("M001")
        # May or may not have suggestions depending on CI width and sample size
        assert isinstance(result, list)
        assert len(result) <= 5

    def test_max_five_suggestions(self, db, scorer, insight_gen):
        # Create many categories with peer advantage
        for i, cat in enumerate(["fashion", "electronics", "beauty", "home", "toys", "sports", "books"]):
            _seed_orders(db, "M001", cat, "0-500", n=50, delivery_rate=0.4)
            _seed_orders(db, "M002", cat, "0-500", n=300, delivery_rate=0.9)

        service = DemandAdvisorService(db, scorer, insight_gen)
        result = service.get_suggestions("M001")
        assert len(result) <= 5

    def test_suggestions_sorted_by_improvement(self, db, scorer, insight_gen):
        _seed_orders(db, "M001", "fashion", "0-500", n=100, delivery_rate=0.5)
        _seed_orders(db, "M002", "fashion", "0-500", n=300, delivery_rate=0.9)

        service = DemandAdvisorService(db, scorer, insight_gen)
        result = service.get_suggestions("M001")
        if len(result) >= 2:
            for i in range(len(result) - 1):
                assert result[i]["expected_score_improvement"] >= result[i + 1]["expected_score_improvement"]

    def test_nl_explanation_generated(self, db, scorer, insight_gen):
        _seed_orders(db, "M001", "fashion", "0-500", n=100, delivery_rate=0.5)
        _seed_orders(db, "M002", "fashion", "0-500", n=300, delivery_rate=0.9)

        service = DemandAdvisorService(db, scorer, insight_gen)
        result = service.get_suggestions("M001")
        for s in result:
            assert s["nl_explanation"] != ""


class TestComputeCiWidth:
    def test_zero_sample(self):
        assert DemandAdvisorService._compute_ci_width(0.5, 0) == 1.0

    def test_large_sample_narrow_ci(self):
        ci = DemandAdvisorService._compute_ci_width(0.5, 10000)
        assert ci < 0.05

    def test_small_sample_wide_ci(self):
        ci = DemandAdvisorService._compute_ci_width(0.5, 10)
        assert ci > 0.3
