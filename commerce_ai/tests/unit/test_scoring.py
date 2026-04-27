"""Unit tests for ai/scoring.py — RealizedCommerceScorer."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from ai.scoring import RealizedCommerceScorer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_orders_df(n: int = 200) -> pd.DataFrame:
    """Create a minimal but realistic orders DataFrame for training."""
    import random

    random.seed(42)
    categories = ["fashion", "electronics", "beauty", "home"]
    price_bands = ["0-500", "500-1000", "1000-2000", "2000+"]
    payment_modes = ["COD", "prepaid"]
    origin_nodes = ["WH001", "WH002", "WH003"]
    dest_clusters = ["north", "south", "east", "west"]
    outcomes = ["delivered", "rto", "pending"]

    rows = []
    for i in range(n):
        cat = random.choice(categories)
        outcome = random.choices(outcomes, weights=[0.7, 0.2, 0.1])[0]
        rows.append(
            {
                "category": cat,
                "price_band": random.choice(price_bands),
                "payment_mode": random.choice(payment_modes),
                "origin_node": random.choice(origin_nodes),
                "destination_cluster": random.choice(dest_clusters),
                "address_quality": round(random.random(), 4),
                "delivery_outcome": outcome,
            }
        )
    return pd.DataFrame(rows)


def _sample_cohort_features() -> dict:
    return {
        "category": "fashion",
        "price_band": "0-500",
        "payment_mode": "COD",
        "origin_node": "WH001",
        "destination_cluster": "north",
        "address_quality": 0.8,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestInit:
    def test_init_no_model(self):
        scorer = RealizedCommerceScorer()
        assert scorer.model is None
        assert scorer.encoders == {}

    def test_init_with_model_path(self, tmp_path: Path):
        # Train and save first
        scorer = RealizedCommerceScorer()
        scorer.train(_make_orders_df())
        model_path = str(tmp_path / "model.joblib")
        scorer.save(model_path)

        # Load via constructor
        loaded = RealizedCommerceScorer(model_path=model_path)
        assert loaded.model is not None
        assert len(loaded.encoders) > 0


class TestTrain:
    def test_train_sets_model_and_encoders(self):
        scorer = RealizedCommerceScorer()
        scorer.train(_make_orders_df())
        assert scorer.model is not None
        assert set(scorer.encoders.keys()) == {
            "category",
            "price_band",
            "payment_mode",
            "origin_node",
            "destination_cluster",
        }

    def test_train_small_dataset(self):
        scorer = RealizedCommerceScorer()
        scorer.train(_make_orders_df(n=10))
        assert scorer.model is not None


class TestPredict:
    def test_predict_returns_float_in_range(self):
        scorer = RealizedCommerceScorer()
        scorer.train(_make_orders_df())
        score = scorer.predict(_sample_cohort_features())
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0

    def test_predict_no_model_returns_fallback(self):
        scorer = RealizedCommerceScorer()
        score = scorer.predict(_sample_cohort_features())
        assert score == 0.5

    def test_predict_unseen_category_returns_fallback(self):
        scorer = RealizedCommerceScorer()
        scorer.train(_make_orders_df())
        features = _sample_cohort_features()
        features["category"] = "never_seen_before_xyz"
        score = scorer.predict(features)
        assert score == 0.5


class TestRankCohorts:
    def test_rank_cohorts_sorted_descending(self):
        scorer = RealizedCommerceScorer()
        scorer.train(_make_orders_df())
        cohorts = [
            {**_sample_cohort_features(), "order_count": 100},
            {
                "category": "electronics",
                "price_band": "2000+",
                "payment_mode": "prepaid",
                "origin_node": "WH002",
                "destination_cluster": "south",
                "address_quality": 0.3,
                "order_count": 200,
            },
        ]
        ranked = scorer.rank_cohorts(cohorts)
        assert len(ranked) == 2
        assert ranked[0]["realized_commerce_score"] >= ranked[1]["realized_commerce_score"]

    def test_rank_cohorts_contains_required_keys(self):
        scorer = RealizedCommerceScorer()
        scorer.train(_make_orders_df())
        cohorts = [{**_sample_cohort_features(), "order_count": 60}]
        ranked = scorer.rank_cohorts(cohorts)
        item = ranked[0]
        assert "cohort_key" in item
        assert "realized_commerce_score" in item
        assert "is_low_confidence" in item
        assert "order_count" in item

    def test_rank_cohorts_unseen_category_marked_low_confidence(self):
        scorer = RealizedCommerceScorer()
        scorer.train(_make_orders_df())
        cohorts = [
            {
                "category": "totally_unknown",
                "price_band": "0-500",
                "payment_mode": "COD",
                "origin_node": "WH001",
                "destination_cluster": "north",
                "address_quality": 0.5,
                "order_count": 100,
            }
        ]
        ranked = scorer.rank_cohorts(cohorts)
        assert ranked[0]["is_low_confidence"] is True
        assert ranked[0]["realized_commerce_score"] == 0.5

    def test_rank_cohorts_empty_list(self):
        scorer = RealizedCommerceScorer()
        scorer.train(_make_orders_df())
        assert scorer.rank_cohorts([]) == []


class TestIsLowConfidence:
    def test_below_threshold(self):
        assert RealizedCommerceScorer.is_low_confidence(49) is True

    def test_at_threshold(self):
        assert RealizedCommerceScorer.is_low_confidence(50) is False

    def test_above_threshold(self):
        assert RealizedCommerceScorer.is_low_confidence(100) is False

    def test_custom_threshold(self):
        assert RealizedCommerceScorer.is_low_confidence(99, min_orders=100) is True
        assert RealizedCommerceScorer.is_low_confidence(100, min_orders=100) is False


class TestSaveLoad:
    def test_round_trip(self, tmp_path: Path):
        scorer = RealizedCommerceScorer()
        scorer.train(_make_orders_df())
        features = _sample_cohort_features()
        original_score = scorer.predict(features)

        model_path = str(tmp_path / "model.joblib")
        scorer.save(model_path)

        loaded = RealizedCommerceScorer()
        loaded.load(model_path)
        loaded_score = loaded.predict(features)

        assert abs(original_score - loaded_score) < 1e-6
