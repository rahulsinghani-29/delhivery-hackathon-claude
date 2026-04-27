"""Unit tests for ai/next_best_action.py — NextBestActionPolicy."""

from __future__ import annotations

import random
from pathlib import Path

import pandas as pd
import pytest

from ai.next_best_action import ACTION_SPACE, NextBestActionPolicy


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_intervention_df(n: int = 300) -> pd.DataFrame:
    """Create a minimal intervention-outcome DataFrame for training."""
    random.seed(42)
    categories = ["fashion", "electronics", "beauty", "home"]
    price_bands = ["0-500", "500-1000", "1000-2000", "2000+"]
    payment_modes = ["COD", "prepaid"]
    origin_nodes = ["WH001", "WH002", "WH003"]
    dest_clusters = ["north", "south", "east", "west"]
    interventions = ["verification", "cancellation", "masked_calling",
                     "cod_to_prepaid", "no_action"]
    outcomes = ["delivered", "rto"]

    rows = []
    for _ in range(n):
        rows.append({
            "category": random.choice(categories),
            "price_band": random.choice(price_bands),
            "payment_mode": random.choice(payment_modes),
            "origin_node": random.choice(origin_nodes),
            "destination_cluster": random.choice(dest_clusters),
            "address_quality": round(random.random(), 4),
            "rto_score": round(random.uniform(0.3, 0.95), 4),
            "intervention_type": random.choice(interventions),
            "delivery_outcome": random.choices(outcomes, weights=[0.7, 0.3])[0],
        })
    return pd.DataFrame(rows)


def _sample_order_context() -> dict:
    return {
        "category": "fashion",
        "price_band": "0-500",
        "payment_mode": "COD",
        "origin_node": "WH001",
        "destination_cluster": "north",
        "address_quality": 0.4,
        "rto_score": 0.75,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestInit:
    def test_init_no_model(self):
        policy = NextBestActionPolicy()
        assert policy.model is None

    def test_init_with_model_path(self, tmp_path: Path):
        policy = NextBestActionPolicy()
        policy.train(_make_intervention_df())
        model_path = str(tmp_path / "nba.joblib")
        policy.save(model_path)

        loaded = NextBestActionPolicy(model_path=model_path)
        assert loaded.model is not None


class TestTrain:
    def test_train_sets_model(self):
        policy = NextBestActionPolicy()
        policy.train(_make_intervention_df())
        assert policy.model is not None
        assert policy.action_encoder is not None
        assert len(policy.feature_encoders) > 0

    def test_train_small_dataset(self):
        policy = NextBestActionPolicy()
        policy.train(_make_intervention_df(n=20))
        assert policy.model is not None


class TestRecommend:
    def test_recommend_returns_dict(self):
        policy = NextBestActionPolicy()
        policy.train(_make_intervention_df())
        result = policy.recommend(_sample_order_context())
        assert isinstance(result, dict)
        assert "intervention_type" in result
        assert "confidence_score" in result

    def test_recommend_action_in_action_space(self):
        policy = NextBestActionPolicy()
        policy.train(_make_intervention_df())
        result = policy.recommend(_sample_order_context())
        assert result["intervention_type"] in ACTION_SPACE

    def test_recommend_confidence_in_range(self):
        policy = NextBestActionPolicy()
        policy.train(_make_intervention_df())
        result = policy.recommend(_sample_order_context())
        assert 0.0 <= result["confidence_score"] <= 1.0

    def test_recommend_no_model_returns_no_action(self):
        policy = NextBestActionPolicy()
        result = policy.recommend(_sample_order_context())
        assert result["intervention_type"] == "no_action"
        assert result["confidence_score"] == 0.0

    def test_recommend_unseen_category_returns_no_action(self):
        policy = NextBestActionPolicy()
        policy.train(_make_intervention_df())
        ctx = _sample_order_context()
        ctx["category"] = "totally_unknown_xyz"
        result = policy.recommend(ctx)
        assert result["intervention_type"] == "no_action"
        assert result["confidence_score"] == 0.0


class TestSaveLoad:
    def test_round_trip(self, tmp_path: Path):
        policy = NextBestActionPolicy()
        policy.train(_make_intervention_df())
        ctx = _sample_order_context()
        original = policy.recommend(ctx)

        model_path = str(tmp_path / "nba.joblib")
        policy.save(model_path)

        loaded = NextBestActionPolicy()
        loaded.load(model_path)
        loaded_result = loaded.recommend(ctx)

        assert original["intervention_type"] == loaded_result["intervention_type"]
        assert abs(original["confidence_score"] - loaded_result["confidence_score"]) < 1e-6
