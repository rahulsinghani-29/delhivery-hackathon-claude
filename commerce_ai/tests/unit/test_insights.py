"""Unit tests for ai/insights.py — InsightGenerator."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from ai.insights import InsightGenerator


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _sample_suggestion() -> dict:
    return {
        "cohort_dimension": "destination_cluster",
        "recommended_value": "Mumbai cluster",
        "expected_score_improvement": 0.12,
        "peer_benchmark": {
            "merchant_score": 0.65,
            "peer_avg_score": 0.78,
            "peer_sample_size": 350,
            "gap": -0.13,
        },
    }


def _sample_order() -> dict:
    return {
        "order_id": "ORD-100",
        "rto_score": 0.81,
        "category": "electronics",
        "payment_mode": "COD",
    }


def _sample_action() -> dict:
    return {
        "intervention_type": "verification",
        "confidence_score": 0.72,
        "risk_factors": [
            "Customer has 2 prior RTOs",
            "COD in high-RTO cluster",
        ],
    }


# ---------------------------------------------------------------------------
# Demand insight — template fallback
# ---------------------------------------------------------------------------

class TestDemandInsightFallback:
    def test_returns_string(self):
        gen = InsightGenerator.__new__(InsightGenerator)
        gen._llm = None
        gen._model_name = "llama3"

        result = gen.generate_demand_insight(_sample_suggestion())
        assert isinstance(result, str)
        assert len(result) > 0

    def test_references_cohort_dimension(self):
        gen = InsightGenerator.__new__(InsightGenerator)
        gen._llm = None
        gen._model_name = "llama3"

        result = gen.generate_demand_insight(_sample_suggestion())
        assert "destination_cluster" in result

    def test_references_score_improvement(self):
        gen = InsightGenerator.__new__(InsightGenerator)
        gen._llm = None
        gen._model_name = "llama3"

        result = gen.generate_demand_insight(_sample_suggestion())
        assert "0.12" in result

    def test_references_peer_sample_size(self):
        gen = InsightGenerator.__new__(InsightGenerator)
        gen._llm = None
        gen._model_name = "llama3"

        result = gen.generate_demand_insight(_sample_suggestion())
        assert "350" in result

    def test_references_merchant_and_peer_scores(self):
        gen = InsightGenerator.__new__(InsightGenerator)
        gen._llm = None
        gen._model_name = "llama3"

        result = gen.generate_demand_insight(_sample_suggestion())
        assert "0.65" in result
        assert "0.78" in result


# ---------------------------------------------------------------------------
# Action insight — template fallback
# ---------------------------------------------------------------------------

class TestActionInsightFallback:
    def test_returns_string(self):
        gen = InsightGenerator.__new__(InsightGenerator)
        gen._llm = None
        gen._model_name = "llama3"

        result = gen.generate_action_insight(_sample_order(), _sample_action())
        assert isinstance(result, str)
        assert len(result) > 0

    def test_references_order_id(self):
        gen = InsightGenerator.__new__(InsightGenerator)
        gen._llm = None
        gen._model_name = "llama3"

        result = gen.generate_action_insight(_sample_order(), _sample_action())
        assert "ORD-100" in result

    def test_references_rto_score(self):
        gen = InsightGenerator.__new__(InsightGenerator)
        gen._llm = None
        gen._model_name = "llama3"

        result = gen.generate_action_insight(_sample_order(), _sample_action())
        assert "0.81" in result

    def test_references_intervention_type(self):
        gen = InsightGenerator.__new__(InsightGenerator)
        gen._llm = None
        gen._model_name = "llama3"

        result = gen.generate_action_insight(_sample_order(), _sample_action())
        assert "verification" in result

    def test_references_risk_factors(self):
        gen = InsightGenerator.__new__(InsightGenerator)
        gen._llm = None
        gen._model_name = "llama3"

        result = gen.generate_action_insight(_sample_order(), _sample_action())
        assert "prior RTOs" in result

    def test_empty_risk_factors(self):
        gen = InsightGenerator.__new__(InsightGenerator)
        gen._llm = None
        gen._model_name = "llama3"

        action = {"intervention_type": "no_action", "confidence_score": 0.5, "risk_factors": []}
        result = gen.generate_action_insight(_sample_order(), action)
        assert "general risk elevation" in result


# ---------------------------------------------------------------------------
# LLM path (mocked)
# ---------------------------------------------------------------------------

class TestLLMDemandInsight:
    def test_llm_success(self):
        gen = InsightGenerator.__new__(InsightGenerator)
        gen._model_name = "llama3"
        mock_llm = MagicMock()
        mock_llm.invoke.return_value = "Shifting to Mumbai cluster improves delivery by 12pp."
        gen._llm = mock_llm

        result = gen.generate_demand_insight(_sample_suggestion())
        assert "Mumbai cluster" in result
        mock_llm.invoke.assert_called_once()

    def test_llm_failure_falls_back(self):
        gen = InsightGenerator.__new__(InsightGenerator)
        gen._model_name = "llama3"
        mock_llm = MagicMock()
        mock_llm.invoke.side_effect = RuntimeError("LLM down")
        gen._llm = mock_llm

        result = gen.generate_demand_insight(_sample_suggestion())
        # Template fallback should reference data points
        assert "destination_cluster" in result
        assert "350" in result


class TestLLMActionInsight:
    def test_llm_success(self):
        gen = InsightGenerator.__new__(InsightGenerator)
        gen._model_name = "llama3"
        mock_llm = MagicMock()
        mock_llm.invoke.return_value = "Verification recommended due to high RTO risk."
        gen._llm = mock_llm

        result = gen.generate_action_insight(_sample_order(), _sample_action())
        assert "Verification" in result
        mock_llm.invoke.assert_called_once()

    def test_llm_failure_falls_back(self):
        gen = InsightGenerator.__new__(InsightGenerator)
        gen._model_name = "llama3"
        mock_llm = MagicMock()
        mock_llm.invoke.side_effect = RuntimeError("LLM down")
        gen._llm = mock_llm

        result = gen.generate_action_insight(_sample_order(), _sample_action())
        assert "ORD-100" in result
        assert "0.81" in result
