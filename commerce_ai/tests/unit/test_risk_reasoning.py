"""Unit tests for ai/risk_reasoning.py — RiskReasoner."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from ai.knowledge_graph import RiskFactor, RiskPath
from ai.risk_reasoning import RiskReasoner
from models import RiskTag


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _sample_order() -> dict:
    return {
        "order_id": "ORD-001",
        "merchant_id": "M001",
        "customer_ucid": "CUST-001",
        "category": "fashion",
        "price_band": "0-500",
        "payment_mode": "COD",
        "destination_cluster": "north",
        "rto_score": 0.82,
        "address_quality": 0.3,
    }


def _sample_risk_path() -> RiskPath:
    return RiskPath(
        order_id="ORD-001",
        rto_score=0.82,
        factors=[
            RiskFactor(
                factor_type="customer_history",
                description="Customer has 3 prior RTO(s) out of 5 orders with this merchant",
                weight=0.3,
                data={"rto_count": 3, "total_orders": 5, "rto_rate": 0.6},
            ),
            RiskFactor(
                factor_type="address_quality",
                description="Address quality score is 0.30 (below threshold of 0.50)",
                weight=0.12,
                data={"address_quality": 0.3, "threshold": 0.5},
            ),
        ],
        total_risk_weight=0.42,
    )


def _empty_risk_path() -> RiskPath:
    return RiskPath(
        order_id="ORD-002",
        rto_score=0.55,
        factors=[],
        total_risk_weight=0.0,
    )


# ---------------------------------------------------------------------------
# Template fallback tests (no LLM)
# ---------------------------------------------------------------------------

class TestTemplateFallback:
    def test_fallback_returns_risk_tag(self):
        reasoner = RiskReasoner.__new__(RiskReasoner)
        reasoner._llm = None
        reasoner._model_name = "llama3"

        tag = reasoner.generate_risk_tag(_sample_order(), _sample_risk_path())
        assert isinstance(tag, RiskTag)
        assert tag.tag_label != ""
        assert tag.explanation != ""

    def test_fallback_references_order_id(self):
        reasoner = RiskReasoner.__new__(RiskReasoner)
        reasoner._llm = None
        reasoner._model_name = "llama3"

        tag = reasoner.generate_risk_tag(_sample_order(), _sample_risk_path())
        assert "ORD-001" in tag.explanation

    def test_fallback_references_rto_score(self):
        reasoner = RiskReasoner.__new__(RiskReasoner)
        reasoner._llm = None
        reasoner._model_name = "llama3"

        tag = reasoner.generate_risk_tag(_sample_order(), _sample_risk_path())
        assert "0.82" in tag.explanation

    def test_fallback_joins_factor_descriptions(self):
        reasoner = RiskReasoner.__new__(RiskReasoner)
        reasoner._llm = None
        reasoner._model_name = "llama3"

        tag = reasoner.generate_risk_tag(_sample_order(), _sample_risk_path())
        assert "Customer has 3 prior RTO(s)" in tag.explanation
        assert "Address quality score" in tag.explanation

    def test_fallback_empty_factors(self):
        reasoner = RiskReasoner.__new__(RiskReasoner)
        reasoner._llm = None
        reasoner._model_name = "llama3"

        tag = reasoner.generate_risk_tag(_sample_order(), _empty_risk_path())
        assert "no specific risk factors" in tag.explanation

    def test_tag_label_with_factors(self):
        reasoner = RiskReasoner.__new__(RiskReasoner)
        reasoner._llm = None
        reasoner._model_name = "llama3"

        tag = reasoner.generate_risk_tag(_sample_order(), _sample_risk_path())
        assert "High RTO Risk" in tag.tag_label
        assert "Customer History" in tag.tag_label

    def test_tag_label_no_factors(self):
        reasoner = RiskReasoner.__new__(RiskReasoner)
        reasoner._llm = None
        reasoner._model_name = "llama3"

        tag = reasoner.generate_risk_tag(_sample_order(), _empty_risk_path())
        assert tag.tag_label == "Elevated RTO Risk"


# ---------------------------------------------------------------------------
# LLM path tests (mocked)
# ---------------------------------------------------------------------------

class TestLLMPath:
    def test_llm_success_returns_risk_tag(self):
        reasoner = RiskReasoner.__new__(RiskReasoner)
        reasoner._model_name = "llama3"
        mock_llm = MagicMock()
        mock_llm.invoke.return_value = "This order is risky due to repeat RTO customer and poor address quality."
        reasoner._llm = mock_llm

        tag = reasoner.generate_risk_tag(_sample_order(), _sample_risk_path())
        assert isinstance(tag, RiskTag)
        assert "repeat RTO" in tag.explanation
        mock_llm.invoke.assert_called_once()

    def test_llm_failure_falls_back_to_template(self):
        reasoner = RiskReasoner.__new__(RiskReasoner)
        reasoner._model_name = "llama3"
        mock_llm = MagicMock()
        mock_llm.invoke.side_effect = RuntimeError("LLM unavailable")
        reasoner._llm = mock_llm

        tag = reasoner.generate_risk_tag(_sample_order(), _sample_risk_path())
        assert isinstance(tag, RiskTag)
        # Should fall back to template — contains order ID
        assert "ORD-001" in tag.explanation

    def test_llm_tag_label_derived_from_risk_path(self):
        reasoner = RiskReasoner.__new__(RiskReasoner)
        reasoner._model_name = "llama3"
        mock_llm = MagicMock()
        mock_llm.invoke.return_value = "Some LLM explanation."
        reasoner._llm = mock_llm

        tag = reasoner.generate_risk_tag(_sample_order(), _sample_risk_path())
        assert "High RTO Risk" in tag.tag_label


# ---------------------------------------------------------------------------
# Prompt building
# ---------------------------------------------------------------------------

class TestBuildPrompt:
    def test_prompt_contains_order_id(self):
        reasoner = RiskReasoner.__new__(RiskReasoner)
        reasoner._model_name = "llama3"
        prompt = reasoner._build_llm_prompt(_sample_order(), _sample_risk_path())
        assert "ORD-001" in prompt

    def test_prompt_contains_factors(self):
        reasoner = RiskReasoner.__new__(RiskReasoner)
        reasoner._model_name = "llama3"
        prompt = reasoner._build_llm_prompt(_sample_order(), _sample_risk_path())
        assert "customer_history" in prompt
        assert "address_quality" in prompt
