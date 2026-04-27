"""LLM-based natural language insight generator for merchant-facing explanations.

Uses LangChain + Ollama to produce plain-language explanations for demand
mix suggestions and action recommendations. Falls back to data-grounded
templates when the LLM is unavailable.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class InsightGenerator:
    """LLM-based natural language insight generator for merchant-facing explanations."""

    def __init__(self, ollama_model: str = "llama3") -> None:
        """Initialize with LangChain + Ollama."""
        self._model_name = ollama_model
        self._llm = None
        # Disabled: LangChain/Ollama causes segfault on Python 3.9
        # Template fallback provides data-grounded explanations

    # ------------------------------------------------------------------
    # Demand insights
    # ------------------------------------------------------------------

    def generate_demand_insight(self, suggestion: dict) -> str:
        """Generate a plain-language explanation for a demand mix suggestion.

        Must reference specific data points: cohort dimension, score
        improvement, peer sample size.
        """
        if self._llm is not None:
            try:
                prompt = self._build_demand_prompt(suggestion)
                response: str = self._llm.invoke(prompt)
                return response.strip()
            except Exception:
                logger.warning("LLM call failed for demand insight — using template")

        return self._template_fallback_demand(suggestion)

    # ------------------------------------------------------------------
    # Action insights
    # ------------------------------------------------------------------

    def generate_action_insight(self, order: dict, action: dict) -> str:
        """Generate a plain-language explanation for an action recommendation.

        Must reference: RTO score, contributing risk factors, rationale for
        the recommended intervention.
        """
        if self._llm is not None:
            try:
                prompt = self._build_action_prompt(order, action)
                response: str = self._llm.invoke(prompt)
                return response.strip()
            except Exception:
                logger.warning("LLM call failed for action insight — using template")

        return self._template_fallback_action(order, action)

    # ------------------------------------------------------------------
    # Prompt builders
    # ------------------------------------------------------------------

    @staticmethod
    def _build_demand_prompt(suggestion: dict) -> str:
        """Build the LLM prompt for a demand insight."""
        peer = suggestion.get("peer_benchmark", {})
        return (
            "You are a logistics advisor. Write ONE concise sentence explaining "
            "this demand mix suggestion to a merchant. Reference specific numbers.\n\n"
            f"Cohort dimension: {suggestion.get('cohort_dimension', 'N/A')}\n"
            f"Recommended value: {suggestion.get('recommended_value', 'N/A')}\n"
            f"Expected score improvement: {suggestion.get('expected_score_improvement', 0):.2f}\n"
            f"Merchant score: {peer.get('merchant_score', 0):.2f}\n"
            f"Peer average score: {peer.get('peer_avg_score', 0):.2f}\n"
            f"Peer sample size: {peer.get('peer_sample_size', 0)}\n"
            f"Gap: {peer.get('gap', 0):.2f}\n\n"
            "One-sentence explanation:"
        )

    @staticmethod
    def _build_action_prompt(order: dict, action: dict) -> str:
        """Build the LLM prompt for an action insight."""
        risk_factors = action.get("risk_factors", [])
        factors_text = "; ".join(risk_factors) if risk_factors else "no specific factors listed"
        return (
            "You are a logistics advisor. Write ONE concise sentence explaining "
            "why this intervention is recommended for a risky order. "
            "Reference the RTO score and risk factors.\n\n"
            f"Order ID: {order.get('order_id', 'N/A')}\n"
            f"RTO Score: {order.get('rto_score', 0):.2f}\n"
            f"Intervention: {action.get('intervention_type', 'N/A')}\n"
            f"Confidence: {action.get('confidence_score', 0):.2f}\n"
            f"Risk factors: {factors_text}\n\n"
            "One-sentence explanation:"
        )

    # ------------------------------------------------------------------
    # Template fallbacks
    # ------------------------------------------------------------------

    @staticmethod
    def _template_fallback_demand(suggestion: dict) -> str:
        """Template fallback for demand insights."""
        peer = suggestion.get("peer_benchmark", {})
        dimension = suggestion.get("cohort_dimension", "unknown dimension")
        recommended = suggestion.get("recommended_value", "N/A")
        improvement = suggestion.get("expected_score_improvement", 0)
        peer_avg = peer.get("peer_avg_score", 0)
        merchant_score = peer.get("merchant_score", 0)
        sample_size = peer.get("peer_sample_size", 0)

        return (
            f"Shifting your {dimension} mix towards '{recommended}' could improve "
            f"your realized commerce score by {improvement:.2f}. "
            f"Your current score ({merchant_score:.2f}) trails the peer average "
            f"({peer_avg:.2f}) based on {sample_size} peer orders."
        )

    @staticmethod
    def _template_fallback_action(order: dict, action: dict) -> str:
        """Template fallback for action insights."""
        order_id = order.get("order_id", "N/A")
        rto_score = order.get("rto_score", 0)
        intervention = action.get("intervention_type", "no_action")
        confidence = action.get("confidence_score", 0)
        risk_factors = action.get("risk_factors", [])

        factors_str = "; ".join(risk_factors) if risk_factors else "general risk elevation"

        return (
            f"Order {order_id} has an RTO score of {rto_score:.2f}. "
            f"Recommended action: {intervention} "
            f"(confidence {confidence:.2f}). "
            f"Contributing factors: {factors_str}."
        )
