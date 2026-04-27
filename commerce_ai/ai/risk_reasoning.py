"""Risk reasoning module — generates merchant-readable risk explanations.

Uses LangChain + Ollama to convert structured risk paths from the knowledge
graph into human-readable RiskTag objects. Falls back to template-based
generation when the LLM is unavailable.
"""

from __future__ import annotations

import logging

from models import RiskTag
from ai.knowledge_graph import RiskPath

logger = logging.getLogger(__name__)


class RiskReasoner:
    """Generates merchant-readable risk explanations from knowledge graph risk paths."""

    def __init__(self, ollama_model: str = "llama3") -> None:
        """Initialize with LangChain + Ollama. Falls back to templates if LLM unavailable."""
        self._model_name = ollama_model
        self._llm = None
        # Disabled: LangChain/Ollama causes segfault on Python 3.9
        # Template fallback provides the same data-grounded explanations

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate_risk_tag(self, order: dict, risk_path: RiskPath) -> RiskTag:
        """Generate a RiskTag from the knowledge graph risk path.

        1. Try LLM: pass structured risk factors to the LLM for a one-sentence
           merchant-readable explanation.
        2. Fallback: format the risk path factors directly into a template.
        """
        if self._llm is not None:
            try:
                prompt = self._build_llm_prompt(order, risk_path)
                response: str = self._llm.invoke(prompt)
                explanation = response.strip()
                tag_label = self._derive_tag_label(risk_path)
                return RiskTag(tag_label=tag_label, explanation=explanation)
            except Exception:
                logger.warning("LLM call failed — falling back to template")

        return self._template_fallback(order, risk_path)

    # ------------------------------------------------------------------
    # Prompt building
    # ------------------------------------------------------------------

    def _build_llm_prompt(self, order: dict, risk_path: RiskPath) -> str:
        """Build the prompt for the LLM from the risk path."""
        factors_text = "\n".join(
            f"- [{f.factor_type}] (weight {f.weight:.2f}): {f.description}"
            for f in risk_path.factors
        )
        return (
            "You are a logistics risk analyst. Given the following risk factors "
            "for an e-commerce order, write ONE concise sentence explaining why "
            "this order is high-risk. Use merchant-friendly language.\n\n"
            f"Order ID: {order.get('order_id', 'N/A')}\n"
            f"RTO Score: {risk_path.rto_score:.2f}\n"
            f"Total Risk Weight: {risk_path.total_risk_weight:.2f}\n\n"
            f"Contributing factors:\n{factors_text}\n\n"
            "One-sentence explanation:"
        )

    # ------------------------------------------------------------------
    # Template fallback
    # ------------------------------------------------------------------

    def _template_fallback(self, order: dict, risk_path: RiskPath) -> RiskTag:
        """Generate a RiskTag from templates when LLM is unavailable."""
        tag_label = self._derive_tag_label(risk_path)

        if not risk_path.factors:
            explanation = (
                f"Order {order.get('order_id', 'N/A')} has an RTO score of "
                f"{risk_path.rto_score:.2f} but no specific risk factors were identified."
            )
        else:
            parts = [f.description for f in risk_path.factors]
            explanation = (
                f"Order {order.get('order_id', 'N/A')} "
                f"(RTO score {risk_path.rto_score:.2f}): "
                + "; ".join(parts)
                + "."
            )

        return RiskTag(tag_label=tag_label, explanation=explanation)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _derive_tag_label(risk_path: RiskPath) -> str:
        """Derive a short tag label from the top risk factors."""
        if not risk_path.factors:
            return "Elevated RTO Risk"

        top_types = [f.factor_type for f in risk_path.factors[:2]]
        type_labels = {
            "customer_history": "Customer History",
            "cluster_rto": "Bad Cluster",
            "category_performance": "Category Underperformance",
            "address_quality": "Bad Address",
            "payment_mode": "COD Cluster",
        }
        labels = [type_labels.get(t, t.replace("_", " ").title()) for t in top_types]
        return "High RTO Risk - " + " + ".join(labels)
