"""Order Action Engine — the core 3-step processing pipeline."""

from __future__ import annotations

import logging


import config as cfg
from ai.insights import InsightGenerator
from ai.knowledge_graph import RiskKnowledgeGraph
from ai.next_best_action import NextBestActionPolicy
from ai.risk_reasoning import RiskReasoner
from data.queries import (
    get_historical_analogs,
    get_merchant_permissions,
    get_recent_orders,
)
from services.auto_cancel import AutoCancelService
from services.impulse_detector import ImpulseDetector

logger = logging.getLogger(__name__)


class OrderActionEngineService:
    """Orchestrates the 3-step order processing priority flow."""

    def __init__(
        self,
        db,
        risk_reasoner: RiskReasoner,
        nba_policy: NextBestActionPolicy,
        insight_gen: InsightGenerator,
        auto_cancel_service: AutoCancelService,
        impulse_detector: ImpulseDetector,
        knowledge_graph: RiskKnowledgeGraph,
    ) -> None:
        self.db = db
        self.risk_reasoner = risk_reasoner
        self.nba_policy = nba_policy
        self.insight_gen = insight_gen
        self.auto_cancel_service = auto_cancel_service
        self.impulse_detector = impulse_detector
        self.knowledge_graph = knowledge_graph

    def process_order(self, order: dict, merchant_config: dict) -> dict:
        """3-step processing priority:

        Step 1: Auto-cancel check — if rto_score > auto_cancel_threshold AND enabled → cancel, STOP
        Step 2: Standard flow — enrich → build risk path → risk tag → NBA → confidence gate
        Step 3: Express upgrade check — if impulsive AND enabled AND rto in risk range → upgrade

        Returns ProcessedOrder-like dict with all results.
        """
        result: dict = {
            "order": order,
            "enrichment": None,
            "risk_tag": None,
            "next_best_action": None,
            "nl_explanation": None,
            "auto_cancel_result": None,
            "impulse_result": None,
            "express_upgrade_result": None,
        }

        # ---- Step 1: Auto-cancel check (highest priority) ----
        auto_cancel_result = self.auto_cancel_service.check_and_cancel(
            order, merchant_config
        )
        result["auto_cancel_result"] = auto_cancel_result

        if auto_cancel_result.get("cancelled", False):
            # Order cancelled — stop processing
            return result

        # ---- Step 2: Standard flow ----
        # 2a. Enrich with historical analogs
        enrichment = self._enrich_order(order)
        result["enrichment"] = enrichment

        # 2b. Build risk path from knowledge graph and generate risk tag
        risk_path = self.knowledge_graph.get_risk_path(order, self.db)
        risk_tag = self.risk_reasoner.generate_risk_tag(order, risk_path)
        result["risk_tag"] = {"tag_label": risk_tag.tag_label, "explanation": risk_tag.explanation}

        # 2c. Get NBA recommendation
        rto_score = order.get("rto_score", 0.0)
        nba_result = {"intervention_type": "no_action", "confidence_score": 0.0}

        if rto_score > cfg.RISK_THRESHOLD:
            nba_result = self.nba_policy.recommend(order)

        # 2d. Confidence gate: suppress if below MIN_CONFIDENCE
        if nba_result.get("confidence_score", 0.0) < cfg.MIN_CONFIDENCE:
            nba_result = {"intervention_type": "no_action", "confidence_score": nba_result.get("confidence_score", 0.0)}

        result["next_best_action"] = nba_result

        # 2e. Generate NL explanation
        risk_factors = [f.description for f in risk_path.factors] if risk_path.factors else []
        action_with_factors = {**nba_result, "risk_factors": risk_factors}
        nl_explanation = self.insight_gen.generate_action_insight(order, action_with_factors)
        result["nl_explanation"] = nl_explanation

        # ---- Step 3: Express upgrade check ----
        impulse_result = self.impulse_detector.detect(order)
        result["impulse_result"] = impulse_result

        auto_cancel_threshold = merchant_config.get("auto_cancel_threshold", cfg.AUTO_CANCEL_THRESHOLD)
        express_result = self.impulse_detector.upgrade_to_express(
            order,
            merchant_config,
            impulse_result,
            risk_threshold=risk_threshold,
            auto_cancel_threshold=auto_cancel_threshold,
        )
        result["express_upgrade_result"] = express_result

        return result

    def get_live_feed(
        self, merchant_id: str, risk_threshold: float = cfg.RISK_THRESHOLD
    ) -> list[dict]:
        """Get recent orders, process each, return sorted by rto_score descending."""
        orders = get_recent_orders(self.db, merchant_id, limit=20)
        if not orders:
            return []

        merchant_config = get_merchant_permissions(self.db, merchant_id)
        processed: list[dict] = []

        for order in orders:
            try:
                result = self.process_order(order, merchant_config)
                processed.append(result)
            except Exception:
                logger.exception(
                    "Order processing failed: order=%s merchant=%s",
                    order.get("order_id"), merchant_id,
                )
                processed.append({"order": order, "error": "processing_failed"})

        # Sort by rto_score descending
        processed.sort(
            key=lambda p: p.get("order", {}).get("rto_score", 0.0), reverse=True
        )
        return processed

    def _enrich_order(self, order: dict) -> dict:
        """Enrich order with historical analog stats."""
        analogs = get_historical_analogs(
            self.db,
            category=order.get("category", ""),
            price_band=order.get("price_band", ""),
            payment_mode=order.get("payment_mode", ""),
            origin_node=order.get("origin_node", ""),
            destination_cluster=order.get("destination_cluster", ""),
        )
        return {
            "order": order,
            "historical_rto_rate": analogs.get("rto_rate") or 0.0,
            "historical_sample_size": analogs.get("sample_size", 0),
            "peer_avg_rto_rate": analogs.get("peer_avg_rto_rate", 0.0),
        }
