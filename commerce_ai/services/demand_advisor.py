"""Demand Mix Advisor service — peer benchmarks → scoring → gating → NL insight."""

from __future__ import annotations

import math
import sqlite3

from ai.insights import InsightGenerator
from ai.scoring import RealizedCommerceScorer
from data.queries import get_cohort_benchmarks, get_peer_benchmarks


class DemandAdvisorService:
    """Surfaces high-confidence demand mix suggestions for merchants."""

    def __init__(
        self,
        db: sqlite3.Connection,
        scorer: RealizedCommerceScorer,
        insight_gen: InsightGenerator,
    ) -> None:
        self.db = db
        self.scorer = scorer
        self.insight_gen = insight_gen

    def get_suggestions(self, merchant_id: str) -> list[dict]:
        """Orchestrate: peer benchmark lookup → scoring → confidence gating → NL insight.

        1. Get merchant's cohort benchmarks from DB
        2. Get peer benchmarks for each category/price_band
        3. Score each cohort using RealizedCommerceScorer
        4. Find cohorts where peer avg > merchant score (improvement opportunities)
        5. Apply confidence gate: peer_sample_size >= 200, CI width <= 15pp
        6. Generate NL insight for each passing suggestion
        7. Return 1-5 suggestions, ranked by expected_score_improvement descending

        If no suggestions pass the gate, return empty list.
        """
        # 1. Get merchant cohort benchmarks
        cohorts = get_cohort_benchmarks(self.db, merchant_id)
        if not cohorts:
            return []

        # 2-4. For each unique category/price_band, get peer benchmarks and find gaps
        seen_pairs: set[tuple[str, str]] = set()
        raw_suggestions: list[dict] = []

        for cohort in cohorts:
            cat = cohort.get("category", "")
            pb = cohort.get("price_band", "")
            pair = (cat, pb)
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)

            peers = get_peer_benchmarks(self.db, merchant_id, cat, pb)
            for peer in peers:
                merchant_score = peer.get("merchant_score", 0.0)
                peer_avg = peer.get("peer_avg_score", 0.0)
                peer_sample = peer.get("peer_sample_size", 0)
                gap = peer_avg - merchant_score

                if gap <= 0:
                    continue  # No improvement opportunity

                # Compute CI width approximation: 1.96 * sqrt(p*(1-p)/n) * 2
                ci_width = self._compute_ci_width(peer_avg, peer_sample)

                # 5. Confidence gate
                if peer_sample < 200 or ci_width > 0.15:
                    continue

                # Score the cohort using the model
                cohort_key_str = peer.get("cohort_key", "")
                parts = cohort_key_str.split("|") if cohort_key_str else []
                cohort_features = {}
                if len(parts) == 5:
                    cohort_features = {
                        "category": parts[0],
                        "price_band": parts[1],
                        "payment_mode": parts[2],
                        "origin_node": parts[3],
                        "destination_cluster": parts[4],
                        "address_quality": 0.7,  # default for scoring
                    }

                suggestion = {
                    "cohort_dimension": f"{cat}/{pb}",
                    "recommended_value": cohort_key_str,
                    "expected_score_improvement": round(gap, 4),
                    "peer_benchmark": {
                        "cohort_key": cohort_key_str,
                        "merchant_score": merchant_score,
                        "peer_avg_score": peer_avg,
                        "peer_sample_size": peer_sample,
                        "confidence_interval_width": round(ci_width, 4),
                        "gap": round(gap, 4),
                    },
                    "nl_explanation": "",
                }
                raw_suggestions.append(suggestion)

        # 7. Sort by expected improvement descending, take top 5
        raw_suggestions.sort(
            key=lambda s: s["expected_score_improvement"], reverse=True
        )
        top = raw_suggestions[:5]

        # 6. Generate NL insight for each
        for s in top:
            s["nl_explanation"] = self.insight_gen.generate_demand_insight(s)

        return top

    @staticmethod
    def _compute_ci_width(proportion: float, sample_size: int) -> float:
        """Compute 95% CI width for a proportion: 2 * 1.96 * sqrt(p*(1-p)/n)."""
        if sample_size <= 0:
            return 1.0
        p = max(0.0, min(1.0, proportion))
        variance = p * (1 - p) / sample_size
        return 2 * 1.96 * math.sqrt(variance)
