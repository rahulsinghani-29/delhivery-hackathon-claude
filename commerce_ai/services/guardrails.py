"""Guardrails service — rate limits, permissions, confidence gating, suppression logging."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime

from data.queries import check_rate_limits, get_merchant_permissions


class GuardrailsService:
    """Enforces safety limits on automated interventions."""

    def __init__(self, db: sqlite3.Connection) -> None:
        self.db = db

    def check_rate_limit(self, merchant_id: str) -> bool:
        """Return True if merchant is within both daily AND hourly limits."""
        status = check_rate_limits(self.db, merchant_id)
        return status["is_within_limits"]

    def check_permission(self, merchant_id: str, intervention_type: str) -> bool:
        """Return True if merchant has enabled this intervention type."""
        perms = get_merchant_permissions(self.db, merchant_id)
        return perms.get("permissions", {}).get(intervention_type, False)

    def apply_confidence_gate(self, recommendation: dict, gate_type: str) -> bool:
        """Check if a recommendation passes the confidence gate.

        gate_type 'demand': peer_sample_size >= 200 AND confidence_interval_width <= 15pp (0.15)
        gate_type 'action': confidence_score >= 0.6
        """
        if gate_type == "demand":
            peer_sample = recommendation.get("peer_sample_size", 0)
            ci_width = recommendation.get("confidence_interval_width", 1.0)
            return peer_sample >= 200 and ci_width <= 0.15
        elif gate_type == "action":
            confidence = recommendation.get("confidence_score", 0.0)
            return confidence >= 0.6
        return False

    def log_suppression(
        self, merchant_id: str, recommendation: dict, reason: str
    ) -> None:
        """Log a suppressed recommendation to the suppressed_recommendations table."""
        rec_type = recommendation.get("type", "unknown")
        rec_data = json.dumps(recommendation, default=str)
        self.db.execute(
            """
            INSERT INTO suppressed_recommendations
                (merchant_id, recommendation_type, recommendation_data, suppression_reason)
            VALUES (?, ?, ?, ?)
            """,
            (merchant_id, rec_type, rec_data, reason),
        )
        self.db.commit()
