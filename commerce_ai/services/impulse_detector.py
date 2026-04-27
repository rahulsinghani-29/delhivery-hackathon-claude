"""Impulse detector — deterministic detection of impulsive buying patterns."""

from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime

from data.queries import get_customer_delivered_orders, log_intervention


class ImpulseDetector:
    """Detects impulsive buying patterns and upgrades shipping to Express."""

    def __init__(
        self, db: sqlite3.Connection, impulse_categories: list[str] | None = None
    ) -> None:
        self.db = db
        if impulse_categories is None:
            impulse_categories = ["fashion", "beauty"]
        self.impulse_categories = impulse_categories

    def detect(self, order: dict) -> dict:
        """Check 4 impulse signals and return ImpulseResult-like dict.

        1. Late-night: created_at hour in {23, 0, 1, 2, 3}
        2. COD: payment_mode == 'COD'
        3. First-time buyer: no prior delivered orders from customer to merchant
        4. High-impulse category: category in impulse_categories

        is_impulsive = True if signal_count >= 3.
        """
        signals: list[str] = []

        # 1. Late-night check
        created_at = order.get("created_at")
        hour = self._extract_hour(created_at)
        if hour is not None and hour in {23, 0, 1, 2, 3}:
            signals.append("late_night")

        # 2. COD check
        if order.get("payment_mode") == "COD":
            signals.append("cod_payment")

        # 3. First-time buyer check
        customer_ucid = order.get("customer_ucid", "")
        merchant_id = order.get("merchant_id", "")
        if customer_ucid and merchant_id:
            delivered = get_customer_delivered_orders(
                self.db, customer_ucid, merchant_id
            )
            if not delivered:
                signals.append("first_time_buyer")

        # 4. High-impulse category
        category = order.get("category", "")
        if category.lower() in [c.lower() for c in self.impulse_categories]:
            signals.append("high_impulse_category")

        return {
            "is_impulsive": len(signals) >= 3,
            "matched_signals": signals,
            "signal_count": len(signals),
            "order_id": order.get("order_id", ""),
            "rto_score": order.get("rto_score", 0.0),
        }

    def upgrade_to_express(
        self,
        order: dict,
        merchant_config: dict,
        impulse_result: dict,
        risk_threshold: float,
        auto_cancel_threshold: float,
    ) -> dict:
        """Upgrade shipping to Express if all conditions are met.

        Conditions:
        1. impulse_result.is_impulsive == True
        2. rto_score > risk_threshold
        3. rto_score <= auto_cancel_threshold
        4. merchant has express_upgrade_enabled
        """
        order_id = order.get("order_id", "")
        merchant_id = order.get("merchant_id", "")
        rto_score = order.get("rto_score", 0.0)
        original_mode = order.get("shipping_mode", "surface")
        matched_signals = impulse_result.get("matched_signals", [])

        base = {
            "order_id": order_id,
            "merchant_id": merchant_id,
            "rto_score": rto_score,
            "matched_signals": matched_signals,
            "original_shipping_mode": original_mode,
        }

        if not impulse_result.get("is_impulsive", False):
            return {**base, "upgraded": False, "reason": "not_impulsive",
                    "new_shipping_mode": None, "upgraded_at": None}

        if rto_score <= risk_threshold:
            return {**base, "upgraded": False, "reason": "below_risk_threshold",
                    "new_shipping_mode": None, "upgraded_at": None}

        if rto_score > auto_cancel_threshold:
            return {**base, "upgraded": False, "reason": "above_auto_cancel_threshold",
                    "new_shipping_mode": None, "upgraded_at": None}

        if not merchant_config.get("express_upgrade_enabled", False):
            return {**base, "upgraded": False, "reason": "express_upgrade_disabled",
                    "new_shipping_mode": None, "upgraded_at": None}

        # All conditions met — upgrade
        now = datetime.utcnow()
        intervention = {
            "intervention_id": str(uuid.uuid4()),
            "order_id": order_id,
            "merchant_id": merchant_id,
            "intervention_type": "express_upgrade",
            "action_owner": "delhivery",
            "initiated_by": "system",
            "confidence_score": None,
            "outcome": "upgraded",
            "executed_at": now.isoformat(),
            "completed_at": now.isoformat(),
        }
        try:
            log_intervention(self.db, intervention)
        except Exception:
            pass

        return {
            **base,
            "upgraded": True,
            "reason": "impulsive_and_enabled",
            "new_shipping_mode": "express",
            "upgraded_at": now.isoformat(),
        }

    @staticmethod
    def _extract_hour(created_at) -> int | None:
        """Extract hour from various datetime representations."""
        if created_at is None:
            return None
        if isinstance(created_at, datetime):
            return created_at.hour
        if isinstance(created_at, str):
            try:
                dt = datetime.fromisoformat(created_at)
                return dt.hour
            except (ValueError, TypeError):
                return None
        return None
