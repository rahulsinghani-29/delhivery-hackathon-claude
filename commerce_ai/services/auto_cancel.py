"""Auto-cancel service — pure rule-based cancellation of extreme-risk orders."""

from __future__ import annotations

import logging
import sqlite3
import uuid
from datetime import datetime

import config as cfg
from data.queries import log_intervention

logger = logging.getLogger(__name__)


class AutoCancelService:
    """Pure threshold check for auto-cancelling high-RTO orders."""

    def __init__(self, db: sqlite3.Connection) -> None:
        self.db = db

    def check_and_cancel(self, order: dict, merchant_config: dict) -> dict:
        """Check if an order should be auto-cancelled.

        1. If not auto_cancel_enabled → cancelled=False, reason='auto_cancel_disabled'
        2. If rto_score <= threshold → cancelled=False, reason='below_threshold'
        3. If rto_score > threshold AND enabled → cancel, log, return cancelled=True

        Returns AutoCancelResult-like dict.
        """
        order_id = order.get("order_id", "")
        merchant_id = order.get("merchant_id", "")
        rto_score = order.get("rto_score", 0.0)
        threshold = merchant_config.get("auto_cancel_threshold", cfg.AUTO_CANCEL_THRESHOLD)
        enabled = merchant_config.get("auto_cancel_enabled", False)

        if not enabled:
            return {
                "cancelled": False,
                "reason": "auto_cancel_disabled",
                "order_id": order_id,
                "merchant_id": merchant_id,
                "rto_score": rto_score,
                "threshold": threshold,
                "cancelled_at": None,
            }

        if rto_score <= threshold:
            return {
                "cancelled": False,
                "reason": "below_threshold",
                "order_id": order_id,
                "merchant_id": merchant_id,
                "rto_score": rto_score,
                "threshold": threshold,
                "cancelled_at": None,
            }

        # Cancel the order
        now = datetime.utcnow()
        intervention = {
            "intervention_id": str(uuid.uuid4()),
            "order_id": order_id,
            "merchant_id": merchant_id,
            "intervention_type": "auto_cancel",
            "action_owner": "delhivery",
            "initiated_by": "system",
            "confidence_score": None,
            "outcome": "cancelled",
            "executed_at": now.isoformat(),
            "completed_at": now.isoformat(),
        }
        try:
            log_intervention(self.db, intervention)
            logger.info("Auto-cancel logged: order=%s rto_score=%.3f", order_id, rto_score)
        except Exception:
            logger.exception("Failed to log auto-cancel intervention: order=%s", order_id)

        return {
            "cancelled": True,
            "reason": "rto_score_exceeded_threshold",
            "order_id": order_id,
            "merchant_id": merchant_id,
            "rto_score": rto_score,
            "threshold": threshold,
            "cancelled_at": now.isoformat(),
        }
