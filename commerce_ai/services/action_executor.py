"""Action executor service — permission checks, rate limits, execution, logging."""

from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime

from data.queries import log_intervention, check_rate_limits, get_merchant_permissions
from models import DELHIVERY_EXECUTABLE, MERCHANT_OWNED, InterventionType


class ActionExecutorService:
    """Executes interventions with permission and rate limit checks."""

    def __init__(self, db: sqlite3.Connection) -> None:
        self.db = db

    def execute(
        self,
        merchant_id: str,
        order_id: str,
        intervention_type: str,
        confidence_score: float | None = None,
    ) -> dict:
        """Check permissions → rate limits → execute → log → return result.

        If execution fails, retry once.
        """
        # Check permission
        perms = get_merchant_permissions(self.db, merchant_id)
        if not perms.get("permissions", {}).get(intervention_type, False):
            return {
                "success": False,
                "intervention_log_id": "",
                "error_message": f"Permission denied for {intervention_type}",
            }

        # Check rate limits
        rate_status = check_rate_limits(self.db, merchant_id)
        if not rate_status["is_within_limits"]:
            return {
                "success": False,
                "intervention_log_id": "",
                "error_message": "Rate limit exceeded",
            }

        # Execute and log
        result = self._execute_and_log(
            merchant_id, order_id, intervention_type, confidence_score
        )

        # Retry once on failure
        if not result["success"]:
            result = self._execute_and_log(
                merchant_id, order_id, intervention_type, confidence_score
            )

        return result

    def categorize_action(self, intervention_type: str) -> str:
        """Return 'delhivery' or 'merchant' based on action ownership."""
        try:
            itype = InterventionType(intervention_type)
        except ValueError:
            return "merchant"

        if itype in DELHIVERY_EXECUTABLE:
            return "delhivery"
        if itype in MERCHANT_OWNED:
            return "merchant"
        return "merchant"

    def retry_failed(self, intervention_log_id: str) -> dict:
        """Retry a failed intervention once."""
        row = self.db.execute(
            "SELECT * FROM interventions WHERE intervention_id = ?",
            (intervention_log_id,),
        ).fetchone()

        if row is None:
            return {
                "success": False,
                "intervention_log_id": intervention_log_id,
                "error_message": "Intervention log not found",
            }

        row_dict = dict(row)
        return self._execute_and_log(
            row_dict["merchant_id"],
            row_dict["order_id"],
            row_dict["intervention_type"],
            row_dict.get("confidence_score"),
        )

    def _execute_and_log(
        self,
        merchant_id: str,
        order_id: str,
        intervention_type: str,
        confidence_score: float | None,
    ) -> dict:
        """Execute the intervention and log it. Returns ExecutionResult-like dict."""
        intervention_id = str(uuid.uuid4())
        now = datetime.utcnow()
        action_owner = self.categorize_action(intervention_type)

        intervention = {
            "intervention_id": intervention_id,
            "order_id": order_id,
            "merchant_id": merchant_id,
            "intervention_type": intervention_type,
            "action_owner": action_owner,
            "initiated_by": "system",
            "confidence_score": confidence_score,
            "outcome": "pending",
            "executed_at": now.isoformat(),
            "completed_at": None,
        }

        try:
            log_intervention(self.db, intervention)
            return {
                "success": True,
                "intervention_log_id": intervention_id,
                "error_message": None,
            }
        except Exception as e:
            return {
                "success": False,
                "intervention_log_id": intervention_id,
                "error_message": str(e),
            }
