"""Outbound communication orchestrator — coordinates WA → wait → voice flow."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta
from typing import Any

import config as cfg

logger = logging.getLogger(__name__)

# Type alias for the communication log dict returned by all methods
CommLog = dict[str, Any]


class OutboundOrchestrator:
    """Orchestrates outbound customer communication for risky COD orders."""

    def __init__(
        self,
        db,
        whatsapp_client,
        voice_ai_client,
        issue_router,
        escalation_window_hours: float = cfg.ESCALATION_WINDOW_HOURS,
    ) -> None:
        self.db = db
        self.whatsapp_client = whatsapp_client
        self.voice_ai_client = voice_ai_client
        self.issue_router = issue_router
        self.escalation_window_hours = escalation_window_hours

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def trigger_outbound(self, order: dict[str, Any], issue_type: str) -> CommLog:
        """Send a WhatsApp message and schedule voice escalation if needed."""
        order_id = order["order_id"]
        merchant_id = order["merchant_id"]
        customer_ucid = order["customer_ucid"]

        if not self.check_communication_limits(order_id, issue_type):
            logger.warning(
                "Communication limit reached: order=%s issue=%s", order_id, issue_type
            )
            return self._build_log(
                order_id, merchant_id, customer_ucid, issue_type,
                channel="whatsapp", status="failed", error="Communication limit reached",
            )

        if not self._check_customer_daily_cap(customer_ucid):
            logger.warning(
                "Customer daily communication cap reached: customer=%s", customer_ucid
            )
            return self._build_log(
                order_id, merchant_id, customer_ucid, issue_type,
                channel="whatsapp", status="failed",
                error="Customer daily communication cap reached",
            )

        if not self._check_permission(merchant_id, issue_type):
            logger.warning(
                "Merchant permission not granted: merchant=%s issue=%s",
                merchant_id, issue_type,
            )
            return self._build_log(
                order_id, merchant_id, customer_ucid, issue_type,
                channel="whatsapp", status="failed",
                error="Merchant permission not granted",
            )

        template_fields = self.issue_router.get_template_fields(order, issue_type)
        wa_result = self.whatsapp_client.send_template_message(
            customer_ucid, issue_type, template_fields
        )

        if wa_result.get("status") != "sent":
            err = wa_result.get("error_message", "WhatsApp send failed")
            logger.error(
                "WhatsApp send failed: order=%s error=%s", order_id, err
            )
            return self._build_log(
                order_id, merchant_id, customer_ucid, issue_type,
                channel="whatsapp", status="failed",
                message_id=wa_result.get("message_id"), error=err,
            )

        escalation_at = (
            datetime.utcnow() + timedelta(hours=self.escalation_window_hours)
        ).isoformat()

        entry = self._build_log(
            order_id, merchant_id, customer_ucid, issue_type,
            channel="whatsapp", status="sent",
            message_id=wa_result["message_id"],
            escalation_scheduled_at=escalation_at,
        )
        self._persist(entry)
        logger.info(
            "WhatsApp sent: order=%s message_id=%s", order_id, wa_result["message_id"]
        )
        return entry

    def check_and_escalate(self, comm_id: str) -> CommLog:
        """Check WhatsApp response and escalate to voice if no reply."""
        entry = self._get(comm_id)
        if entry is None:
            return {"error": "Communication log not found", "status": "failed"}

        message_id = entry.get("message_id")
        if message_id:
            try:
                resp = self.whatsapp_client.check_response(message_id)
            except Exception:
                logger.exception(
                    "WhatsApp check_response failed: comm_id=%s message_id=%s",
                    comm_id, message_id,
                )
                resp = {}

            if resp.get("responded"):
                resolution = self._derive_resolution(
                    entry["issue_type"], resp.get("response_content", "")
                )
                self._update(
                    comm_id, "responded",
                    resolution=resolution,
                    customer_response=resp.get("response_content"),
                    responded_at=resp.get("responded_at"),
                )
                self.update_order_resolution(entry["order_id"], resolution)
                return {**entry, "status": "responded", "resolution": resolution}

        if not self._voice_ok(entry["order_id"], entry["issue_type"]):
            self._update(comm_id, "no_response")
            return {**entry, "status": "no_response"}

        call_context = {
            "order_summary": {
                "order_id": entry["order_id"],
                "customer_ucid": entry["customer_ucid"],
                "merchant_name": "",
            }
        }
        try:
            call_result = self.voice_ai_client.initiate_call(
                entry["customer_ucid"], entry["issue_type"], call_context
            )
        except Exception:
            logger.exception(
                "Voice call initiation failed: order=%s", entry["order_id"]
            )
            call_result = {"status": "failed", "call_id": None, "resolution": None}

        voice_log = self._build_log(
            entry["order_id"], entry["merchant_id"], entry["customer_ucid"],
            entry["issue_type"],
            channel="voice",
            status=self._map_call_status(call_result.get("status", "failed")),
            message_id=call_result.get("call_id"),
            resolution=call_result.get("resolution"),
        )
        self._persist(voice_log)

        if call_result.get("resolution"):
            self.update_order_resolution(entry["order_id"], call_result["resolution"])

        return voice_log

    def update_order_resolution(self, order_id: str, resolution: str) -> None:
        """Update the order delivery outcome after a successful communication."""
        try:
            self.db.execute(
                "UPDATE orders SET delivery_outcome = ? WHERE order_id = ?",
                ("pending", order_id),
            )
            self.db.commit()
            logger.info(
                "Order outcome updated: order=%s resolution=%s", order_id, resolution
            )
        except Exception:
            logger.exception(
                "Failed to update order resolution: order=%s", order_id
            )

    def get_communication_status(self, order_id: str) -> list[CommLog]:
        """Return all communication logs for an order, newest first."""
        try:
            rows = self.db.execute(
                "SELECT * FROM communication_logs WHERE order_id = ? ORDER BY sent_at DESC",
                (order_id,),
            ).fetchall()
            return [dict(r) for r in rows]
        except Exception:
            logger.exception(
                "Failed to fetch communication status: order=%s", order_id
            )
            return []

    def check_communication_limits(self, order_id: str, issue_type: str) -> bool:
        """Return True if this order/issue combination hasn't hit the WA limit."""
        try:
            row = self.db.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM communication_logs
                WHERE order_id = ? AND issue_type = ? AND channel = 'whatsapp'
                """,
                (order_id, issue_type),
            ).fetchone()
            return (row["cnt"] if row else 0) < 1
        except Exception:
            logger.exception(
                "Failed to check communication limits: order=%s", order_id
            )
            return True  # fail-open so orchestration can proceed

    def fallback_to_next_intervention(self, order_id: str) -> None:
        """Mark all unresolved communications as no_response."""
        try:
            self.db.execute(
                """
                UPDATE communication_logs
                SET status = 'no_response'
                WHERE order_id = ? AND resolution IS NULL
                """,
                (order_id,),
            )
            self.db.commit()
            logger.info("Fallback applied for order: %s", order_id)
        except Exception:
            logger.exception(
                "Failed to apply fallback: order=%s", order_id
            )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _check_customer_daily_cap(self, customer_ucid: str) -> bool:
        """Return True if this customer hasn't exceeded their daily comms cap."""
        try:
            from datetime import timedelta
            cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat()
            row = self.db.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM communication_logs
                WHERE customer_ucid = ? AND sent_at >= ?
                """,
                (customer_ucid, cutoff),
            ).fetchone()
            return (row["cnt"] if row else 0) < cfg.DEFAULT_CUSTOMER_DAILY_COMM_CAP
        except Exception:
            logger.exception(
                "Failed to check customer daily cap: customer=%s", customer_ucid
            )
            return True  # fail-open

    def _check_permission(self, merchant_id: str, issue_type: str) -> bool:
        """Return True if merchant has enabled the relevant outreach intervention."""
        intervention_map = {
            "address_enrichment": "address_enrichment_outreach",
            "cod_to_prepaid": "cod_to_prepaid_outreach",
        }
        intervention_type = intervention_map.get(issue_type, issue_type)
        try:
            row = self.db.execute(
                """
                SELECT is_enabled
                FROM merchant_permissions
                WHERE merchant_id = ? AND intervention_type = ?
                """,
                (merchant_id, intervention_type),
            ).fetchone()
            return bool(row["is_enabled"]) if row else False
        except Exception:
            logger.exception(
                "Failed to check permission: merchant=%s issue=%s",
                merchant_id, issue_type,
            )
            return False

    def _voice_ok(self, order_id: str, issue_type: str) -> bool:
        """Return True if a voice call hasn't already been placed for this order/issue."""
        try:
            row = self.db.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM communication_logs
                WHERE order_id = ? AND issue_type = ? AND channel = 'voice'
                """,
                (order_id, issue_type),
            ).fetchone()
            return (row["cnt"] if row else 0) < 1
        except Exception:
            logger.exception(
                "Failed to check voice limit: order=%s", order_id
            )
            return True

    def _build_log(
        self,
        order_id: str,
        merchant_id: str,
        customer_ucid: str,
        issue_type: str,
        *,
        channel: str,
        status: str,
        message_id: str | None = None,
        resolution: str | None = None,
        error: str | None = None,
        escalation_scheduled_at: str | None = None,
    ) -> CommLog:
        return {
            "communication_id": "comm_" + uuid.uuid4().hex[:12],
            "order_id": order_id,
            "merchant_id": merchant_id,
            "customer_ucid": customer_ucid,
            "issue_type": issue_type,
            "channel": channel,
            "template_id": issue_type if channel == "whatsapp" else None,
            "message_id": message_id,
            "status": status,
            "customer_response": None,
            "resolution": resolution,
            "sent_at": datetime.utcnow().isoformat(),
            "responded_at": None,
            "escalation_scheduled_at": escalation_scheduled_at,
            "error_message": error,
        }

    def _persist(self, entry: CommLog) -> None:
        try:
            self.db.execute(
                """
                INSERT INTO communication_logs (
                    communication_id, order_id, merchant_id, customer_ucid,
                    issue_type, channel, template_id, message_id, status,
                    customer_response, resolution, sent_at, responded_at,
                    escalation_scheduled_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    entry["communication_id"], entry["order_id"],
                    entry["merchant_id"], entry["customer_ucid"],
                    entry["issue_type"], entry["channel"],
                    entry.get("template_id"), entry.get("message_id"),
                    entry["status"], entry.get("customer_response"),
                    entry.get("resolution"), entry["sent_at"],
                    entry.get("responded_at"), entry.get("escalation_scheduled_at"),
                ),
            )
            self.db.commit()
        except Exception:
            logger.exception(
                "Failed to persist communication log: comm_id=%s",
                entry.get("communication_id"),
            )

    def _get(self, comm_id: str) -> CommLog | None:
        try:
            row = self.db.execute(
                "SELECT * FROM communication_logs WHERE communication_id = ?",
                (comm_id,),
            ).fetchone()
            return dict(row) if row else None
        except Exception:
            logger.exception(
                "Failed to fetch communication log: comm_id=%s", comm_id
            )
            return None

    def _update(
        self,
        comm_id: str,
        status: str,
        *,
        resolution: str | None = None,
        customer_response: str | None = None,
        responded_at: str | None = None,
    ) -> None:
        try:
            self.db.execute(
                """
                UPDATE communication_logs
                SET status = ?, resolution = ?, customer_response = ?, responded_at = ?
                WHERE communication_id = ?
                """,
                (status, resolution, customer_response, responded_at, comm_id),
            )
            self.db.commit()
        except Exception:
            logger.exception(
                "Failed to update communication log: comm_id=%s", comm_id
            )

    @staticmethod
    def _map_call_status(status: str) -> str:
        return {
            "completed": "call_completed",
            "failed": "call_failed",
            "no_answer": "call_no_answer",
        }.get(status, "call_initiated")

    @staticmethod
    def _derive_resolution(issue_type: str, content: str) -> str:
        if issue_type == "address_enrichment":
            return "address_updated"
        if issue_type == "cod_to_prepaid":
            lo = (content or "").lower()
            if any(k in lo for k in ("yes", "pay", "prepaid", "switch")):
                return "payment_converted"
        return "no_resolution"
