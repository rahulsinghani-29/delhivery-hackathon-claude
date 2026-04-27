"""Unit tests for services/outbound_orchestrator.py — OutboundOrchestrator."""

from __future__ import annotations

import sqlite3
from unittest.mock import MagicMock

import pytest

from data.db import init_db, get_db
from services.outbound_orchestrator import OutboundOrchestrator


@pytest.fixture
def db(tmp_path):
    db_path = str(tmp_path / "test.db")
    init_db(db_path)
    conn = get_db(db_path)
    conn.execute("INSERT INTO merchants (merchant_id, name) VALUES ('M001', 'Test')")
    conn.execute(
        "INSERT INTO warehouse_nodes (node_id, merchant_id, city, state, pincode) "
        "VALUES ('WH001', 'M001', 'Delhi', 'DL', '110001')"
    )
    conn.execute(
        """INSERT INTO orders (order_id, merchant_id, customer_ucid, category, price_band,
           payment_mode, origin_node, destination_pincode, destination_cluster,
           address_quality, rto_score, delivery_outcome, created_at)
           VALUES ('ORD001', 'M001', 'CUST001', 'fashion', '0-500', 'COD', 'WH001',
                   '110001', 'north', 0.3, 0.7, 'pending', '2024-01-01T10:00:00')"""
    )
    # Enable address_enrichment_outreach permission
    conn.execute(
        """INSERT INTO merchant_permissions (merchant_id, intervention_type, is_enabled)
           VALUES ('M001', 'address_enrichment_outreach', 1)"""
    )
    conn.execute(
        """INSERT INTO merchant_permissions (merchant_id, intervention_type, is_enabled)
           VALUES ('M001', 'cod_to_prepaid_outreach', 1)"""
    )
    conn.commit()
    yield conn
    conn.close()


def _mock_wa_client(send_status="sent", responded=False):
    client = MagicMock()
    client.send_template_message.return_value = {
        "message_id": "wa_test123",
        "status": send_status,
        "error_message": None if send_status == "sent" else "Send failed",
    }
    client.check_response.return_value = {
        "responded": responded,
        "response_content": "Yes, correct" if responded else None,
        "responded_at": "2024-01-01T12:00:00" if responded else None,
    }
    return client


def _mock_voice_client(status="completed", resolution="address_updated"):
    client = MagicMock()
    client.initiate_call.return_value = {
        "call_id": "gemini_test456",
        "status": status,
        "resolution": resolution,
        "transcript_summary": "Mock transcript",
    }
    return client


def _mock_router():
    router = MagicMock()
    router.get_template_fields.return_value = {
        "order_id": "ORD001",
        "customer_name": "Rajesh",
        "merchant_name": "Amazon",
        "current_address": "Central Park 2",
    }
    return router


def _order():
    return {
        "order_id": "ORD001",
        "merchant_id": "M001",
        "customer_ucid": "CUST001",
        "customer_name": "Rajesh",
        "merchant_name": "Amazon",
    }


@pytest.fixture
def orchestrator(db):
    return OutboundOrchestrator(
        db=db,
        whatsapp_client=_mock_wa_client(),
        voice_ai_client=_mock_voice_client(),
        issue_router=_mock_router(),
        escalation_window_hours=2.0,
    )


class TestTriggerOutbound:
    def test_successful_send(self, orchestrator):
        result = orchestrator.trigger_outbound(_order(), "address_enrichment")
        assert result["status"] == "sent"
        assert result["channel"] == "whatsapp"
        assert result["order_id"] == "ORD001"
        assert result["escalation_scheduled_at"] is not None

    def test_persists_log_to_db(self, db, orchestrator):
        orchestrator.trigger_outbound(_order(), "address_enrichment")
        rows = db.execute("SELECT * FROM communication_logs").fetchall()
        assert len(rows) == 1
        row = dict(rows[0])
        assert row["order_id"] == "ORD001"
        assert row["channel"] == "whatsapp"
        assert row["status"] == "sent"

    def test_limit_blocks_second_wa(self, orchestrator):
        orchestrator.trigger_outbound(_order(), "address_enrichment")
        result = orchestrator.trigger_outbound(_order(), "address_enrichment")
        assert result["status"] == "failed"
        assert result.get("error_message") is not None

    def test_permission_denied(self, db):
        # Remove permission
        db.execute("DELETE FROM merchant_permissions WHERE merchant_id = 'M001'")
        db.commit()
        orch = OutboundOrchestrator(
            db=db,
            whatsapp_client=_mock_wa_client(),
            voice_ai_client=_mock_voice_client(),
            issue_router=_mock_router(),
        )
        result = orch.trigger_outbound(_order(), "address_enrichment")
        assert result["status"] == "failed"
        assert result.get("error_message") is not None

    def test_wa_send_failure(self, db):
        orch = OutboundOrchestrator(
            db=db,
            whatsapp_client=_mock_wa_client(send_status="failed"),
            voice_ai_client=_mock_voice_client(),
            issue_router=_mock_router(),
        )
        result = orch.trigger_outbound(_order(), "address_enrichment")
        assert result["status"] == "failed"


class TestCheckAndEscalate:
    def test_escalate_to_voice_when_no_wa_response(self, db):
        wa = _mock_wa_client(responded=False)
        voice = _mock_voice_client(status="completed", resolution="address_updated")
        orch = OutboundOrchestrator(
            db=db, whatsapp_client=wa, voice_ai_client=voice,
            issue_router=_mock_router(),
        )
        # First trigger WA
        log = orch.trigger_outbound(_order(), "address_enrichment")
        comm_id = log["communication_id"]
        # Escalate
        result = orch.check_and_escalate(comm_id)
        assert result["channel"] == "voice"
        assert result["status"] == "call_completed"
        assert result["resolution"] == "address_updated"

    def test_no_escalation_when_wa_responded(self, db):
        wa = _mock_wa_client(responded=True)
        orch = OutboundOrchestrator(
            db=db, whatsapp_client=wa, voice_ai_client=_mock_voice_client(),
            issue_router=_mock_router(),
        )
        log = orch.trigger_outbound(_order(), "address_enrichment")
        result = orch.check_and_escalate(log["communication_id"])
        assert result["status"] == "responded"
        assert result["resolution"] is not None

    def test_nonexistent_log_returns_error(self, orchestrator):
        result = orchestrator.check_and_escalate("nonexistent_id")
        assert result["status"] == "failed"

    def test_voice_limit_prevents_second_call(self, db):
        wa = _mock_wa_client(responded=False)
        voice = _mock_voice_client()
        orch = OutboundOrchestrator(
            db=db, whatsapp_client=wa, voice_ai_client=voice,
            issue_router=_mock_router(),
        )
        log = orch.trigger_outbound(_order(), "address_enrichment")
        orch.check_and_escalate(log["communication_id"])
        # Insert another WA log manually to test voice limit
        db.execute(
            """INSERT INTO communication_logs
            (communication_id, order_id, merchant_id, customer_ucid,
             issue_type, channel, status, sent_at)
            VALUES ('comm_second', 'ORD001', 'M001', 'CUST001',
                    'address_enrichment', 'whatsapp', 'sent', '2024-01-01T14:00:00')"""
        )
        db.commit()
        result = orch.check_and_escalate("comm_second")
        # Voice limit reached — should get no_response
        assert result["status"] == "no_response"


class TestCommunicationStatus:
    def test_returns_logs_for_order(self, orchestrator):
        orchestrator.trigger_outbound(_order(), "address_enrichment")
        logs = orchestrator.get_communication_status("ORD001")
        assert len(logs) >= 1
        assert logs[0]["order_id"] == "ORD001"

    def test_empty_for_unknown_order(self, orchestrator):
        logs = orchestrator.get_communication_status("UNKNOWN")
        assert logs == []


class TestCheckCommunicationLimits:
    def test_allows_first_message(self, orchestrator):
        assert orchestrator.check_communication_limits("ORD001", "address_enrichment") is True

    def test_blocks_after_first_wa(self, orchestrator):
        orchestrator.trigger_outbound(_order(), "address_enrichment")
        assert orchestrator.check_communication_limits("ORD001", "address_enrichment") is False

    def test_different_issue_type_allowed(self, orchestrator):
        orchestrator.trigger_outbound(_order(), "address_enrichment")
        assert orchestrator.check_communication_limits("ORD001", "cod_to_prepaid") is True


class TestFallback:
    def test_fallback_marks_no_response(self, orchestrator):
        orchestrator.trigger_outbound(_order(), "address_enrichment")
        orchestrator.fallback_to_next_intervention("ORD001")
        logs = orchestrator.get_communication_status("ORD001")
        assert any(l["status"] == "no_response" for l in logs)
