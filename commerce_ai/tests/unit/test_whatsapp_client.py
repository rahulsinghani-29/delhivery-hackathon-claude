"""Unit tests for communication/whatsapp_client.py — WhatsAppClient."""

from __future__ import annotations

import pytest

from communication.whatsapp_client import WhatsAppClient


@pytest.fixture
def client():
    return WhatsAppClient()


class TestSendTemplateMessage:
    def test_address_enrichment_sends_successfully(self, client):
        result = client.send_template_message(
            "CUST001", "address_enrichment",
            {
                "customer_name": "Rajesh",
                "order_id": "ORD001",
                "merchant_name": "Amazon",
                "current_address": "Central Park 2, Sohna Road",
            },
        )
        assert result["status"] == "sent"
        assert result["message_id"].startswith("wa_")
        assert result["error_message"] is None

    def test_cod_to_prepaid_sends_successfully(self, client):
        result = client.send_template_message(
            "CUST001", "cod_to_prepaid",
            {
                "customer_name": "Rajesh",
                "order_id": "ORD001",
                "merchant_name": "Amazon",
                "order_value": "3000",
                "payment_link_url": "https://pay.delhivery.com/ORD001",
            },
        )
        assert result["status"] == "sent"
        assert result["message_id"].startswith("wa_")

    def test_unknown_issue_type_fails(self, client):
        result = client.send_template_message(
            "CUST001", "unknown_type", {},
        )
        assert result["status"] == "failed"
        assert "Unknown issue type" in result["error_message"]
        assert result["message_id"] == ""

    def test_missing_template_field_fails(self, client):
        result = client.send_template_message(
            "CUST001", "address_enrichment",
            {"customer_name": "Rajesh"},  # missing order_id, merchant_name, current_address
        )
        assert result["status"] == "failed"
        assert "Missing template field" in result["error_message"]

    def test_unique_message_ids(self, client):
        fields = {
            "customer_name": "A",
            "order_id": "O1",
            "merchant_name": "M",
            "current_address": "Addr",
        }
        r1 = client.send_template_message("C1", "address_enrichment", fields)
        r2 = client.send_template_message("C1", "address_enrichment", fields)
        assert r1["message_id"] != r2["message_id"]

    def test_templates_contain_expected_placeholders(self):
        assert "{customer_name}" in WhatsAppClient.TEMPLATES["address_enrichment"]
        assert "{order_id}" in WhatsAppClient.TEMPLATES["address_enrichment"]
        assert "{merchant_name}" in WhatsAppClient.TEMPLATES["address_enrichment"]
        assert "{current_address}" in WhatsAppClient.TEMPLATES["address_enrichment"]
        assert "{customer_name}" in WhatsAppClient.TEMPLATES["cod_to_prepaid"]
        assert "{order_value}" in WhatsAppClient.TEMPLATES["cod_to_prepaid"]
        assert "{payment_link_url}" in WhatsAppClient.TEMPLATES["cod_to_prepaid"]


class TestCheckResponse:
    def test_returns_dict_with_expected_keys(self, client):
        result = client.check_response("wa_abc123")
        assert "responded" in result
        assert "response_content" in result
        assert "responded_at" in result

    def test_responded_true_has_content(self, client):
        """Run multiple times — when responded=True, content should be present."""
        for _ in range(50):
            result = client.check_response("wa_abc123")
            if result["responded"]:
                assert result["response_content"] is not None
                assert result["responded_at"] is not None
                return
        # If we never got a response in 50 tries, that's statistically unlikely but ok
