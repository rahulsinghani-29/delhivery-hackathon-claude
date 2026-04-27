"""Mock WhatsApp Business API client for hackathon."""

from __future__ import annotations

import random
import uuid
from datetime import datetime


class WhatsAppClient:
    """Mock WhatsApp Business API client.

    For hackathon: no real API calls — returns mock success responses.
    Templates are pre-defined per issue type with dynamic field population.
    """

    TEMPLATES = {
        "address_enrichment": (
            "Hi {customer_name}, this is Delhivery. We're preparing your order "
            "{order_id} from {merchant_name}. We need to confirm your delivery "
            "address: {current_address}. Please reply with your complete address "
            "including flat/house number, floor, tower, and landmark."
        ),
        "cod_to_prepaid": (
            "Hi {customer_name}, this is Delhivery. Your COD order {order_id} "
            "from {merchant_name} worth \u20b9{order_value} is being processed. "
            "Pay now and get 5% off! Click here to pay: {payment_link_url}"
        ),
    }

    def __init__(
        self,
        api_base_url: str = "http://localhost:8080",
        api_token: str = "mock",
    ) -> None:
        self.api_base_url = api_base_url
        self.api_token = api_token

    def send_template_message(
        self,
        customer_ucid: str,
        issue_type: str,
        template_fields: dict,
    ) -> dict:
        """Send a templated WhatsApp message (mock).

        Selects the pre-defined template for *issue_type*, populates it with
        *template_fields*, and returns a WhatsAppSendResult-like dict:
            { message_id, status, error_message }
        """
        template = self.TEMPLATES.get(issue_type)
        if template is None:
            return {
                "message_id": "",
                "status": "failed",
                "error_message": f"Unknown issue type: {issue_type}",
            }

        # Validate that all required fields are present
        try:
            template.format(**template_fields)
        except KeyError as exc:
            return {
                "message_id": "",
                "status": "failed",
                "error_message": f"Missing template field: {exc}",
            }

        message_id = f"wa_{uuid.uuid4().hex[:12]}"
        return {
            "message_id": message_id,
            "status": "sent",
            "error_message": None,
        }

    def check_response(self, message_id: str) -> dict:
        """Check if customer responded (mock — random outcome).

        Returns a WhatsAppResponseStatus-like dict:
            { responded, response_content, responded_at }
        """
        responded = random.random() < 0.4  # 40% chance of response in mock
        if responded:
            return {
                "responded": True,
                "response_content": "Yes, address is correct",
                "responded_at": datetime.utcnow().isoformat(),
            }
        return {
            "responded": False,
            "response_content": None,
            "responded_at": None,
        }
