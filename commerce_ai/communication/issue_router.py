"""Deterministic communication issue routing — no AI needed."""

from __future__ import annotations


class CommunicationIssueRouter:
    """Routes COD orders to the appropriate communication issue type.

    Routing logic (deterministic):
    1. Only COD orders are eligible (prepaid → None)
    2. IF address_quality < threshold → address_enrichment (higher priority)
    3. ELIF cluster COD-RTO rate > cluster_threshold → cod_to_prepaid
    4. ELSE → None
    Address enrichment takes precedence when both conditions are true.
    """

    def __init__(
        self,
        address_quality_threshold: float = 0.5,
        cluster_rto_threshold: float = 0.3,
    ) -> None:
        self.address_quality_threshold = address_quality_threshold
        self.cluster_rto_threshold = cluster_rto_threshold

    def route(self, order: dict, cluster_rto_rate: float) -> str | None:
        """Return the issue type for outbound communication, or None.

        Deterministic routing:
        1. Only COD orders eligible (prepaid → None)
        2. IF address_quality < threshold → "address_enrichment" (higher priority)
        3. ELIF cluster_rto_rate > cluster_threshold → "cod_to_prepaid"
        4. ELSE → None
        """
        payment_mode = order.get("payment_mode", "")
        if payment_mode != "COD":
            return None

        address_quality = order.get("address_quality", 1.0)
        if address_quality < self.address_quality_threshold:
            return "address_enrichment"

        if cluster_rto_rate > self.cluster_rto_threshold:
            return "cod_to_prepaid"

        return None

    def get_template_fields(self, order: dict, issue_type: str) -> dict:
        """Return dynamic fields for the WhatsApp template based on issue type.

        For address_enrichment:
            { order_id, customer_name, current_address, merchant_name }
        For cod_to_prepaid:
            { order_id, customer_name, order_value, payment_link_url, merchant_name }
        """
        base = {
            "order_id": order.get("order_id", ""),
            "customer_name": order.get("customer_name", "Customer"),
            "merchant_name": order.get("merchant_name", "your seller"),
        }

        if issue_type == "address_enrichment":
            base["current_address"] = order.get(
                "current_address",
                order.get("destination_pincode", ""),
            )
            return base

        if issue_type == "cod_to_prepaid":
            base["order_value"] = order.get("order_value", "0")
            base["payment_link_url"] = order.get(
                "payment_link_url",
                f"https://pay.delhivery.com/{order.get('order_id', '')}",
            )
            return base

        return {}
