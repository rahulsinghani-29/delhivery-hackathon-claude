"""Unit tests for communication/issue_router.py — CommunicationIssueRouter."""

from __future__ import annotations

import pytest

from communication.issue_router import CommunicationIssueRouter


@pytest.fixture
def router():
    return CommunicationIssueRouter(
        address_quality_threshold=0.5,
        cluster_rto_threshold=0.3,
    )


def _order(payment_mode="COD", address_quality=0.8, **kwargs):
    base = {
        "order_id": "ORD001",
        "merchant_id": "M001",
        "customer_ucid": "CUST001",
        "payment_mode": payment_mode,
        "address_quality": address_quality,
        "destination_pincode": "110001",
        "customer_name": "Rajesh Sharma",
        "merchant_name": "Amazon",
        "order_value": "3000",
    }
    base.update(kwargs)
    return base


class TestRoute:
    def test_prepaid_order_returns_none(self, router):
        result = router.route(_order(payment_mode="prepaid"), cluster_rto_rate=0.5)
        assert result is None

    def test_cod_low_address_quality_returns_address_enrichment(self, router):
        result = router.route(_order(address_quality=0.3), cluster_rto_rate=0.1)
        assert result == "address_enrichment"

    def test_cod_high_cluster_rto_returns_cod_to_prepaid(self, router):
        result = router.route(_order(address_quality=0.8), cluster_rto_rate=0.5)
        assert result == "cod_to_prepaid"

    def test_cod_good_address_low_rto_returns_none(self, router):
        result = router.route(_order(address_quality=0.8), cluster_rto_rate=0.1)
        assert result is None

    def test_address_enrichment_takes_precedence(self, router):
        """When both conditions are true, address_enrichment wins."""
        result = router.route(_order(address_quality=0.3), cluster_rto_rate=0.5)
        assert result == "address_enrichment"

    def test_boundary_address_quality_at_threshold(self, router):
        """address_quality exactly at threshold should NOT trigger (must be strictly less)."""
        result = router.route(_order(address_quality=0.5), cluster_rto_rate=0.1)
        assert result is None

    def test_boundary_address_quality_just_below(self, router):
        result = router.route(_order(address_quality=0.499), cluster_rto_rate=0.1)
        assert result == "address_enrichment"

    def test_boundary_cluster_rto_at_threshold(self, router):
        """cluster_rto_rate exactly at threshold should NOT trigger (must be strictly greater)."""
        result = router.route(_order(address_quality=0.8), cluster_rto_rate=0.3)
        assert result is None

    def test_boundary_cluster_rto_just_above(self, router):
        result = router.route(_order(address_quality=0.8), cluster_rto_rate=0.301)
        assert result == "cod_to_prepaid"

    def test_missing_payment_mode_returns_none(self, router):
        order = {"order_id": "X", "address_quality": 0.1}
        result = router.route(order, cluster_rto_rate=0.5)
        assert result is None


class TestGetTemplateFields:
    def test_address_enrichment_fields(self, router):
        order = _order(current_address="Central Park 2, Sohna Road")
        fields = router.get_template_fields(order, "address_enrichment")
        assert fields["order_id"] == "ORD001"
        assert fields["customer_name"] == "Rajesh Sharma"
        assert fields["merchant_name"] == "Amazon"
        assert "current_address" in fields

    def test_cod_to_prepaid_fields(self, router):
        fields = router.get_template_fields(_order(), "cod_to_prepaid")
        assert fields["order_id"] == "ORD001"
        assert fields["customer_name"] == "Rajesh Sharma"
        assert fields["merchant_name"] == "Amazon"
        assert "order_value" in fields
        assert "payment_link_url" in fields

    def test_unknown_issue_type_returns_empty(self, router):
        fields = router.get_template_fields(_order(), "unknown_type")
        assert fields == {}
