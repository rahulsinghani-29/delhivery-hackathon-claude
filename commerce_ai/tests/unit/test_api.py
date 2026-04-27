"""Unit tests for the FastAPI API layer (Tasks 6.1–6.4)."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# Ensure commerce_ai root is importable
_pkg_root = str(Path(__file__).resolve().parent.parent.parent)
if _pkg_root not in sys.path:
    sys.path.insert(0, _pkg_root)

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.routes import router


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_db() -> sqlite3.Connection:
    """Create an in-memory SQLite DB with schema and seed data."""
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.execute("PRAGMA foreign_keys=OFF")  # simplify test seeding
    conn.row_factory = sqlite3.Row

    # Schema
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS merchants (
            merchant_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS warehouse_nodes (
            node_id TEXT PRIMARY KEY,
            merchant_id TEXT NOT NULL,
            city TEXT NOT NULL,
            state TEXT NOT NULL,
            pincode TEXT NOT NULL,
            is_active BOOLEAN DEFAULT TRUE
        );
        CREATE TABLE IF NOT EXISTS orders (
            order_id TEXT PRIMARY KEY,
            merchant_id TEXT NOT NULL,
            customer_ucid TEXT NOT NULL,
            category TEXT NOT NULL,
            price_band TEXT NOT NULL,
            payment_mode TEXT NOT NULL,
            origin_node TEXT NOT NULL,
            destination_pincode TEXT NOT NULL,
            destination_cluster TEXT NOT NULL,
            address_quality REAL NOT NULL,
            rto_score REAL NOT NULL,
            delivery_outcome TEXT NOT NULL,
            shipping_mode TEXT NOT NULL DEFAULT 'surface',
            created_at TIMESTAMP NOT NULL
        );
        CREATE TABLE IF NOT EXISTS interventions (
            intervention_id TEXT PRIMARY KEY,
            order_id TEXT NOT NULL,
            merchant_id TEXT NOT NULL,
            intervention_type TEXT NOT NULL,
            action_owner TEXT NOT NULL,
            initiated_by TEXT NOT NULL,
            confidence_score REAL,
            outcome TEXT,
            executed_at TIMESTAMP NOT NULL,
            completed_at TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS communication_logs (
            communication_id TEXT PRIMARY KEY,
            order_id TEXT NOT NULL,
            merchant_id TEXT NOT NULL,
            customer_ucid TEXT NOT NULL,
            issue_type TEXT NOT NULL,
            channel TEXT NOT NULL,
            template_id TEXT,
            message_id TEXT,
            status TEXT NOT NULL,
            customer_response TEXT,
            resolution TEXT,
            sent_at TIMESTAMP NOT NULL,
            responded_at TIMESTAMP,
            escalation_scheduled_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS merchant_permissions (
            merchant_id TEXT NOT NULL,
            intervention_type TEXT NOT NULL,
            is_enabled BOOLEAN DEFAULT FALSE,
            daily_cap INTEGER DEFAULT 500,
            hourly_cap INTEGER DEFAULT 100,
            auto_cancel_enabled BOOLEAN DEFAULT FALSE,
            auto_cancel_threshold REAL DEFAULT 0.9,
            express_upgrade_enabled BOOLEAN DEFAULT FALSE,
            impulse_categories TEXT DEFAULT 'fashion,beauty',
            PRIMARY KEY (merchant_id, intervention_type)
        );
        CREATE TABLE IF NOT EXISTS suppressed_recommendations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            merchant_id TEXT NOT NULL,
            recommendation_type TEXT NOT NULL,
            recommendation_data TEXT NOT NULL,
            suppression_reason TEXT NOT NULL,
            suppressed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # Seed data
    conn.execute("INSERT INTO merchants VALUES ('m1', 'Test Merchant', '2024-01-01')")
    conn.execute(
        "INSERT INTO warehouse_nodes VALUES ('wh1', 'm1', 'Delhi', 'DL', '110001', 1)"
    )
    conn.execute(
        "INSERT INTO orders VALUES "
        "('ord1', 'm1', 'cust1', 'electronics', '1000-2000', 'COD', 'wh1', "
        "'110001', 'north_delhi', 0.3, 0.75, 'pending', 'surface', '2024-06-01T10:00:00')"
    )
    conn.execute(
        "INSERT INTO merchant_permissions VALUES "
        "('m1', 'verification', 1, 500, 100, 0, 0.9, 0, 'fashion,beauty')"
    )
    conn.commit()
    return conn


@pytest.fixture
def client():
    """Create a test client with mocked services."""
    app = FastAPI()
    app.include_router(router)

    db = _make_db()

    # Mock services
    order_engine = MagicMock()
    order_engine.get_live_feed.return_value = [
        {"order": {"order_id": "ord1", "rto_score": 0.75}, "risk_tag": {"tag_label": "High RTO"}}
    ]

    demand_advisor = MagicMock()
    demand_advisor.get_suggestions.return_value = [
        {"cohort_dimension": "electronics/1000-2000", "expected_score_improvement": 0.05}
    ]

    action_executor = MagicMock()
    action_executor.execute.return_value = {
        "success": True,
        "intervention_log_id": "int_123",
        "error_message": None,
    }

    guardrails = MagicMock()
    guardrails.check_permission.return_value = True
    guardrails.check_rate_limit.return_value = True

    outbound_orchestrator = MagicMock()
    outbound_orchestrator.get_communication_status.return_value = []
    outbound_orchestrator.trigger_outbound.return_value = {
        "communication_id": "comm_123",
        "status": "sent",
        "error_message": None,
    }

    issue_router = MagicMock()

    app.state.db = db
    app.state.order_engine = order_engine
    app.state.demand_advisor = demand_advisor
    app.state.action_executor = action_executor
    app.state.guardrails = guardrails
    app.state.outbound_orchestrator = outbound_orchestrator
    app.state.issue_router = issue_router

    return TestClient(app)


# ---------------------------------------------------------------------------
# Task 6.1 — App setup tests
# ---------------------------------------------------------------------------

class TestAppSetup:
    def test_health_endpoint_exists(self, client):
        """The app should respond to requests (basic connectivity)."""
        # We test via the routes — health is on the main app, not the router.
        # Just verify the router-based endpoints work.
        resp = client.get("/merchants/m1/snapshot")
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Task 6.2 — Core endpoint tests
# ---------------------------------------------------------------------------

class TestMerchantSnapshot:
    def test_returns_snapshot(self, client):
        resp = client.get("/merchants/m1/snapshot")
        assert resp.status_code == 200
        data = resp.json()
        assert data["merchant_id"] == "m1"

    def test_unknown_merchant_404(self, client):
        resp = client.get("/merchants/nonexistent/snapshot")
        assert resp.status_code == 404


class TestDemandSuggestions:
    def test_returns_suggestions(self, client):
        resp = client.get("/merchants/m1/demand-suggestions")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)

    def test_unknown_merchant_404(self, client):
        resp = client.get("/merchants/nonexistent/demand-suggestions")
        assert resp.status_code == 404


class TestLiveOrders:
    def test_returns_feed(self, client):
        resp = client.get("/merchants/m1/orders/live")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) == 1

    def test_unknown_merchant_404(self, client):
        resp = client.get("/merchants/nonexistent/orders/live")
        assert resp.status_code == 404


class TestExecuteAction:
    def test_successful_execution(self, client):
        resp = client.post(
            "/merchants/m1/actions/execute",
            json={"order_id": "ord1", "intervention_type": "verification"},
        )
        assert resp.status_code == 200
        assert resp.json()["success"] is True

    def test_permission_denied_403(self, client):
        client.app.state.guardrails.check_permission.return_value = False
        resp = client.post(
            "/merchants/m1/actions/execute",
            json={"order_id": "ord1", "intervention_type": "cancellation"},
        )
        assert resp.status_code == 403

    def test_rate_limit_429(self, client):
        client.app.state.guardrails.check_permission.return_value = True
        client.app.state.guardrails.check_rate_limit.return_value = False
        resp = client.post(
            "/merchants/m1/actions/execute",
            json={"order_id": "ord1", "intervention_type": "verification"},
        )
        assert resp.status_code == 429

    def test_unknown_merchant_404(self, client):
        resp = client.post(
            "/merchants/nonexistent/actions/execute",
            json={"order_id": "ord1", "intervention_type": "verification"},
        )
        assert resp.status_code == 404


class TestActionLog:
    def test_returns_log(self, client):
        resp = client.get("/merchants/m1/actions/log")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)


class TestDashboard:
    def test_returns_dashboard(self, client):
        resp = client.get("/merchants/m1/dashboard")
        assert resp.status_code == 200
        data = resp.json()
        assert data["merchant_id"] == "m1"
        assert "intervention_counts" in data


class TestPermissions:
    def test_get_permissions(self, client):
        resp = client.get("/merchants/m1/permissions")
        assert resp.status_code == 200
        data = resp.json()
        assert data["merchant_id"] == "m1"

    def test_update_permissions(self, client):
        resp = client.put(
            "/merchants/m1/permissions",
            json={"intervention_type": "cancellation", "is_enabled": True},
        )
        assert resp.status_code == 200

    def test_update_existing_permission(self, client):
        resp = client.put(
            "/merchants/m1/permissions",
            json={"intervention_type": "verification", "is_enabled": False},
        )
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Task 6.3 — Communication endpoint tests
# ---------------------------------------------------------------------------

class TestCommunicationEndpoints:
    def test_merchant_communications_status(self, client):
        resp = client.get("/merchants/m1/communications/status")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_order_communications(self, client):
        resp = client.get("/orders/ord1/communications")
        assert resp.status_code == 200

    def test_order_communications_unknown_order_404(self, client):
        resp = client.get("/orders/nonexistent/communications")
        assert resp.status_code == 404

    def test_trigger_communication(self, client):
        resp = client.post(
            "/orders/ord1/communications/trigger",
            json={"issue_type": "address_enrichment"},
        )
        assert resp.status_code == 200

    def test_trigger_communication_unknown_order_404(self, client):
        resp = client.post(
            "/orders/nonexistent/communications/trigger",
            json={"issue_type": "address_enrichment"},
        )
        assert resp.status_code == 404

    def test_trigger_communication_invalid_issue_type_422(self, client):
        resp = client.post(
            "/orders/ord1/communications/trigger",
            json={"issue_type": "invalid_type"},
        )
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Task 6.4 — Error handling tests
# ---------------------------------------------------------------------------

class TestErrorHandling:
    def test_404_for_unknown_merchant(self, client):
        resp = client.get("/merchants/unknown_id/snapshot")
        assert resp.status_code == 404

    def test_403_for_permission_denied(self, client):
        client.app.state.guardrails.check_permission.return_value = False
        resp = client.post(
            "/merchants/m1/actions/execute",
            json={"order_id": "ord1", "intervention_type": "verification"},
        )
        assert resp.status_code == 403

    def test_429_for_rate_limit(self, client):
        client.app.state.guardrails.check_permission.return_value = True
        client.app.state.guardrails.check_rate_limit.return_value = False
        resp = client.post(
            "/merchants/m1/actions/execute",
            json={"order_id": "ord1", "intervention_type": "verification"},
        )
        assert resp.status_code == 429

    def test_auto_cancel_threshold_validation(self, client):
        """auto_cancel_threshold must be > risk_threshold (0.4)."""
        resp = client.put(
            "/merchants/m1/permissions",
            json={
                "intervention_type": "verification",
                "is_enabled": True,
                "auto_cancel_threshold": 0.3,  # below risk threshold
            },
        )
        assert resp.status_code == 422

    def test_auto_cancel_threshold_at_boundary(self, client):
        """auto_cancel_threshold == risk_threshold should also be rejected."""
        resp = client.put(
            "/merchants/m1/permissions",
            json={
                "intervention_type": "verification",
                "is_enabled": True,
                "auto_cancel_threshold": 0.4,  # exactly at risk threshold
            },
        )
        assert resp.status_code == 422

    def test_auto_cancel_threshold_valid(self, client):
        """auto_cancel_threshold > risk_threshold should succeed."""
        resp = client.put(
            "/merchants/m1/permissions",
            json={
                "intervention_type": "verification",
                "is_enabled": True,
                "auto_cancel_threshold": 0.8,
            },
        )
        assert resp.status_code == 200

    def test_422_for_missing_required_field(self, client):
        """FastAPI returns 422 for missing required fields."""
        resp = client.post(
            "/merchants/m1/actions/execute",
            json={"order_id": "ord1"},  # missing intervention_type
        )
        assert resp.status_code == 422
