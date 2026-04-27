"""Unit tests for services/action_executor.py — ActionExecutorService."""

from __future__ import annotations

import pytest

from data.db import init_db, get_db
from services.action_executor import ActionExecutorService


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
                   '110001', 'north', 0.8, 0.7, 'pending', '2024-01-01T10:00:00')"""
    )
    conn.execute(
        """INSERT INTO merchant_permissions (merchant_id, intervention_type, is_enabled, daily_cap, hourly_cap)
           VALUES ('M001', 'verification', 1, 500, 100)"""
    )
    conn.commit()
    yield conn
    conn.close()


@pytest.fixture
def service(db):
    return ActionExecutorService(db)


class TestExecute:
    def test_successful_execution(self, service):
        result = service.execute("M001", "ORD001", "verification", 0.8)
        assert result["success"] is True
        assert result["intervention_log_id"] != ""
        assert result["error_message"] is None

    def test_permission_denied(self, service):
        result = service.execute("M001", "ORD001", "cancellation", 0.8)
        assert result["success"] is False
        assert "Permission denied" in result["error_message"]

    def test_rate_limit_exceeded(self, db, service):
        # Set very low cap and fill it up
        db.execute(
            "UPDATE merchant_permissions SET daily_cap = 1, hourly_cap = 1 WHERE merchant_id = 'M001'"
        )
        # Insert an intervention to exhaust the cap
        from datetime import datetime
        db.execute(
            """INSERT INTO interventions
               (intervention_id, order_id, merchant_id, intervention_type,
                action_owner, initiated_by, executed_at)
               VALUES ('existing_int', 'ORD001', 'M001', 'verification', 'delhivery', 'system', ?)""",
            (datetime.utcnow().isoformat(),),
        )
        db.commit()
        result = service.execute("M001", "ORD001", "verification", 0.8)
        assert result["success"] is False
        assert "Rate limit" in result["error_message"]

    def test_logs_intervention(self, db, service):
        service.execute("M001", "ORD001", "verification", 0.8)
        rows = db.execute(
            "SELECT * FROM interventions WHERE intervention_type = 'verification'"
        ).fetchall()
        assert len(rows) >= 1


class TestCategorizeAction:
    def test_delhivery_actions(self, service):
        delhivery_types = [
            "verification", "cancellation", "masked_calling", "premium_courier",
            "address_enrichment_outreach", "cod_to_prepaid_outreach",
            "auto_cancel", "express_upgrade",
        ]
        for t in delhivery_types:
            assert service.categorize_action(t) == "delhivery", f"{t} should be delhivery"

    def test_merchant_actions(self, service):
        assert service.categorize_action("merchant_confirmation") == "merchant"

    def test_unknown_action(self, service):
        assert service.categorize_action("totally_unknown") == "merchant"


class TestRetryFailed:
    def test_retry_existing_intervention(self, db, service):
        # First execute to create a log entry
        result = service.execute("M001", "ORD001", "verification", 0.8)
        log_id = result["intervention_log_id"]

        retry_result = service.retry_failed(log_id)
        assert retry_result["success"] is True

    def test_retry_nonexistent(self, service):
        result = service.retry_failed("nonexistent_id")
        assert result["success"] is False
        assert "not found" in result["error_message"]
