"""Unit tests for data/db.py — SQLite connection manager and schema."""

import os
import sqlite3
import tempfile

import pytest

from data.db import close_db, get_db, init_db

EXPECTED_TABLES = [
    "merchants",
    "warehouse_nodes",
    "orders",
    "interventions",
    "communication_logs",
    "merchant_permissions",
    "suppressed_recommendations",
]

EXPECTED_INDEXES = [
    "idx_orders_merchant_id",
    "idx_orders_customer_ucid",
    "idx_orders_destination_cluster",
    "idx_interventions_order_id",
    "idx_interventions_merchant_id",
    "idx_communication_logs_order_id",
]


@pytest.fixture()
def db_path(tmp_path):
    return str(tmp_path / "test.db")


class TestGetDb:
    def test_returns_connection(self, db_path):
        conn = get_db(db_path)
        assert isinstance(conn, sqlite3.Connection)
        conn.close()

    def test_wal_mode_enabled(self, db_path):
        conn = get_db(db_path)
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode == "wal"
        conn.close()

    def test_row_factory_set(self, db_path):
        conn = get_db(db_path)
        assert conn.row_factory is sqlite3.Row
        conn.close()

    def test_foreign_keys_enabled(self, db_path):
        conn = get_db(db_path)
        fk = conn.execute("PRAGMA foreign_keys").fetchone()[0]
        assert fk == 1
        conn.close()


class TestInitDb:
    def test_creates_all_tables(self, db_path):
        init_db(db_path)
        conn = get_db(db_path)
        tables = [
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        ]
        conn.close()
        for table in EXPECTED_TABLES:
            assert table in tables, f"Missing table: {table}"

    def test_creates_all_indexes(self, db_path):
        init_db(db_path)
        conn = get_db(db_path)
        indexes = [
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        ]
        conn.close()
        for idx in EXPECTED_INDEXES:
            assert idx in indexes, f"Missing index: {idx}"

    def test_idempotent(self, db_path):
        init_db(db_path)
        init_db(db_path)  # should not raise
        conn = get_db(db_path)
        tables = [
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        ]
        conn.close()
        assert len(tables) == len(EXPECTED_TABLES)

    def test_orders_table_has_shipping_mode_default(self, db_path):
        init_db(db_path)
        conn = get_db(db_path)
        # Insert a merchant and warehouse node first (FK constraints)
        conn.execute("INSERT INTO merchants (merchant_id, name) VALUES ('m1', 'Test')")
        conn.execute(
            "INSERT INTO warehouse_nodes (node_id, merchant_id, city, state, pincode) "
            "VALUES ('n1', 'm1', 'Delhi', 'DL', '110001')"
        )
        conn.execute(
            "INSERT INTO orders (order_id, merchant_id, customer_ucid, category, price_band, "
            "payment_mode, origin_node, destination_pincode, destination_cluster, "
            "address_quality, rto_score, delivery_outcome, created_at) "
            "VALUES ('o1', 'm1', 'c1', 'fashion', 'mid', 'COD', 'n1', '400001', 'west', "
            "0.8, 0.3, 'delivered', '2024-01-01')"
        )
        conn.commit()
        row = conn.execute("SELECT shipping_mode FROM orders WHERE order_id='o1'").fetchone()
        assert row["shipping_mode"] == "surface"
        conn.close()

    def test_merchant_permissions_composite_pk(self, db_path):
        init_db(db_path)
        conn = get_db(db_path)
        conn.execute("INSERT INTO merchants (merchant_id, name) VALUES ('m1', 'Test')")
        conn.execute(
            "INSERT INTO merchant_permissions (merchant_id, intervention_type) "
            "VALUES ('m1', 'verification')"
        )
        conn.execute(
            "INSERT INTO merchant_permissions (merchant_id, intervention_type) "
            "VALUES ('m1', 'cancellation')"
        )
        conn.commit()
        rows = conn.execute("SELECT * FROM merchant_permissions WHERE merchant_id='m1'").fetchall()
        assert len(rows) == 2
        conn.close()


class TestCloseDb:
    def test_closes_connection(self, db_path):
        conn = get_db(db_path)
        close_db(conn)
        with pytest.raises(sqlite3.ProgrammingError):
            conn.execute("SELECT 1")
