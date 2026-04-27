"""Unit tests for data/load_data.py — CSV/JSON loading with malformed row handling."""

import json
import os
import sqlite3
import tempfile

import pytest

from data.db import init_db
from data.load_data import (
    LoadResult,
    load_all,
    load_interventions,
    load_merchants,
    load_orders,
    load_warehouse_nodes,
)


@pytest.fixture()
def db_conn(tmp_path):
    """Provide an initialised in-memory-like SQLite connection."""
    db_path = str(tmp_path / "test.db")
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


def _write_csv(tmp_path, name: str, header: str, rows: list[str]) -> str:
    p = tmp_path / name
    p.write_text(header + "\n" + "\n".join(rows) + "\n", encoding="utf-8")
    return str(p)


def _write_json(tmp_path, name: str, data: list[dict]) -> str:
    p = tmp_path / name
    p.write_text(json.dumps(data), encoding="utf-8")
    return str(p)


# ── Merchants ──────────────────────────────────────────────────────────────


class TestLoadMerchants:
    def test_valid_csv(self, db_conn, tmp_path):
        path = _write_csv(
            tmp_path,
            "merchants.csv",
            "merchant_id,name",
            ["m1,Acme Corp", "m2,Beta Inc"],
        )
        res = load_merchants(db_conn, path)
        assert res.loaded == 2
        assert res.skipped == 0
        assert res.errors == []

    def test_missing_name_skips_row(self, db_conn, tmp_path):
        path = _write_csv(
            tmp_path,
            "merchants.csv",
            "merchant_id,name",
            ["m1,Acme Corp", "m2,"],
        )
        res = load_merchants(db_conn, path)
        assert res.loaded == 1
        assert res.skipped == 1
        assert any("missing field 'name'" in e for e in res.errors)

    def test_json_format(self, db_conn, tmp_path):
        path = _write_json(
            tmp_path,
            "merchants.json",
            [{"merchant_id": "m1", "name": "Acme"}],
        )
        res = load_merchants(db_conn, path)
        assert res.loaded == 1
        assert res.skipped == 0


# ── Warehouse Nodes ────────────────────────────────────────────────────────


class TestLoadWarehouseNodes:
    def test_valid_csv(self, db_conn, tmp_path):
        # Need a merchant first
        db_conn.execute("INSERT INTO merchants VALUES ('m1', 'Acme', CURRENT_TIMESTAMP)")
        db_conn.commit()

        path = _write_csv(
            tmp_path,
            "warehouse_nodes.csv",
            "node_id,merchant_id,city,state,pincode,is_active",
            ["n1,m1,Mumbai,MH,400001,true"],
        )
        res = load_warehouse_nodes(db_conn, path)
        assert res.loaded == 1
        assert res.skipped == 0

    def test_missing_city_skips(self, db_conn, tmp_path):
        db_conn.execute("INSERT INTO merchants VALUES ('m1', 'Acme', CURRENT_TIMESTAMP)")
        db_conn.commit()

        path = _write_csv(
            tmp_path,
            "warehouse_nodes.csv",
            "node_id,merchant_id,city,state,pincode",
            ["n1,m1,,MH,400001"],
        )
        res = load_warehouse_nodes(db_conn, path)
        assert res.loaded == 0
        assert res.skipped == 1


# ── Orders ─────────────────────────────────────────────────────────────────


def _seed_merchant_and_node(db_conn):
    db_conn.execute("INSERT OR IGNORE INTO merchants VALUES ('m1', 'Acme', CURRENT_TIMESTAMP)")
    db_conn.execute(
        "INSERT OR IGNORE INTO warehouse_nodes VALUES ('n1', 'm1', 'Mumbai', 'MH', '400001', 1)"
    )
    db_conn.commit()


_ORDER_HEADER = (
    "order_id,merchant_id,customer_ucid,category,price_band,payment_mode,"
    "origin_node,destination_pincode,destination_cluster,address_quality,"
    "rto_score,delivery_outcome,created_at,shipping_mode"
)

_VALID_ORDER_ROW = (
    "o1,m1,cust1,fashion,mid,COD,n1,500001,cluster_a,0.8,0.45,delivered,"
    "2024-01-15 10:30:00,surface"
)


class TestLoadOrders:
    def test_valid_order(self, db_conn, tmp_path):
        _seed_merchant_and_node(db_conn)
        path = _write_csv(tmp_path, "orders.csv", _ORDER_HEADER, [_VALID_ORDER_ROW])
        res = load_orders(db_conn, path)
        assert res.loaded == 1
        assert res.skipped == 0

    def test_default_shipping_mode(self, db_conn, tmp_path):
        _seed_merchant_and_node(db_conn)
        header_no_ship = (
            "order_id,merchant_id,customer_ucid,category,price_band,payment_mode,"
            "origin_node,destination_pincode,destination_cluster,address_quality,"
            "rto_score,delivery_outcome,created_at"
        )
        row = (
            "o2,m1,cust1,fashion,mid,COD,n1,500001,cluster_a,0.8,0.45,delivered,"
            "2024-01-15 10:30:00"
        )
        path = _write_csv(tmp_path, "orders.csv", header_no_ship, [row])
        res = load_orders(db_conn, path)
        assert res.loaded == 1

        row_db = db_conn.execute("SELECT shipping_mode FROM orders WHERE order_id='o2'").fetchone()
        assert row_db["shipping_mode"] == "surface"

    def test_invalid_rto_score_skips(self, db_conn, tmp_path):
        _seed_merchant_and_node(db_conn)
        bad_row = (
            "o3,m1,cust1,fashion,mid,COD,n1,500001,cluster_a,0.8,NOT_A_NUMBER,delivered,"
            "2024-01-15 10:30:00,surface"
        )
        path = _write_csv(tmp_path, "orders.csv", _ORDER_HEADER, [bad_row])
        res = load_orders(db_conn, path)
        assert res.loaded == 0
        assert res.skipped == 1
        assert any("rto_score" in e for e in res.errors)

    def test_missing_customer_ucid_skips(self, db_conn, tmp_path):
        _seed_merchant_and_node(db_conn)
        bad_row = (
            "o4,m1,,fashion,mid,COD,n1,500001,cluster_a,0.8,0.5,delivered,"
            "2024-01-15 10:30:00,surface"
        )
        path = _write_csv(tmp_path, "orders.csv", _ORDER_HEADER, [bad_row])
        res = load_orders(db_conn, path)
        assert res.loaded == 0
        assert res.skipped == 1
        assert any("customer_ucid" in e for e in res.errors)

    def test_continues_after_bad_row(self, db_conn, tmp_path):
        _seed_merchant_and_node(db_conn)
        bad_row = "o_bad,m1,,fashion,mid,COD,n1,500001,cluster_a,0.8,0.5,delivered,2024-01-15 10:30:00,surface"
        path = _write_csv(tmp_path, "orders.csv", _ORDER_HEADER, [bad_row, _VALID_ORDER_ROW])
        res = load_orders(db_conn, path)
        assert res.loaded == 1
        assert res.skipped == 1



# ── Interventions ──────────────────────────────────────────────────────────


_INTERVENTION_HEADER = (
    "intervention_id,order_id,merchant_id,intervention_type,"
    "action_owner,initiated_by,confidence_score,outcome,executed_at,completed_at"
)


class TestLoadInterventions:
    def test_valid_intervention(self, db_conn, tmp_path):
        _seed_merchant_and_node(db_conn)
        # Need an order first
        db_conn.execute(
            "INSERT INTO orders VALUES "
            "('o1','m1','cust1','fashion','mid','COD','n1','500001','cluster_a',0.8,0.45,'delivered','surface','2024-01-15')"
        )
        db_conn.commit()

        row = "int1,o1,m1,verification,delhivery,system,0.85,success,2024-01-15 11:00:00,2024-01-15 11:05:00"
        path = _write_csv(tmp_path, "interventions.csv", _INTERVENTION_HEADER, [row])
        res = load_interventions(db_conn, path)
        assert res.loaded == 1
        assert res.skipped == 0

    def test_missing_intervention_type_skips(self, db_conn, tmp_path):
        _seed_merchant_and_node(db_conn)
        row = "int2,o1,m1,,delhivery,system,0.85,,2024-01-15 11:00:00,"
        path = _write_csv(tmp_path, "interventions.csv", _INTERVENTION_HEADER, [row])
        res = load_interventions(db_conn, path)
        assert res.skipped == 1
        assert any("intervention_type" in e for e in res.errors)

    def test_invalid_confidence_score_skips(self, db_conn, tmp_path):
        _seed_merchant_and_node(db_conn)
        db_conn.execute(
            "INSERT OR IGNORE INTO orders VALUES "
            "('o1','m1','cust1','fashion','mid','COD','n1','500001','cluster_a',0.8,0.45,'delivered','surface','2024-01-15')"
        )
        db_conn.commit()
        row = "int3,o1,m1,verification,delhivery,system,BAD,,2024-01-15 11:00:00,"
        path = _write_csv(tmp_path, "interventions.csv", _INTERVENTION_HEADER, [row])
        res = load_interventions(db_conn, path)
        assert res.skipped == 1
        assert any("confidence_score" in e for e in res.errors)


# ── load_all ───────────────────────────────────────────────────────────────


class TestLoadAll:
    def test_loads_available_files(self, db_conn, tmp_path):
        _write_csv(
            tmp_path,
            "merchants.csv",
            "merchant_id,name",
            ["m1,Acme Corp"],
        )
        results = load_all(db_conn, str(tmp_path))
        assert "merchants" in results
        assert results["merchants"].loaded == 1

    def test_skips_missing_files(self, db_conn, tmp_path):
        # Empty directory — nothing to load
        results = load_all(db_conn, str(tmp_path))
        assert results == {}

    def test_unsupported_extension_raises(self, db_conn, tmp_path):
        path = tmp_path / "merchants.xml"
        path.write_text("<data/>")
        with pytest.raises(ValueError, match="Unsupported file extension"):
            load_merchants(db_conn, str(path))


# ── LoadResult dataclass ──────────────────────────────────────────────────


class TestLoadResult:
    def test_defaults(self):
        r = LoadResult(table="test")
        assert r.loaded == 0
        assert r.skipped == 0
        assert r.errors == []

    def test_error_accumulation(self):
        r = LoadResult(table="test")
        r.errors.append("Row 1: bad")
        r.errors.append("Row 2: bad")
        assert len(r.errors) == 2
