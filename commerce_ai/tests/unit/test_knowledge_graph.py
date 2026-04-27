"""Unit tests for ai/knowledge_graph.py — RiskKnowledgeGraph."""

from __future__ import annotations

import sqlite3

import pytest

from ai.knowledge_graph import RiskFactor, RiskKnowledgeGraph, RiskPath


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def db() -> sqlite3.Connection:
    """In-memory SQLite database with schema and sample data."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE merchants (
            merchant_id TEXT PRIMARY KEY,
            name TEXT NOT NULL
        );
        CREATE TABLE warehouse_nodes (
            node_id TEXT PRIMARY KEY,
            merchant_id TEXT NOT NULL,
            city TEXT, state TEXT, pincode TEXT,
            is_active BOOLEAN DEFAULT TRUE
        );
        CREATE TABLE orders (
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
        """
    )
    # Seed merchants and warehouse nodes
    conn.execute("INSERT INTO merchants VALUES ('M001', 'Merchant One')")
    conn.execute("INSERT INTO merchants VALUES ('M002', 'Merchant Two')")
    conn.execute(
        "INSERT INTO warehouse_nodes VALUES ('WH001', 'M001', 'Delhi', 'DL', '110001', 1)"
    )
    conn.execute(
        "INSERT INTO warehouse_nodes VALUES ('WH002', 'M002', 'Mumbai', 'MH', '400001', 1)"
    )

    # Seed orders — mix of delivered and RTO across clusters/categories/payment modes
    orders = [
        # Cluster 'north' — moderate RTO for COD, low for prepaid
        ("O001", "M001", "C100", "fashion",     "0-500",    "COD",     "WH001", "110001", "north", 0.8, 0.6, "delivered", "surface", "2025-01-01"),
        ("O002", "M001", "C100", "fashion",     "0-500",    "COD",     "WH001", "110002", "north", 0.7, 0.7, "rto",       "surface", "2025-01-02"),
        ("O003", "M001", "C100", "fashion",     "0-500",    "COD",     "WH001", "110003", "north", 0.9, 0.3, "delivered", "surface", "2025-01-03"),
        ("O004", "M001", "C200", "electronics", "2000+",    "prepaid", "WH001", "110004", "north", 0.9, 0.2, "delivered", "surface", "2025-01-04"),
        ("O005", "M001", "C200", "electronics", "2000+",    "prepaid", "WH001", "110005", "north", 0.8, 0.1, "delivered", "surface", "2025-01-05"),
        # Cluster 'south' — high RTO for COD
        ("O006", "M002", "C300", "beauty",      "500-1000", "COD",     "WH002", "600001", "south", 0.3, 0.8, "rto",       "surface", "2025-01-06"),
        ("O007", "M002", "C300", "beauty",      "500-1000", "COD",     "WH002", "600002", "south", 0.4, 0.9, "rto",       "surface", "2025-01-07"),
        ("O008", "M002", "C300", "beauty",      "500-1000", "COD",     "WH002", "600003", "south", 0.6, 0.5, "delivered", "surface", "2025-01-08"),
        ("O009", "M002", "C400", "fashion",     "0-500",    "prepaid", "WH002", "600004", "south", 0.9, 0.1, "delivered", "surface", "2025-01-09"),
        # Customer C100 has 1 RTO with M001 (O002)
        # Customer C300 has 2 RTOs with M002 (O006, O007)
    ]
    conn.executemany(
        """
        INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        orders,
    )
    conn.commit()
    return conn


@pytest.fixture()
def graph(db: sqlite3.Connection) -> RiskKnowledgeGraph:
    """Pre-built knowledge graph."""
    kg = RiskKnowledgeGraph()
    kg.build_graph(db)
    return kg


# ---------------------------------------------------------------------------
# Tests — Graph construction
# ---------------------------------------------------------------------------

class TestBuildGraph:
    def test_creates_merchant_nodes(self, graph: RiskKnowledgeGraph):
        assert graph.get_node_info("merchant:M001") == {"node_type": "merchant"}
        assert graph.get_node_info("merchant:M002") == {"node_type": "merchant"}

    def test_creates_warehouse_nodes(self, graph: RiskKnowledgeGraph):
        assert graph.get_node_info("warehouse:WH001") == {"node_type": "warehouse"}
        assert graph.get_node_info("warehouse:WH002") == {"node_type": "warehouse"}

    def test_creates_cluster_nodes(self, graph: RiskKnowledgeGraph):
        assert graph.get_node_info("cluster:north") == {"node_type": "cluster"}
        assert graph.get_node_info("cluster:south") == {"node_type": "cluster"}

    def test_creates_category_nodes(self, graph: RiskKnowledgeGraph):
        assert graph.get_node_info("category:fashion") == {"node_type": "category"}
        assert graph.get_node_info("category:electronics") == {"node_type": "category"}
        assert graph.get_node_info("category:beauty") == {"node_type": "category"}

    def test_creates_payment_nodes(self, graph: RiskKnowledgeGraph):
        assert graph.get_node_info("payment:COD") == {"node_type": "payment_mode"}
        assert graph.get_node_info("payment:prepaid") == {"node_type": "payment_mode"}

    def test_creates_customer_nodes(self, graph: RiskKnowledgeGraph):
        assert graph.get_node_info("customer:C100") == {"node_type": "customer"}
        assert graph.get_node_info("customer:C300") == {"node_type": "customer"}

    def test_merchant_to_warehouse_edge(self, graph: RiskKnowledgeGraph):
        edge = graph.get_edge_info("merchant:M001", "warehouse:WH001")
        assert edge is not None
        assert edge["edge_type"] == "ships_from"
        assert edge["order_count"] == 5  # O001-O005

    def test_warehouse_to_cluster_edge(self, graph: RiskKnowledgeGraph):
        edge = graph.get_edge_info("warehouse:WH001", "cluster:north")
        assert edge is not None
        assert edge["edge_type"] == "ships_to"
        assert edge["order_count"] == 5

    def test_customer_to_merchant_edge(self, graph: RiskKnowledgeGraph):
        edge = graph.get_edge_info("customer:C100", "merchant:M001")
        assert edge is not None
        assert edge["total_orders"] == 3  # O001, O002, O003
        assert edge["rto_count"] == 1  # O002

    def test_category_to_cluster_edge(self, graph: RiskKnowledgeGraph):
        edge = graph.get_edge_info("category:beauty", "cluster:south")
        assert edge is not None
        assert edge["order_count"] == 3  # O006, O007, O008

    def test_payment_to_cluster_edge(self, graph: RiskKnowledgeGraph):
        edge = graph.get_edge_info("payment:COD", "cluster:south")
        assert edge is not None
        assert edge["order_count"] == 3  # O006, O007, O008

    def test_nonexistent_node_returns_none(self, graph: RiskKnowledgeGraph):
        assert graph.get_node_info("merchant:NONEXISTENT") is None

    def test_nonexistent_edge_returns_none(self, graph: RiskKnowledgeGraph):
        assert graph.get_edge_info("merchant:M001", "cluster:south") is None


# ---------------------------------------------------------------------------
# Tests — Risk path
# ---------------------------------------------------------------------------

class TestGetRiskPath:
    def test_returns_risk_path_dataclass(self, graph: RiskKnowledgeGraph, db: sqlite3.Connection):
        order = {
            "order_id": "O002",
            "merchant_id": "M001",
            "customer_ucid": "C100",
            "category": "fashion",
            "price_band": "0-500",
            "payment_mode": "COD",
            "origin_node": "WH001",
            "destination_cluster": "north",
            "address_quality": 0.7,
            "rto_score": 0.7,
        }
        path = graph.get_risk_path(order, db)
        assert isinstance(path, RiskPath)
        assert path.order_id == "O002"
        assert path.rto_score == 0.7

    def test_customer_history_factor(self, graph: RiskKnowledgeGraph, db: sqlite3.Connection):
        """Customer C100 has 1 RTO with M001 — should produce a customer_history factor."""
        order = {
            "order_id": "NEW1",
            "merchant_id": "M001",
            "customer_ucid": "C100",
            "category": "fashion",
            "price_band": "0-500",
            "payment_mode": "COD",
            "origin_node": "WH001",
            "destination_cluster": "north",
            "address_quality": 0.9,
            "rto_score": 0.5,
        }
        path = graph.get_risk_path(order, db)
        customer_factors = [f for f in path.factors if f.factor_type == "customer_history"]
        assert len(customer_factors) == 1
        assert customer_factors[0].data["rto_count"] == 1

    def test_high_rto_customer_higher_weight(self, graph: RiskKnowledgeGraph, db: sqlite3.Connection):
        """Customer C300 has 2 RTOs with M002 — weight should be higher than C100's 1 RTO."""
        order = {
            "order_id": "NEW2",
            "merchant_id": "M002",
            "customer_ucid": "C300",
            "category": "beauty",
            "price_band": "500-1000",
            "payment_mode": "COD",
            "origin_node": "WH002",
            "destination_cluster": "south",
            "address_quality": 0.3,
            "rto_score": 0.8,
        }
        path = graph.get_risk_path(order, db)
        customer_factors = [f for f in path.factors if f.factor_type == "customer_history"]
        assert len(customer_factors) == 1
        assert customer_factors[0].data["rto_count"] == 2
        assert customer_factors[0].weight > 0.1  # 0.1 * 2 = 0.2

    def test_address_quality_factor(self, graph: RiskKnowledgeGraph, db: sqlite3.Connection):
        """Low address quality should produce an address_quality factor."""
        order = {
            "order_id": "NEW3",
            "merchant_id": "M002",
            "customer_ucid": "C999",  # new customer, no history
            "category": "beauty",
            "price_band": "500-1000",
            "payment_mode": "prepaid",
            "origin_node": "WH002",
            "destination_cluster": "south",
            "address_quality": 0.2,
            "rto_score": 0.6,
        }
        path = graph.get_risk_path(order, db)
        addr_factors = [f for f in path.factors if f.factor_type == "address_quality"]
        assert len(addr_factors) == 1
        assert addr_factors[0].data["address_quality"] == 0.2

    def test_no_address_factor_when_quality_ok(self, graph: RiskKnowledgeGraph, db: sqlite3.Connection):
        order = {
            "order_id": "NEW4",
            "merchant_id": "M001",
            "customer_ucid": "C999",
            "category": "fashion",
            "price_band": "0-500",
            "payment_mode": "prepaid",
            "origin_node": "WH001",
            "destination_cluster": "north",
            "address_quality": 0.8,
            "rto_score": 0.3,
        }
        path = graph.get_risk_path(order, db)
        addr_factors = [f for f in path.factors if f.factor_type == "address_quality"]
        assert len(addr_factors) == 0

    def test_new_customer_no_history_factor(self, graph: RiskKnowledgeGraph, db: sqlite3.Connection):
        """A brand-new customer should have no customer_history factor."""
        order = {
            "order_id": "NEW5",
            "merchant_id": "M001",
            "customer_ucid": "BRAND_NEW",
            "category": "fashion",
            "price_band": "0-500",
            "payment_mode": "prepaid",
            "origin_node": "WH001",
            "destination_cluster": "north",
            "address_quality": 0.9,
            "rto_score": 0.2,
        }
        path = graph.get_risk_path(order, db)
        customer_factors = [f for f in path.factors if f.factor_type == "customer_history"]
        assert len(customer_factors) == 0

    def test_factors_sorted_by_weight_descending(self, graph: RiskKnowledgeGraph, db: sqlite3.Connection):
        """Factors should be sorted by weight, highest first."""
        order = {
            "order_id": "NEW6",
            "merchant_id": "M002",
            "customer_ucid": "C300",
            "category": "beauty",
            "price_band": "500-1000",
            "payment_mode": "COD",
            "origin_node": "WH002",
            "destination_cluster": "south",
            "address_quality": 0.2,
            "rto_score": 0.9,
        }
        path = graph.get_risk_path(order, db)
        weights = [f.weight for f in path.factors]
        assert weights == sorted(weights, reverse=True)

    def test_total_risk_weight_capped_at_one(self, graph: RiskKnowledgeGraph, db: sqlite3.Connection):
        """total_risk_weight should never exceed 1.0."""
        order = {
            "order_id": "NEW7",
            "merchant_id": "M002",
            "customer_ucid": "C300",
            "category": "beauty",
            "price_band": "500-1000",
            "payment_mode": "COD",
            "origin_node": "WH002",
            "destination_cluster": "south",
            "address_quality": 0.1,
            "rto_score": 0.95,
        }
        path = graph.get_risk_path(order, db)
        assert 0.0 <= path.total_risk_weight <= 1.0

    def test_empty_graph_returns_empty_factors(self, db: sqlite3.Connection):
        """An empty graph should still return a valid RiskPath with no factors (except address)."""
        kg = RiskKnowledgeGraph()
        order = {
            "order_id": "EMPTY1",
            "merchant_id": "M001",
            "customer_ucid": "C100",
            "category": "fashion",
            "payment_mode": "COD",
            "destination_cluster": "north",
            "address_quality": 0.9,
            "rto_score": 0.5,
        }
        path = kg.get_risk_path(order, db)
        assert isinstance(path, RiskPath)
        assert path.order_id == "EMPTY1"
        # No graph-based factors, and address quality is fine
        assert len(path.factors) == 0


# ---------------------------------------------------------------------------
# Tests — Update and maintenance
# ---------------------------------------------------------------------------

class TestUpdateEdgeWeights:
    def test_update_rebuilds_graph(self, graph: RiskKnowledgeGraph, db: sqlite3.Connection):
        """update_edge_weights should clear and rebuild the graph."""
        original_node_count = graph.graph.number_of_nodes()
        assert original_node_count > 0

        graph.update_edge_weights(db)
        assert graph.graph.number_of_nodes() == original_node_count

    def test_update_reflects_new_data(self, graph: RiskKnowledgeGraph, db: sqlite3.Connection):
        """After inserting new orders, update should reflect them."""
        # Add a new order for a new cluster
        db.execute(
            """
            INSERT INTO orders VALUES
            ('O100', 'M001', 'C100', 'fashion', '0-500', 'COD', 'WH001',
             '999999', 'newcluster', 0.5, 0.5, 'rto', 'surface', '2025-02-01')
            """
        )
        db.commit()

        assert graph.get_node_info("cluster:newcluster") is None
        graph.update_edge_weights(db)
        assert graph.get_node_info("cluster:newcluster") is not None


# ---------------------------------------------------------------------------
# Tests — Data classes
# ---------------------------------------------------------------------------

class TestDataClasses:
    def test_risk_factor_defaults(self):
        rf = RiskFactor(factor_type="test", description="desc", weight=0.5)
        assert rf.data == {}

    def test_risk_path_defaults(self):
        rp = RiskPath(order_id="X", rto_score=0.5)
        assert rp.factors == []
        assert rp.total_risk_weight == 0.0
