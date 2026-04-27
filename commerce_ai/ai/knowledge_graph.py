"""Risk Knowledge Graph for explainable RTO risk reasoning.

Builds a NetworkX directed graph from order data, modeling relationships
between merchants, warehouse nodes, destination clusters, customers,
categories, and payment modes. Graph traversal produces structured risk
paths that explain *why* an order is risky.
"""

from __future__ import annotations

import logging
import math
import sqlite3
from dataclasses import dataclass, field

import networkx as nx

import config as cfg

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class RiskFactor:
    """A single contributing factor in the risk path."""

    factor_type: str  # "customer_history", "cluster_rto", "category_performance", "address_quality", "payment_mode"
    description: str  # e.g., "Customer has 3 prior RTOs with this merchant"
    weight: float  # 0-1, how much this factor contributes to risk
    data: dict = field(default_factory=dict)  # Raw data backing this factor


@dataclass
class RiskPath:
    """Structured risk explanation from knowledge graph traversal."""

    order_id: str
    rto_score: float
    factors: list[RiskFactor] = field(default_factory=list)
    total_risk_weight: float = 0.0  # Sum of factor weights, normalized to 0-1


# ---------------------------------------------------------------------------
# Knowledge Graph
# ---------------------------------------------------------------------------

class RiskKnowledgeGraph:
    """NetworkX-based knowledge graph for explainable RTO risk reasoning."""

    def __init__(self) -> None:
        self.graph = nx.DiGraph()

    # ------------------------------------------------------------------
    # Graph construction
    # ------------------------------------------------------------------

    def build_graph(self, db: sqlite3.Connection) -> None:
        """Build the knowledge graph from order data in SQLite.

        Creates nodes and edges for:
        1. Merchant → Warehouse Node (edge weight: order volume)
        2. Warehouse Node → Destination Cluster (edge weight: delivery rate, RTO rate, order count)
        3. Customer → Merchant (edge weight: total orders, RTO count, RTO rate)
        4. Category → Destination Cluster (edge weight: delivery rate by category in that cluster)
        5. Payment Mode → Destination Cluster (edge weight: RTO rate by payment mode in that cluster)
        """
        # Sample up to 500k orders for graph build — avoids full scans on large datasets.
        # The statistics are representative enough at this scale.
        SAMPLE = "SELECT * FROM orders ORDER BY ROWID LIMIT 500000"

        # 1. Merchant → Warehouse Node
        rows = db.execute(
            f"""
            SELECT
                o.merchant_id,
                o.origin_node,
                COUNT(*) AS order_count
            FROM ({SAMPLE}) o
            GROUP BY o.merchant_id, o.origin_node
            """
        ).fetchall()
        for r in rows:
            m_id = f"merchant:{r['merchant_id']}"
            w_id = f"warehouse:{r['origin_node']}"
            self.graph.add_node(m_id, node_type="merchant")
            self.graph.add_node(w_id, node_type="warehouse")
            self.graph.add_edge(m_id, w_id, order_count=r["order_count"], edge_type="ships_from")

        # 2. Warehouse Node → Destination Cluster
        rows = db.execute(
            f"""
            SELECT
                origin_node,
                destination_cluster,
                COUNT(*) AS order_count,
                AVG(CASE WHEN delivery_outcome = 'delivered' THEN 1.0 ELSE 0.0 END) AS delivery_rate,
                AVG(CASE WHEN delivery_outcome = 'rto' THEN 1.0 ELSE 0.0 END) AS rto_rate,
                SUM(CASE WHEN delivery_outcome = 'rto' THEN 1 ELSE 0 END) AS rto_count
            FROM ({SAMPLE})
            GROUP BY origin_node, destination_cluster
            """
        ).fetchall()
        for r in rows:
            w_id = f"warehouse:{r['origin_node']}"
            c_id = f"cluster:{r['destination_cluster']}"
            self.graph.add_node(w_id, node_type="warehouse")
            self.graph.add_node(c_id, node_type="cluster")
            self.graph.add_edge(
                w_id, c_id,
                order_count=r["order_count"],
                delivery_rate=r["delivery_rate"],
                rto_rate=r["rto_rate"],
                rto_count=r["rto_count"],
                edge_type="ships_to",
            )

        # 3. Customer → Merchant
        rows = db.execute(
            f"""
            SELECT
                customer_ucid,
                merchant_id,
                COUNT(*) AS total_orders,
                SUM(CASE WHEN delivery_outcome = 'rto' THEN 1 ELSE 0 END) AS rto_count
            FROM ({SAMPLE})
            GROUP BY customer_ucid, merchant_id
            """
        ).fetchall()
        for r in rows:
            cu_id = f"customer:{r['customer_ucid']}"
            m_id = f"merchant:{r['merchant_id']}"
            total = r["total_orders"]
            rto_cnt = r["rto_count"]
            self.graph.add_node(cu_id, node_type="customer")
            self.graph.add_edge(
                cu_id, m_id,
                total_orders=total,
                rto_count=rto_cnt,
                rto_rate=rto_cnt / total if total > 0 else 0.0,
                edge_type="orders_from",
            )

        # 4. Category → Destination Cluster
        rows = db.execute(
            f"""
            SELECT
                category,
                destination_cluster,
                COUNT(*) AS order_count,
                AVG(CASE WHEN delivery_outcome = 'delivered' THEN 1.0 ELSE 0.0 END) AS delivery_rate,
                AVG(CASE WHEN delivery_outcome = 'rto' THEN 1.0 ELSE 0.0 END) AS rto_rate,
                SUM(CASE WHEN delivery_outcome = 'rto' THEN 1 ELSE 0 END) AS rto_count
            FROM ({SAMPLE})
            GROUP BY category, destination_cluster
            """
        ).fetchall()
        for r in rows:
            cat_id = f"category:{r['category']}"
            c_id = f"cluster:{r['destination_cluster']}"
            self.graph.add_node(cat_id, node_type="category")
            self.graph.add_edge(
                cat_id, c_id,
                order_count=r["order_count"],
                delivery_rate=r["delivery_rate"],
                rto_rate=r["rto_rate"],
                rto_count=r["rto_count"],
                edge_type="category_performance",
            )

        # 5. Payment Mode → Destination Cluster
        rows = db.execute(
            f"""
            SELECT
                payment_mode,
                destination_cluster,
                COUNT(*) AS order_count,
                AVG(CASE WHEN delivery_outcome = 'rto' THEN 1.0 ELSE 0.0 END) AS rto_rate,
                SUM(CASE WHEN delivery_outcome = 'rto' THEN 1 ELSE 0 END) AS rto_count
            FROM ({SAMPLE})
            GROUP BY payment_mode, destination_cluster
            """
        ).fetchall()
        for r in rows:
            p_id = f"payment:{r['payment_mode']}"
            c_id = f"cluster:{r['destination_cluster']}"
            self.graph.add_node(p_id, node_type="payment_mode")
            self.graph.add_edge(
                p_id, c_id,
                order_count=r["order_count"],
                rto_rate=r["rto_rate"],
                rto_count=r["rto_count"],
                edge_type="payment_performance",
            )

    # ------------------------------------------------------------------
    # Risk path traversal
    # ------------------------------------------------------------------

    def get_risk_path(self, order: dict, db: sqlite3.Connection) -> RiskPath:
        """Traverse the graph to find the risk explanation for a specific order.

        Checks these risk factors in order:
        1. Customer history — prior RTOs with this merchant
        2. Cluster RTO rate — destination cluster + payment mode RTO rate
        3. Category performance — category in this cluster vs network average
        4. Address quality — below threshold
        5. Payment mode risk — COD in a high-RTO cluster

        Returns RiskPath with all applicable factors, sorted by weight descending.
        """
        factors: list[RiskFactor] = []

        customer_id = f"customer:{order.get('customer_ucid', '')}"
        merchant_id = f"merchant:{order.get('merchant_id', '')}"
        cluster_id = f"cluster:{order.get('destination_cluster', '')}"
        category_id = f"category:{order.get('category', '')}"
        payment_id = f"payment:{order.get('payment_mode', '')}"

        # --- 1. Customer history ---
        if self.graph.has_edge(customer_id, merchant_id):
            edge = self.graph.edges[customer_id, merchant_id]
            rto_count = edge.get("rto_count", 0)
            total_orders = edge.get("total_orders", 0)
            if rto_count > 0:
                # Weight scales with number of prior RTOs, capped at 0.4
                weight = min(0.4, 0.1 * rto_count)
                factors.append(RiskFactor(
                    factor_type="customer_history",
                    description=(
                        f"Customer has {rto_count} prior RTO(s) out of "
                        f"{total_orders} orders with this merchant"
                    ),
                    weight=weight,
                    data={"rto_count": rto_count, "total_orders": total_orders,
                          "rto_rate": edge.get("rto_rate", 0.0)},
                ))

        # --- 2. Cluster RTO rate (for this payment mode) ---
        if self.graph.has_edge(payment_id, cluster_id):
            edge = self.graph.edges[payment_id, cluster_id]
            edge_orders = edge.get("order_count", 0)
            if edge_orders >= cfg.GRAPH_MIN_EDGE_ORDERS:
                cluster_rto = edge.get("rto_rate", 0.0)
                if not (math.isnan(cluster_rto) or math.isinf(cluster_rto)):
                    network_avg = self._network_avg_rto_for_payment(order.get("payment_mode", ""))
                    if cluster_rto > network_avg and network_avg > 0:
                        excess = cluster_rto - network_avg
                        weight = min(0.35, excess * 1.5)
                        factors.append(RiskFactor(
                            factor_type="cluster_rto",
                            description=(
                                f"Destination cluster '{order.get('destination_cluster', '')}' has "
                                f"{cluster_rto:.0%} RTO rate for {order.get('payment_mode', '')} "
                                f"(network avg: {network_avg:.0%})"
                            ),
                            weight=weight,
                            data={"cluster_rto_rate": cluster_rto, "network_avg": network_avg,
                                  "order_count": edge_orders},
                        ))

        # --- 3. Category performance in this cluster ---
        if self.graph.has_edge(category_id, cluster_id):
            edge = self.graph.edges[category_id, cluster_id]
            edge_orders = edge.get("order_count", 0)
            if edge_orders >= cfg.GRAPH_MIN_EDGE_ORDERS:
                cat_delivery = edge.get("delivery_rate", 0.0)
                if not (math.isnan(cat_delivery) or math.isinf(cat_delivery)):
                    network_cat_avg = self._network_avg_delivery_for_category(
                        order.get("category", "")
                    )
                    if network_cat_avg > 0 and cat_delivery < network_cat_avg:
                        gap = network_cat_avg - cat_delivery
                        weight = min(0.3, gap * 1.0)
                        factors.append(RiskFactor(
                            factor_type="category_performance",
                            description=(
                                f"Category '{order.get('category', '')}' in cluster "
                                f"'{order.get('destination_cluster', '')}' has "
                                f"{cat_delivery:.0%} delivery rate "
                                f"(network avg: {network_cat_avg:.0%})"
                            ),
                            weight=weight,
                            data={"cluster_delivery_rate": cat_delivery,
                                  "network_avg": network_cat_avg,
                                  "order_count": edge_orders},
                        ))

        # --- 4. Address quality ---
        address_quality = order.get("address_quality", 1.0)
        if address_quality is None or math.isnan(float(address_quality)):
            address_quality = 1.0
        if address_quality < cfg.ADDRESS_QUALITY_THRESHOLD:
            weight = min(0.3, (cfg.ADDRESS_QUALITY_THRESHOLD - address_quality) * 0.6)
            factors.append(RiskFactor(
                factor_type="address_quality",
                description=(
                    f"Address quality score is {address_quality:.2f} "
                    f"(below threshold of {cfg.ADDRESS_QUALITY_THRESHOLD})"
                ),
                weight=weight,
                data={
                    "address_quality": address_quality,
                    "threshold": cfg.ADDRESS_QUALITY_THRESHOLD,
                },
            ))

        # --- 5. Payment mode risk (COD in high-RTO cluster) ---
        if order.get("payment_mode") == "COD" and self.graph.has_edge(payment_id, cluster_id):
            edge = self.graph.edges[payment_id, cluster_id]
            cod_rto = edge.get("rto_rate", 0.0)
            if math.isnan(cod_rto) or math.isinf(cod_rto):
                cod_rto = 0.0
            # Compare COD vs prepaid RTO in same cluster
            prepaid_id = "payment:prepaid"
            prepaid_rto = 0.0
            if self.graph.has_edge(prepaid_id, cluster_id):
                _pr = self.graph.edges[prepaid_id, cluster_id].get("rto_rate", 0.0)
                if not (math.isnan(_pr) or math.isinf(_pr)):
                    prepaid_rto = _pr
            gap = cod_rto - prepaid_rto
            if gap > 0.05:  # meaningful gap
                weight = min(0.25, gap * 0.8)
                factors.append(RiskFactor(
                    factor_type="payment_mode",
                    description=(
                        f"COD RTO rate ({cod_rto:.0%}) is {gap:.0%} higher than "
                        f"prepaid ({prepaid_rto:.0%}) in cluster "
                        f"'{order.get('destination_cluster', '')}'"
                    ),
                    weight=weight,
                    data={"cod_rto": cod_rto, "prepaid_rto": prepaid_rto, "gap": gap},
                ))

        # Sort by weight descending
        factors.sort(key=lambda f: f.weight, reverse=True)

        # Normalize total_risk_weight to [0, 1]
        raw_total = sum(f.weight for f in factors)
        total_risk_weight = min(1.0, raw_total)

        return RiskPath(
            order_id=order.get("order_id", ""),
            rto_score=order.get("rto_score", 0.0),
            factors=factors,
            total_risk_weight=total_risk_weight,
        )

    # ------------------------------------------------------------------
    # Graph maintenance
    # ------------------------------------------------------------------

    def update_edge_weights(self, db: sqlite3.Connection) -> None:
        """Refresh all edge weights from current data. Called periodically."""
        self.graph.clear()
        self.build_graph(db)

    # ------------------------------------------------------------------
    # Node / edge inspection
    # ------------------------------------------------------------------

    def get_node_info(self, node_id: str) -> dict | None:
        """Get attributes of a specific node in the graph."""
        if node_id not in self.graph:
            return None
        return dict(self.graph.nodes[node_id])

    def get_edge_info(self, source: str, target: str) -> dict | None:
        """Get attributes of a specific edge in the graph."""
        if not self.graph.has_edge(source, target):
            return None
        return dict(self.graph.edges[source, target])

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _network_avg_rto_for_payment(self, payment_mode: str) -> float:
        """Compute the average RTO rate across all clusters for a payment mode."""
        p_id = f"payment:{payment_mode}"
        if p_id not in self.graph:
            return 0.0
        total_orders = 0
        weighted_rto = 0.0
        for _, target, data in self.graph.out_edges(p_id, data=True):
            cnt = data.get("order_count", 0)
            total_orders += cnt
            weighted_rto += data.get("rto_rate", 0.0) * cnt
        return weighted_rto / total_orders if total_orders > 0 else 0.0

    def _network_avg_delivery_for_category(self, category: str) -> float:
        """Compute the average delivery rate across all clusters for a category."""
        cat_id = f"category:{category}"
        if cat_id not in self.graph:
            return 0.0
        total_orders = 0
        weighted_delivery = 0.0
        for _, target, data in self.graph.out_edges(cat_id, data=True):
            cnt = data.get("order_count", 0)
            total_orders += cnt
            weighted_delivery += data.get("delivery_rate", 0.0) * cnt
        return weighted_delivery / total_orders if total_orders > 0 else 0.0
