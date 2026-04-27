"""SQLite connection manager and schema initialization."""

import sqlite3

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS merchants (
    merchant_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS warehouse_nodes (
    node_id TEXT PRIMARY KEY,
    merchant_id TEXT NOT NULL REFERENCES merchants(merchant_id),
    city TEXT NOT NULL,
    state TEXT NOT NULL,
    pincode TEXT NOT NULL,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    merchant_id TEXT NOT NULL REFERENCES merchants(merchant_id),
    customer_ucid TEXT NOT NULL,
    category TEXT NOT NULL,
    price_band TEXT NOT NULL,
    payment_mode TEXT NOT NULL,
    origin_node TEXT NOT NULL REFERENCES warehouse_nodes(node_id),
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
    order_id TEXT NOT NULL REFERENCES orders(order_id),
    merchant_id TEXT NOT NULL REFERENCES merchants(merchant_id),
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
    order_id TEXT NOT NULL REFERENCES orders(order_id),
    merchant_id TEXT NOT NULL REFERENCES merchants(merchant_id),
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
    merchant_id TEXT NOT NULL REFERENCES merchants(merchant_id),
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
"""

_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_orders_merchant_id ON orders(merchant_id);
CREATE INDEX IF NOT EXISTS idx_orders_customer_ucid ON orders(customer_ucid);
CREATE INDEX IF NOT EXISTS idx_orders_destination_cluster ON orders(destination_cluster);
CREATE INDEX IF NOT EXISTS idx_interventions_order_id ON interventions(order_id);
CREATE INDEX IF NOT EXISTS idx_interventions_merchant_id ON interventions(merchant_id);
CREATE INDEX IF NOT EXISTS idx_communication_logs_order_id ON communication_logs(order_id);
CREATE INDEX IF NOT EXISTS idx_orders_cohort ON orders(merchant_id, category, price_band, payment_mode, delivery_outcome);
"""


def get_db(db_path: str) -> sqlite3.Connection:
    """Return a SQLite connection with WAL mode and Row factory."""
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: str) -> None:
    """Create all tables and indexes if they don't exist."""
    conn = get_db(db_path)
    try:
        conn.executescript(_SCHEMA_SQL)
        conn.executescript(_INDEX_SQL)
        conn.commit()
    finally:
        conn.close()


def close_db(conn: sqlite3.Connection) -> None:
    """Close a SQLite connection."""
    conn.close()
