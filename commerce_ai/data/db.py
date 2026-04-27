"""Database connection manager — supports SQLite and Postgres.

Set DATABASE_URL env var to a postgresql:// DSN to use Postgres.
Otherwise falls back to SQLite (COMMERCE_AI_DB or default path).

Both backends expose the same interface:
    - conn.execute(sql, params) with %s placeholders
    - rows are dict-like (row["column_name"])
    - conn.commit() / conn.close()
"""

from __future__ import annotations

import os
import sqlite3
import logging
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Backend detection
# ---------------------------------------------------------------------------

DATABASE_URL: str | None = os.environ.get("DATABASE_URL")


def is_postgres() -> bool:
    return DATABASE_URL is not None and DATABASE_URL.startswith("postgresql")



# ---------------------------------------------------------------------------
# Postgres wrapper — makes psycopg2 behave like sqlite3 with Row factory
# ---------------------------------------------------------------------------

class PgCursorWrapper:
    """Wraps a psycopg2 cursor so fetchone/fetchall return dict-like rows."""

    def __init__(self, real_cursor):
        self._cursor = real_cursor

    def fetchall(self):
        rows = self._cursor.fetchall()
        if not rows:
            return []
        cols = [desc[0] for desc in self._cursor.description]
        return [dict(zip(cols, row)) for row in rows]

    def fetchone(self):
        row = self._cursor.fetchone()
        if row is None:
            return None
        cols = [desc[0] for desc in self._cursor.description]
        return dict(zip(cols, row))

    def __iter__(self):
        return iter(self.fetchall())


class PgConnectionWrapper:
    """Wraps a psycopg2 connection to match the sqlite3.Connection interface.

    Key differences handled:
        - Converts ? placeholders to %s for psycopg2
        - Returns dict-like rows via PgCursorWrapper
        - executescript() splits on ; and runs each statement
    """

    def __init__(self, pg_conn):
        self._conn = pg_conn

    @staticmethod
    def _convert_placeholders(sql: str) -> str:
        """Convert SQLite ? placeholders to Postgres %s.

        Careful not to replace ? inside string literals or ?? (which is a
        Postgres JSON operator).
        """
        result = []
        in_string = False
        quote_char = None
        i = 0
        while i < len(sql):
            ch = sql[i]
            if in_string:
                result.append(ch)
                if ch == quote_char:
                    in_string = False
            elif ch in ("'", '"'):
                in_string = True
                quote_char = ch
                result.append(ch)
            elif ch == '?' and (i + 1 >= len(sql) or sql[i + 1] != '?'):
                result.append('%s')
            else:
                result.append(ch)
            i += 1
        return ''.join(result)

    def execute(self, sql: str, params: tuple | list = ()) -> PgCursorWrapper:
        cur = self._conn.cursor()
        converted = self._convert_placeholders(sql)
        try:
            cur.execute(converted, params)
        except Exception:
            # Roll back the failed transaction so subsequent queries work
            self._conn.rollback()
            raise
        return PgCursorWrapper(cur)

    def executescript(self, sql: str) -> None:
        cur = self._conn.cursor()
        cur.execute(sql)
        self._conn.commit()

    def commit(self) -> None:
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    @property
    def raw(self):
        """Access the underlying psycopg2 connection for advanced use."""
        return self._conn



# ---------------------------------------------------------------------------
# SQLite schema (used only when running in SQLite mode)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Public API — get_db / init_db / close_db
# ---------------------------------------------------------------------------

def get_db(db_path: str = "") -> PgConnectionWrapper | sqlite3.Connection:
    """Return a database connection.

    If DATABASE_URL is set to a postgresql:// DSN, returns a PgConnectionWrapper.
    Otherwise returns a sqlite3.Connection using db_path.
    """
    if is_postgres():
        import psycopg2
        logger.info("Connecting to Postgres: %s", DATABASE_URL[:40] + "...")
        pg_conn = psycopg2.connect(DATABASE_URL)
        pg_conn.autocommit = False
        return PgConnectionWrapper(pg_conn)
    else:
        logger.info("Using SQLite: %s", db_path)
        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.row_factory = sqlite3.Row
        return conn


def init_db(db_path: str = "") -> None:
    """Initialize the database schema.

    For Postgres: assumes schema was applied by the ETL (postgres_schema.sql).
    For SQLite: creates tables and indexes.
    """
    if is_postgres():
        logger.info("Postgres mode — schema managed by ETL, skipping init_db")
        return

    conn = get_db(db_path)
    try:
        conn.executescript(_SCHEMA_SQL)
        conn.executescript(_INDEX_SQL)
        conn.commit()
    finally:
        conn.close()


def close_db(conn) -> None:
    """Close a database connection."""
    conn.close()
