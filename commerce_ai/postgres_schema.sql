-- =============================================================================
-- Delhivery Commerce AI — Postgres Schema
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- =============================================================================
-- 1. DIMENSION TABLES
-- =============================================================================

CREATE TABLE IF NOT EXISTS merchants (
    merchant_id     TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    client_type     TEXT,
    total_orders    INTEGER DEFAULT 0,
    rto_rate        REAL DEFAULT 0.0,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS cities (
    city_id         SERIAL PRIMARY KEY,
    raw_name        TEXT NOT NULL,
    canonical_name  TEXT NOT NULL,
    state           TEXT,
    UNIQUE(canonical_name, state)
);

CREATE TABLE IF NOT EXISTS categories (
    category_id     SERIAL PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    parent_category TEXT
);

-- =============================================================================
-- 2. CORE FACT TABLE — orders
-- =============================================================================

CREATE TABLE IF NOT EXISTS orders (
    order_id            TEXT PRIMARY KEY,
    merchant_id         TEXT NOT NULL REFERENCES merchants(merchant_id),
    buyer_id            TEXT,
    buyer_rto_history   REAL DEFAULT 0.0,
    category            TEXT,
    price_band          TEXT NOT NULL,
    payment_mode        TEXT NOT NULL,
    origin_city         TEXT,
    origin_state        TEXT,
    destination_city    TEXT,
    destination_state   TEXT,
    destination_pincode TEXT,
    address_quality     REAL DEFAULT 0.5,
    was_adfix_corrected BOOLEAN DEFAULT FALSE,
    manifest_latency    INTEGER DEFAULT 0,
    order_amount        NUMERIC(12,2) NOT NULL,
    delivery_outcome    TEXT NOT NULL,
    rto_score           REAL DEFAULT 0.5,
    shipping_mode       TEXT DEFAULT 'standard',
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_merchant       ON orders (merchant_id);
CREATE INDEX IF NOT EXISTS idx_orders_outcome        ON orders (delivery_outcome);
CREATE INDEX IF NOT EXISTS idx_orders_cohort         ON orders (merchant_id, category, price_band, payment_mode);
CREATE INDEX IF NOT EXISTS idx_orders_destination    ON orders (merchant_id, destination_city);
CREATE INDEX IF NOT EXISTS idx_orders_buyer          ON orders (buyer_id);
CREATE INDEX IF NOT EXISTS idx_orders_payment_mode   ON orders (payment_mode);
CREATE INDEX IF NOT EXISTS idx_orders_rto_score      ON orders (rto_score) WHERE rto_score > 0.4;

-- =============================================================================
-- 3. LINE ITEMS
-- =============================================================================

CREATE TABLE IF NOT EXISTS order_line_items (
    line_item_id    SERIAL PRIMARY KEY,
    order_id        TEXT,
    merchant_id     TEXT NOT NULL REFERENCES merchants(merchant_id),
    item_name       TEXT NOT NULL,
    item_amount     NUMERIC(12,2),
    inferred_category TEXT
);
CREATE INDEX IF NOT EXISTS idx_line_items_order    ON order_line_items (order_id);
CREATE INDEX IF NOT EXISTS idx_line_items_merchant ON order_line_items (merchant_id);

-- =============================================================================
-- 4. OPERATIONAL TABLES
-- =============================================================================

CREATE TABLE IF NOT EXISTS interventions (
    intervention_id     UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id            TEXT NOT NULL REFERENCES orders(order_id),
    merchant_id         TEXT NOT NULL REFERENCES merchants(merchant_id),
    intervention_type   TEXT NOT NULL,
    action_owner        TEXT NOT NULL DEFAULT 'delhivery',
    initiated_by        TEXT NOT NULL DEFAULT 'system',
    confidence_score    REAL,
    outcome             TEXT,
    executed_at         TIMESTAMPTZ DEFAULT NOW(),
    completed_at        TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_interventions_merchant ON interventions (merchant_id, executed_at);
CREATE INDEX IF NOT EXISTS idx_interventions_order    ON interventions (order_id);

CREATE TABLE IF NOT EXISTS communication_logs (
    communication_id    UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id            TEXT NOT NULL,
    merchant_id         TEXT NOT NULL,
    buyer_id            TEXT,
    issue_type          TEXT NOT NULL,
    channel             TEXT NOT NULL,
    status              TEXT NOT NULL,
    resolution          TEXT,
    sent_at             TIMESTAMPTZ DEFAULT NOW(),
    responded_at        TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_comms_merchant ON communication_logs (merchant_id);
CREATE INDEX IF NOT EXISTS idx_comms_order    ON communication_logs (order_id);

CREATE TABLE IF NOT EXISTS merchant_permissions (
    merchant_id             TEXT NOT NULL REFERENCES merchants(merchant_id),
    intervention_type       TEXT NOT NULL DEFAULT 'all',
    is_enabled              BOOLEAN DEFAULT TRUE,
    daily_cap               INTEGER DEFAULT 500,
    hourly_cap              INTEGER DEFAULT 100,
    auto_cancel_enabled     BOOLEAN DEFAULT FALSE,
    auto_cancel_threshold   REAL DEFAULT 0.9,
    express_upgrade_enabled BOOLEAN DEFAULT FALSE,
    impulse_categories      TEXT DEFAULT 'fashion,beauty',
    PRIMARY KEY (merchant_id, intervention_type)
);
