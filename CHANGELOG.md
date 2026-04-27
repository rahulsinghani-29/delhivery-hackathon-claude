# Changelog — `pg-etl` branch

## 2026-04-27 — Postgres Migration & ETL Pipeline

### Summary

Migrated the Commerce AI backend from SQLite to Postgres, built a containerized ETL pipeline
to transform and load 1.9M rows of raw Delhivery shipment data, and made the entire app
dual-backend (Postgres or SQLite) via a single `DATABASE_URL` environment variable.

---

### New Files

| File | Purpose |
|------|---------|
| `commerce_ai/postgres_schema.sql` | Full Postgres schema — dimension tables, orders, line items, operational tables, indexes |
| `commerce_ai/Dockerfile.etl` | Python 3.11 container that runs the ETL + validation pipeline |
| `commerce_ai/docker-compose.etl.yml` | Postgres 16 + ETL container, mounts the raw CSV |
| `commerce_ai/scripts/load_postgres.py` | 3-pass ETL: merchants → 474k orders → 1.43M line items → materialized views |
| `commerce_ai/scripts/validate_postgres.py` | 31 automated validation checks (row counts, nulls, enums, RTO benchmarks, referential integrity, city normalization) |

### Modified Files

| File | What Changed |
|------|-------------|
| `commerce_ai/data/db.py` | Rewrote to support both SQLite and Postgres. Added `PgConnectionWrapper` (converts `?` → `%s`, returns dict rows, auto-rollback on errors). Added `is_postgres()` function. |
| `commerce_ai/data/queries.py` | Removed all `sqlite3` type hints. Added Postgres-specific query paths using materialized views for `get_merchant_snapshot`, `get_cohort_benchmarks`, `get_peer_benchmarks`, `get_all_merchants`, `get_demand_map`. Fixed column name differences (`destination_cluster` → `destination_city`, `origin_node` → `origin_state`, `customer_ucid` → `buyer_id`). |
| `commerce_ai/api/app.py` | Updated lifespan to detect Postgres via `DATABASE_URL`, skip sample data loading in Postgres mode, log which backend is active. |
| `commerce_ai/ai/knowledge_graph.py` | Removed `sqlite3` import. Column names now adapt to backend (`origin_state`/`destination_city` for Postgres, `origin_node`/`destination_cluster` for SQLite). Replaced `ORDER BY ROWID` with plain `LIMIT` for Postgres. |
| `commerce_ai/services/action_executor.py` | Removed `sqlite3` import and type hint. |
| `commerce_ai/services/auto_cancel.py` | Removed `sqlite3` import and type hint. |
| `commerce_ai/services/demand_advisor.py` | Removed `sqlite3` import and type hint. |
| `commerce_ai/services/guardrails.py` | Removed `sqlite3` import and type hint. |
| `commerce_ai/services/impulse_detector.py` | Removed `sqlite3` import and type hint. |
| `commerce_ai/services/order_engine.py` | Removed `sqlite3` import and type hint. |
| `commerce_ai/.env` | Added `DATABASE_URL` documentation. |
| `commerce_ai/.env.example` | Added `DATABASE_URL` documentation. |

---

### ETL Pipeline Details

**Input:** `rto-data-hackathon-result-1-2026-04-27-13-59-08.csv` (222 MB, 1.9M rows)

**Data profile discovered:**
- 474,408 order-level rows (have buyer, origin, destination, final_status)
- 1,429,938 line-item rows (product names + amounts only, no buyer/status)
- 2,873 unique merchants, 233k unique buyers
- Payment split: 50.7% COD / 49.3% Prepaid
- RTO rate: 17.7% overall — COD 34.8% vs Prepaid 1.2%
- `category_name` 97% empty — inferred from `line_item_name` via keyword matching
- `destination_city` had duplicates (Bangalore/BANGALORE/Bengaluru) — normalized

**Transforms applied:**
- City normalization (11 city alias groups)
- Category inference from line_item_name (8 categories: fashion, beauty, jewellery, food, electronics, health, home, footwear)
- Payment mode from `hudi_pt_tag` (cleaner than `payment_method`)
- Price band derivation from `order_amt` (low/mid/high/premium)
- Address quality from `was_adfix_corrected` + `buyer_rto_history_pct`
- Order amount capped at 50,000 (P99.5 outlier removal)
- Synthetic order IDs generated (CSV has no waybill/order_id column)

**Materialized views created:**
| View | Rows | Purpose |
|------|------|---------|
| `mv_merchant_cohort_stats` | 115,455 | Per-merchant cohort performance (replaces SQLite ROWID sampling) |
| `mv_peer_benchmarks` | 7,345 | Network-wide peer comparison for Demand Mix Advisor |
| `mv_merchant_summary` | 2,873 | Merchant list with aggregated stats |
| `mv_demand_map` | 63,792 | Destination heatmap data |

**Validation results:** 31/31 checks passed, 0 failures, 0 warnings.

**Total ETL time:** 53 seconds (containerized).

---

### How to Use

**Postgres mode (new):**
```bash
# Start Postgres with data (first time)
cd commerce_ai
podman-compose -f docker-compose.etl.yml up

# Run the app against Postgres
export DATABASE_URL="postgresql://commerce:commerce@localhost:5432/commerce_ai"
python -m uvicorn api.app:app --host 0.0.0.0 --port 8000
```

**SQLite mode (unchanged):**
```bash
cd commerce_ai
python -m uvicorn api.app:app --host 0.0.0.0 --port 8000
```

**Frontend:**
```bash
cd commerce_ai/frontend
npm run dev
# Opens at http://localhost:5173
```
