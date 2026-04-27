# Changelog — `pg-etl` branch

## 2026-04-27 — Interactive Filtered Dashboard

### Summary

Made the Snapshot page fully interactive — filters for category, price band, and payment mode
now drive the demand heatmap, metric cards, and category pie chart in addition to the
benchmark list. Added a new filtered demand-map backend endpoint. Fixed several frontend
rendering issues.

### Changes

| File | What Changed |
|------|-------------|
| `commerce_ai/data/queries.py` | Added `get_demand_map_filtered()` — queries the orders table with optional category/price_band/payment_mode WHERE clauses, returns city-level stats matching the filter. |
| `commerce_ai/api/routes.py` | Extended `GET /merchants/{id}/demand-map` with optional query params `?category=&price_band=&payment_mode=`. When any filter is present, calls the new filtered query instead of the cached materialized view. |
| `commerce_ai/frontend/src/lib/api.ts` | `fetchDemandMap()` now accepts an optional filters object and appends query params. |
| `commerce_ai/frontend/src/pages/Snapshot.tsx` | Major rework of the Snapshot page: |
| | — Filters moved above metrics/charts so they visually control the whole page |
| | — Demand heatmap re-fetches from backend when any filter changes |
| | — Category pie chart recomputes from filtered benchmark gaps |
| | — Metric cards now show 4 values: Destination Cities, Filtered/Total Orders, Avg RTO Rate, Cohorts Shown |
| | — Labels update to indicate filtered state ("Filtered Orders", "Filtered Avg RTO") |
| | — Fixed default merchant ID (`M001` → empty, waits for merchant list) |
| | — Fixed `loading` state stuck on `true` when no merchant selected |
| | — Fixed city coordinate lookup (lowercases input to match the lookup table) |
| | — Downgraded `react-leaflet` from v5 to v4.2.1 (v5 requires React 19, project uses React 18) |

### New API Behavior

```
GET /merchants/{id}/demand-map                          → full cached demand map (unchanged)
GET /merchants/{id}/demand-map?payment_mode=COD         → COD orders only (1,002 cities for ZIBBRI)
GET /merchants/{id}/demand-map?category=general         → general category only (2,324 cities)
GET /merchants/{id}/demand-map?category=health&payment_mode=Prepaid → combined filter
```

---


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
| `commerce_ai/data/queries.py` | Removed all `sqlite3` type hints. Added Postgres-specific query paths using materialized views. Fixed column name differences (`destination_cluster` → `destination_city`, `origin_node` → `origin_state`, `customer_ucid` → `buyer_id`). |
| `commerce_ai/api/app.py` | Updated lifespan to detect Postgres via `DATABASE_URL`, skip sample data loading in Postgres mode. |
| `commerce_ai/ai/knowledge_graph.py` | Removed `sqlite3` import. Column names adapt to backend. Replaced `ORDER BY ROWID` with plain `LIMIT` for Postgres. |
| `commerce_ai/services/*.py` (6 files) | Removed `sqlite3` imports and type hints from action_executor, auto_cancel, demand_advisor, guardrails, impulse_detector, order_engine. |
| `commerce_ai/.env` / `.env.example` | Added `DATABASE_URL` documentation. |

### ETL Pipeline Details

**Input:** `rto-data-hackathon-result-1-2026-04-27-13-59-08.csv` (222 MB, 1.9M rows)

**Data profile:**
- 474,408 order rows + 1,429,938 line-item rows
- 2,873 merchants, 233k buyers
- RTO: 17.7% overall — COD 34.8% vs Prepaid 1.2%

**Transforms:** city normalization (11 groups), category inference (8 categories from item names), payment mode cleanup, price band derivation, amount capping at 50k, synthetic order IDs.

**Materialized views:** `mv_merchant_cohort_stats` (115k rows), `mv_peer_benchmarks` (7.3k), `mv_merchant_summary` (2.9k), `mv_demand_map` (64k).

**Validation:** 31/31 checks passed. Total ETL time: 53 seconds.
