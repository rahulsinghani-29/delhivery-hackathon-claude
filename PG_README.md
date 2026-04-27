# Delhivery Commerce AI — Postgres Setup Guide

This guide covers how to run the full Commerce AI stack with Postgres as the database backend.

## Prerequisites

| Tool | Version | Check | Install |
|------|---------|-------|---------|
| Python | 3.11+ | `python3 --version` | [python.org](https://www.python.org/) |
| Node.js | 18+ | `node --version` | `brew install node` |
| Podman or Docker | any | `podman --version` or `docker --version` | `brew install podman` |
| podman-compose or docker-compose | any | `podman-compose --version` | `brew install podman-compose` |
| libomp (macOS only) | any | `ls /opt/homebrew/opt/libomp/lib/libomp.dylib` | `brew install libomp` |

> If you're on macOS with Apple Silicon and using podman, make sure the podman machine is running:
> `podman machine start`

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌──────────────┐
│  React Frontend │────▶│  FastAPI Backend  │────▶│  Postgres 16 │
│  localhost:5173  │     │  localhost:8000   │     │  localhost:5432│
│  (Vite + Tailwind)    │  (Uvicorn)        │     │  (Container)  │
└─────────────────┘     └──────────────────┘     └──────────────┘
        │                        │
        │  /api proxy            │  DATABASE_URL env var
        └────────────────────────┘
```

## Step-by-Step Setup

### Step 1: Start Postgres and Load Data

The ETL pipeline runs inside containers. It starts Postgres, applies the schema, transforms the
raw CSV (1.9M rows → 474k orders + 1.43M line items), and runs 31 validation checks.

```bash
cd commerce_ai

# If using podman:
podman-compose -f docker-compose.etl.yml up

# If using docker:
docker-compose -f docker-compose.etl.yml up
```

**What happens:**
1. Postgres 16 container starts on port 5432
2. ETL container waits for Postgres to be ready
3. Schema is applied (tables, indexes)
4. CSV is loaded in 3 passes: merchants → orders → line items
5. 4 materialized views are created (cohort stats, peer benchmarks, merchant summary, demand map)
6. 31 validation checks run automatically
7. ETL container exits, Postgres stays running

**Expected output (last lines):**
```
etl  | VALIDATION SUMMARY
etl  |   Passed:   31
etl  |   Failed:   0
etl  |   Warnings: 0
```

**Time:** ~60 seconds total.

> **Note:** The `docker-compose.etl.yml` file mounts the CSV from a hardcoded path. If your CSV
> is in a different location, edit the `volumes` section in `docker-compose.etl.yml`:
> ```yaml
> volumes:
>   - /your/path/to/data.csv:/data/input.csv:ro
> ```

### Step 2: Install Python Dependencies

```bash
cd commerce_ai
pip install fastapi uvicorn pydantic xgboost scikit-learn langchain langchain-community \
    networkx pandas numpy psycopg2-binary
```

On macOS with Apple Silicon, XGBoost needs OpenMP:
```bash
brew install libomp
```

### Step 3: Start the Backend

```bash
cd commerce_ai
export DATABASE_URL="postgresql://commerce:commerce@localhost:5432/commerce_ai"
python3 -m uvicorn api.app:app --host 0.0.0.0 --port 8000
```

**Expected output:**
```
Using Postgres backend (DATABASE_URL)
INFO:     Application startup complete.
INFO:     Uvicorn running on http://0.0.0.0:8000
```

**Verify it works:**
```bash
curl http://localhost:8000/health
# {"status":"ok"}

curl http://localhost:8000/merchants | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'{len(d)} merchants')"
# 200 merchants
```

### Step 4: Install Frontend Dependencies

```bash
cd commerce_ai/frontend
npm install --legacy-peer-deps
```

### Step 5: Start the Frontend

```bash
cd commerce_ai/frontend
npm run dev
```

**Expected output:**
```
VITE v5.x.x  ready in XXX ms
➜  Local:   http://localhost:5173/
```

### Step 6: Open the App

Open http://localhost:5173 in your browser.

Pages:
- `/snapshot` — Merchant dashboard with category distribution, demand heatmap, peer benchmark insights. Filters for category, price band, and payment mode drive all widgets (metrics, pie chart, map, benchmark list).
- `/advisor` — Demand mix optimization suggestions
- `/orders` — Live order feed with risk tags and next-best-action recommendations
- `/actions` — Intervention history and execution console

The default merchant is ZIBBRI B2C (M-8A25EB3E) with ~65k orders.

---

## API Endpoints

### Merchant Operations
| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/merchants` | List all merchants with order counts (top 200) |
| `GET` | `/merchants/{id}/snapshot` | Dashboard data: distributions, warehouse nodes, benchmark gaps |
| `GET` | `/merchants/{id}/demand-suggestions` | Demand Mix Advisor suggestions |
| `GET` | `/merchants/{id}/orders/live` | Live order feed with risk tags and NBA |
| `GET` | `/merchants/{id}/dashboard` | Metrics and trends |
| `GET` | `/merchants/{id}/permissions` | Permission configuration |
| `PUT` | `/merchants/{id}/permissions` | Update permissions |

### Demand Map (supports filtering)
| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/merchants/{id}/demand-map` | Full demand heatmap (cached via materialized view) |
| `GET` | `/merchants/{id}/demand-map?category=fashion` | Filtered by category |
| `GET` | `/merchants/{id}/demand-map?payment_mode=COD` | Filtered by payment mode |
| `GET` | `/merchants/{id}/demand-map?category=general&price_band=mid&payment_mode=COD` | Combined filters |

Query params are all optional. When any filter is present, the endpoint queries the orders table directly instead of the materialized view.

### Order Actions
| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/merchants/{id}/actions/execute` | Execute an intervention on an order |
| `GET` | `/merchants/{id}/actions/log` | Intervention history |

### Communications
| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/merchants/{id}/communications/status` | Communication logs for a merchant |
| `GET` | `/orders/{id}/communications` | Communication logs for an order |
| `POST` | `/merchants/{mid}/orders/{oid}/communications/trigger` | Trigger outbound communication |

### System
| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Health check |


---

## Switching Between SQLite and Postgres

The app supports both backends via a single environment variable.

| Mode | How to activate | Data source |
|------|----------------|-------------|
| **Postgres** | `export DATABASE_URL="postgresql://commerce:commerce@localhost:5432/commerce_ai"` | ETL-loaded production data |
| **SQLite** | Don't set `DATABASE_URL` (or unset it) | Sample CSV data auto-loaded on first start |

When `DATABASE_URL` is set:
- Heavy queries use materialized views (sub-millisecond instead of full table scans)
- Knowledge graph uses `origin_state`/`destination_city` columns
- Sample data loading is skipped (data comes from ETL)

When `DATABASE_URL` is not set:
- Falls back to SQLite at the path in `COMMERCE_AI_DB` env var (default: `commerce_ai.db`)
- Sample data from `data/sample/*.csv` is auto-loaded if the DB is empty
- Uses `ROWID`-based sampling for large queries

---

## Database Details

### Connection Info (Postgres container)

| Field | Value |
|-------|-------|
| Host | `localhost` |
| Port | `5432` |
| Database | `commerce_ai` |
| User | `commerce` |
| Password | `commerce` |
| DSN | `postgresql://commerce:commerce@localhost:5432/commerce_ai` |

### Tables

| Table | Rows | Description |
|-------|------|-------------|
| `merchants` | 2,873 | Merchant dimension table |
| `orders` | 474,408 | Core fact table with cohort dimensions |
| `order_line_items` | 1,429,938 | Product-level detail (item names, amounts) |
| `interventions` | 0 (runtime) | Logged actions taken on orders |
| `communication_logs` | 0 (runtime) | WhatsApp/voice communication logs |
| `merchant_permissions` | 2,873 | Per-merchant intervention caps and thresholds |
| `cities` | — | City normalization lookup (canonical names) |
| `categories` | — | Category dimension (inferred from item names) |

### Orders Table Schema

This is the core fact table. All cohort-based queries and the demand map filter against it.

```
orders
├── order_id            TEXT PRIMARY KEY    -- ORD-{sha256[:12]}
├── merchant_id         TEXT NOT NULL       -- FK → merchants
├── buyer_id            TEXT                -- customer identifier
├── buyer_rto_history   REAL               -- 0-1 historical RTO rate
├── category            TEXT                -- inferred: fashion, beauty, health, food, electronics, home, jewellery, footwear, general
├── price_band          TEXT NOT NULL       -- low (<300), mid (300-999), high (1000-2999), premium (3000+)
├── payment_mode        TEXT NOT NULL       -- COD or Prepaid
├── origin_city         TEXT                -- normalized city name
├── origin_state        TEXT                -- state name
├── destination_city    TEXT                -- normalized city name (Title Case)
├── destination_pincode TEXT                -- 6-digit pincode
├── address_quality     REAL               -- 0-1, derived from adfix correction + buyer RTO history
├── was_adfix_corrected BOOLEAN            -- address was auto-corrected
├── manifest_latency    INTEGER            -- days between order and manifest
├── order_amount        NUMERIC(12,2)      -- capped at 50,000
├── delivery_outcome    TEXT NOT NULL       -- delivered, rto, in_transit, cancelled, pending
├── rto_score           REAL DEFAULT 0.5   -- ML-predicted risk (filled by scorer at runtime)
├── shipping_mode       TEXT DEFAULT 'standard'
└── created_at          TIMESTAMPTZ
```

**Key indexes:**
- `(merchant_id)` — merchant lookups
- `(merchant_id, category, price_band, payment_mode)` — cohort queries
- `(merchant_id, destination_city)` — demand map
- `(delivery_outcome)` — RTO aggregations
- `(rto_score) WHERE rto_score > 0.4` — partial index for risky orders

### Materialized Views

These replace the SQLite sampling hacks and make dashboard queries instant:

| View | Rows | Replaces |
|------|------|----------|
| `mv_merchant_cohort_stats` | 115,455 | `ORDER BY ROWID LIMIT 1000000` sampling in snapshot |
| `mv_peer_benchmarks` | 7,345 | Cross-merchant aggregation for Demand Mix Advisor |
| `mv_merchant_summary` | 2,873 | `ORDER BY ROWID LIMIT 2000000` sampling in merchant list |
| `mv_demand_map` | 63,792 | Per-merchant destination heatmap aggregation |

To refresh after data changes:
```sql
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_merchant_cohort_stats;
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_peer_benchmarks;
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_merchant_summary;
REFRESH MATERIALIZED VIEW mv_demand_map;
```

---

## Data Transforms Applied During ETL

The raw CSV has significant data quality issues. The ETL pipeline applies these fixes:

| Issue | Fix |
|-------|-----|
| 1.43M rows are line items, not orders | Separated by checking `final_status` presence |
| `category_name` is 97% empty | Inferred from `line_item_name` via keyword matching (8 categories) |
| City name duplicates (Bangalore/BANGALORE/Bengaluru) | Normalized via alias map (11 city groups) |
| `payment_method` is messy (cod/Cash/cash/COD) | Uses `hudi_pt_tag` instead (clean COD/Pre-paid) |
| `mapped_dest_state` is 95% "Other" | Ignored; uses `destination_city` instead |
| `order_amt` outliers (max 1.2 billion) | Capped at 50,000 (P99.5) |
| No `order_id` or `waybill` column | Generated deterministic IDs: `ORD-{sha256[:12]}` |
| No `rto_score` in raw data | Defaults to 0.5; ML scorer fills it at runtime |

---

## Troubleshooting

### "XGBoost Library could not be loaded" (macOS)
```bash
brew install libomp
```

### "psycopg2 not installed"
```bash
pip install psycopg2-binary
```

### Postgres container won't start
```bash
# Check if port 5432 is already in use
lsof -i :5432

# Reset everything
podman-compose -f docker-compose.etl.yml down -v
podman-compose -f docker-compose.etl.yml up
```

### Frontend shows blank map on Snapshot page
Hard-refresh with Cmd+Shift+R. The Leaflet map tiles need a clean cache after dependency changes.

### Backend starts but knowledge graph fails
Check the error message. Common cause: column name mismatch if running against a different
schema version. The knowledge graph adapts column names based on `is_postgres()` — make sure
`DATABASE_URL` is set correctly.

### ETL validation fails
The validation script checks 31 conditions. If any fail, the error message tells you exactly
what's wrong. Common issues:
- CSV path is wrong (check the volume mount in `docker-compose.etl.yml`)
- Postgres ran out of disk space (check `podman system df`)

---

## For AI Agents (Kiro/Claude)

If you're an AI agent working on this codebase, here's what you need to know:

### Quick Start Commands
```bash
# 1. Start Postgres (if not already running)
cd commerce_ai
podman-compose -f docker-compose.etl.yml up postgres -d

# 2. Start backend
DATABASE_URL="postgresql://commerce:commerce@localhost:5432/commerce_ai" \
  python3 -m uvicorn api.app:app --host 0.0.0.0 --port 8000

# 3. Start frontend
cd commerce_ai/frontend && npm run dev
```

### Key Files to Know

| File | What it does |
|------|-------------|
| `data/db.py` | Database abstraction layer. `is_postgres()` checks `DATABASE_URL`. `PgConnectionWrapper` converts `?` → `%s` placeholders. |
| `data/queries.py` | All SQL queries. Uses `is_postgres()` to branch between materialized views (Postgres) and sampling (SQLite). Includes `get_demand_map_filtered()` for cohort-filtered city stats. |
| `api/app.py` | FastAPI app. Lifespan handler creates all services with dependency injection. |
| `api/routes.py` | All REST endpoints. `GET /merchants/{id}/demand-map` accepts optional `?category=&price_band=&payment_mode=` query params. |
| `postgres_schema.sql` | Postgres schema (tables + indexes, no materialized views — those are created by the ETL). |
| `scripts/load_postgres.py` | ETL pipeline. 3-pass CSV processing with city normalization, category inference, amount capping. |
| `scripts/validate_postgres.py` | 31-check validation suite. Run after any data load. |
| `docker-compose.etl.yml` | Postgres 16 + ETL container definition. |
| `frontend/src/pages/Snapshot.tsx` | Main dashboard page. Filters drive metrics, pie chart, demand map, and benchmark list. |
| `frontend/src/lib/api.ts` | Frontend API client. `fetchDemandMap()` accepts optional filter params. |

### Column Name Differences

The Postgres schema uses different column names than the SQLite schema:

| SQLite | Postgres | Used in |
|--------|----------|---------|
| `origin_node` | `origin_state` or `origin_city` | orders, knowledge graph, queries |
| `destination_cluster` | `destination_city` | orders, knowledge graph, queries, demand map |
| `customer_ucid` | `buyer_id` | orders, customer lookup |

All runtime code handles this via `is_postgres()` checks in `queries.py` and `knowledge_graph.py`.

### Testing a Change

```bash
# Verify backend API
curl http://localhost:8000/health
curl http://localhost:8000/merchants/M-8A25EB3E/snapshot | python3 -m json.tool | head -20

# Verify frontend proxy
curl http://localhost:5173/api/merchants | python3 -m json.tool | head -5

# Run validation against Postgres
DATABASE_URL="postgresql://commerce:commerce@localhost:5432/commerce_ai" \
  python3 -m scripts.validate_postgres
```
