# Session Notes — Claude (April 26-27 2026)

## Context
All changes live in `Hackathon_RTO/claude/` — Kiro's originals in `Hackathon_RTO/commerce_ai/` and `Hackathon_RTO/project-echo/` are UNTOUCHED.

---

## What Was Built (All 14 Improvements Applied)

### Commerce AI (`claude/commerce_ai/`)
| # | Fix | Files |
|---|-----|-------|
| 1 | Open `/communications/trigger` endpoint — merchant ownership check added | `api/routes.py` |
| 2 | Silent `except: pass` → `logger.exception()` everywhere | `services/outbound_orchestrator.py`, `services/auto_cancel.py`, `services/order_engine.py` |
| 3 | Model training pipeline | `scripts/train_models.py` |
| 5 | All thresholds centralized with env var overrides | `config.py` |
| 6 | Knowledge graph NaN guards + minimum edge order count | `ai/knowledge_graph.py` |
| 7 | Rate limit off-by-one fixed (`<` → `<=`) + per-customer daily cap | `data/queries.py`, `services/outbound_orchestrator.py` |
| 8 | 60s TTL in-process cache for dashboard/snapshot queries | `data/queries.py` |

### Project Echo (`claude/project-echo/`)
| # | Fix | Files |
|---|-----|-------|
| 9 | Gemini API key server-side (auth guard on `/api/session`) | `src/app/api/session/route.ts` |
| 10 | `useGeminiVoice.ts` split into 4 focused files | `src/hooks/audio-utils.ts`, `useAudioCapture.ts`, `useAudioPlayback.ts`, `useGeminiVoice.ts` |
| 11 | Dead backup file deleted | `LiveVoiceDemo.backup.tsx` gone |
| 12 | Zustand store persisted to localStorage (agents only) | `src/lib/store.ts` |
| 13 | WebSocket reconnect with session resumption + status UI | `src/hooks/useGeminiVoice.ts` |
| 14 | NextAuth restricted to `@delhivery.com` (env override: `ALLOWED_DOMAIN`) | `src/lib/auth.ts` |

---

## Deployment State

### Services Running Locally
| Service | URL | Log |
|---------|-----|-----|
| Commerce AI backend (FastAPI) | http://localhost:8000 | `/tmp/commerce-ai.log` |
| Commerce AI frontend (Vite) | http://localhost:5174 | `/tmp/commerce-ai-fe.log` |
| Project Echo frontend (Next.js) | http://localhost:3000 | `/tmp/project-echo.log` |

**To restart:**
```bash
# Backend
cd ".../Hackathon_RTO/claude/commerce_ai"
python3 -m uvicorn api.app:app --host 0.0.0.0 --port 8000 > /tmp/commerce-ai.log 2>&1 &

# Commerce AI frontend
cd ".../Hackathon_RTO/claude/commerce_ai/frontend"
npm run dev > /tmp/commerce-ai-fe.log 2>&1 &

# Project Echo frontend
cd ".../Hackathon_RTO/claude/project-echo"
npm run dev > /tmp/project-echo.log 2>&1 &
```

### Vercel Deployment
- Commerce AI frontend deployed to Vercel (project: `delhivery-commerce`)
- **The frontend will show UI but API calls fail** until the backend has a public URL
- Next step: expose backend via ngrok (needs authtoken) or deploy backend to Railway

**To make API calls work on Vercel:**
1. Get ngrok authtoken from https://dashboard.ngrok.com → run `ngrok authtoken <token>`
2. `ngrok http 8000` → copy the `https://xxxxx.ngrok-free.app` URL
3. `cd .../claude/commerce_ai/frontend && vercel env add VITE_API_URL production` → paste ngrok URL
4. `vercel --prod` → redeploy

---

## Data Loading

### CSV File
- Path: `Hackathon_RTO/claude/e019c113-850e-48c8-8741-65b6dde2ca3d.csv`
- Rows: 17.7M raw → 10,030,747 unique waybills → 39,030 merchants

### Load Script (NEEDS TO BE RE-RUN — was running when session ended)
```bash
cd ".../Hackathon_RTO/claude/commerce_ai"
python3 -m scripts.load_production_data \
  --csv ".../Hackathon_RTO/claude/e019c113-850e-48c8-8741-65b6dde2ca3d.csv" \
  --db commerce_ai.db
```
**Status**: May or may not have completed — verify with:
```python
import sqlite3
db = sqlite3.connect("commerce_ai.db")
print(db.execute("SELECT COUNT(*) FROM orders").fetchone())  # Should be ~10M
print(db.execute("SELECT COUNT(*) FROM merchants").fetchone())  # Should be ~39k
```
If count is 5000, sample data is loaded, not real data. Re-run the load script.

### Top Merchant for Demo
- **ZIBBRI B2C** → merchant_id: `M-8A25EB3E` → 391,611 orders
- All pages already updated to use this merchant_id (was `M001`)

### After Loading — Retrain Models
```bash
cd ".../Hackathon_RTO/claude/commerce_ai"
python3 -m scripts.train_models \
  --db commerce_ai.db
# Saves to models/scorer.pkl and models/nba.pkl
```
Then restart the backend to pick up new models.

---

## Environment Files Created

### `claude/project-echo/.env.local`
```
GEMINI_API_KEY=AIzaSyCuHjTnu6NUoq1j89UvixljglJhIzqpNyU
AUTH_SECRET=CqPMFRGvpjV5UJF3nsi7lVCO0+Yuod4f5jG2GK2c8bQ=
GOOGLE_CLIENT_ID=local-dev
GOOGLE_CLIENT_SECRET=local-dev
ALLOWED_DOMAIN=   (empty = allow all emails for local dev)
```

### `claude/commerce_ai/.env`
```
GEMINI_API_KEY=AIzaSyCuHjTnu6NUoq1j89UvixljglJhIzqpNyU
COMMERCE_AI_DB=commerce_ai.db
PORT=8000
```

---

## Key Decisions Made
1. Auth is bypassed in dev for Project Echo (`NODE_ENV !== 'development'` check in `/api/session`)
2. `ALLOWED_DOMAIN=` (empty) → any email allowed for local dev login
3. Commerce AI frontend uses MERCHANT_ID `M-8A25EB3E` (ZIBBRI B2C) — largest merchant in real data
4. `VITE_API_URL` env var controls backend URL for Vercel deployment (not set = uses Vite proxy in dev, fails in prod)

---

## What Still Needs To Be Done
- [ ] **Verify data load** — check orders count is ~10M, not 5000
- [ ] **Retrain models** on real data after load confirmed
- [ ] **Backend public URL** — ngrok authtoken OR Railway deployment for Vercel → backend connectivity
- [ ] **DELHIVERY_HQ_TOKEN** — if needed for AWB lookup in Project Echo demo
