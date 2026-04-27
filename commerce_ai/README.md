# Delhivery Commerce AI

Merchant-facing intelligence layer built on Delhivery's RTO Predictor. Scores every live order for return risk, explains *why* via a knowledge graph, and auto-executes interventions (address verification, COD→prepaid nudges, express upgrades, auto-cancellation) — all surfaced through a React dashboard and WhatsApp/Gemini voice outreach.

## Architecture

```
┌─────────────┐
│  React 18   │  ← TypeScript + Tailwind + Recharts
└──────┬──────┘
       │ REST
┌──────▼──────┐
│  API Layer  │  ← FastAPI, routes, error handling
├─────────────┤
│  Services   │  ← Order engine, auto-cancel, impulse detection, guardrails, outbound orchestrator
├─────────────┤
│  AI Layer   │  ← XGBoost scoring, knowledge graph (NetworkX), risk reasoning, insights (LangChain)
├─────────────┤
│ Communication│ ← WhatsApp client, Gemini voice AI, issue router
├─────────────┤
│  Data Layer │  ← SQLite, sample data, queries
└─────────────┘
```

## Key Features

- **Knowledge graph explainability** — NetworkX graph links orders → pincodes → categories → payment modes to explain risk
- **Auto-cancel** — Automatically cancels orders above configurable RTO threshold with guardrails
- **Impulse detection** — Flags impulsive buying patterns for express upgrade intervention
- **WhatsApp + Gemini voice outreach** — Multi-channel customer communication for address verification and payment nudges
- **Demand mix advisor** — Peer-benchmarked suggestions to shift order mix toward lower-RTO cohorts

## Quick Start

```bash
# Backend
cd commerce_ai
pip install -e ".[dev]"
python -m data.generate_sample_data
uvicorn api.app:app --reload --port 8000

# Frontend
cd frontend
npm install
npm run dev
```

## Tech Stack

Python 3.11+ · FastAPI · SQLite · XGBoost · NetworkX · LangChain · Gemini · React 18 · TypeScript · Tailwind CSS

## Test

```bash
cd commerce_ai
pytest
```

## API Docs

http://localhost:8000/docs (FastAPI auto-generated)
