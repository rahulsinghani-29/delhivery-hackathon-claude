# What's Next — Commerce AI Evaluation & Roadmap

## Current State Assessment

### What's strong

**Product thesis and framing.** The "decision layer on top of the existing RTO predictor" positioning is the right pitch. It doesn't reinvent the wheel — it builds the missing merchant intelligence layer. The thesis document is better than most hackathon submissions on its own.

**Real data pipeline.** 1.9M rows of actual Delhivery shipment data, properly profiled, cleaned (97% empty categories inferred, city duplicates normalized, payment modes cleaned), transformed, and loaded into Postgres with 4 materialized views. This isn't toy data.

**Knowledge graph.** `RiskKnowledgeGraph` builds a real NetworkX graph with 5 relationship types (merchant→warehouse, warehouse→cluster, customer→merchant, category→cluster, payment→cluster). The `get_risk_path` traversal produces explainable risk paths — genuinely useful for the "why is this order risky?" question.

**Interactive dashboard.** The Snapshot page with filters driving the demand heatmap, metrics, pie chart, and benchmark list simultaneously is visually compelling and demo-ready.

### What's weak

| Gap | Details | Impact |
|-----|---------|--------|
| LLM layer disabled | `RiskReasoner` and `InsightGenerator` both have `self._llm = None`. Every insight is a template string, not AI-generated. | For an AI hackathon, the "AI" in insights is just string formatting. |
| ML models not trained | XGBoost scorer and RandomForest NBA policy exist as code but no `.pkl` files. Models return fallback values (0.5 score, "no_action"). | The Order Action Engine pipeline runs but produces generic results. |
| Communication layer is 100% mock | WhatsApp returns fake message IDs. Voice client returns random outcomes. Gemini prompts are written but never called. | Not "agentic" — no real customer interaction. |
| Nothing runs autonomously | Everything is request-response. No background processing, no event-driven triggers, no autonomous decision-making. | It's a dashboard, not an agent. |
| Order Engine has a bug | `process_order()` references `risk_threshold` variable that's undefined in its scope (it's a param of `get_live_feed` but not passed through). | Live feed crashes if it hits the express upgrade path. |

---

## Priority Roadmap

### P0 — Fix the bug (30 minutes)

In `services/order_engine.py`, `process_order()` references `risk_threshold` on the express upgrade call but it's not a parameter. Fix:

```python
# In process_order(), add risk_threshold parameter or use cfg.RISK_THRESHOLD:
express_result = self.impulse_detector.upgrade_to_express(
    order,
    merchant_config,
    impulse_result,
    risk_threshold=cfg.RISK_THRESHOLD,          # was: undefined variable
    auto_cancel_threshold=auto_cancel_threshold,
)
```

### P1 — Enable LLM insights via Gemini (1-2 hours)

The Gemini API key is already in `.env`. The prompts are already written in `InsightGenerator` and `RiskReasoner`. The gap is just the LLM client initialization.

**What to do:**
1. In `ai/insights.py` and `ai/risk_reasoning.py`, replace the disabled Ollama client with a Gemini API call
2. Use `google-generativeai` Python SDK or a simple HTTP call to `gemini-2.0-flash`
3. Keep the template fallback for when the API is down

**Why it matters:** The benchmark insights on the Snapshot page would go from "Your COD general (mid-value) orders have a 100.0% RTO rate — 83.3pp higher than the 16.7% average" (template) to a genuinely AI-generated merchant-readable explanation. For an AI hackathon, this is the difference between "we built a dashboard" and "we built an AI product."

**Files to change:**
- `ai/insights.py` — `InsightGenerator.__init__()` and the `_build_*_prompt` methods
- `ai/risk_reasoning.py` — `RiskReasoner.__init__()` and `generate_risk_tag()`
- `requirements: google-generativeai` or use `langchain-google-genai`


### P2 — Train models on real data (1-2 hours)

The XGBoost scorer and RandomForest NBA policy are fully implemented but untrained. With 474k orders in Postgres, training would produce real predictions instead of 0.5 fallbacks.

**What to do:**
1. Adapt `scripts/train_models.py` to load from Postgres (it currently only supports SQLite or CSV)
2. Run training: scorer learns delivery probability by cohort, NBA learns best intervention by order context
3. Save `.pkl` files to `models/` directory
4. Restart backend — models auto-load on startup

**Why it matters:** The Orders page (`/orders`) shows a live feed with risk tags and next-best-action recommendations. Right now every order gets "no_action" with 0.0 confidence. With trained models, you'd see real recommendations like "verification (0.82 confidence)" or "cod_to_prepaid_outreach (0.71 confidence)."

**Column mapping issue:** The training script expects SQLite column names (`origin_node`, `destination_cluster`, `customer_ucid`). For Postgres, either:
- Add a Postgres loader to `train_models.py` that aliases columns, or
- Train from a CSV export: `\copy (SELECT ...) TO 'training_data.csv' WITH CSV HEADER`

### P3 — Make it agentic: background order processing (2-3 hours)

This is the gap between "dashboard" and "agent." Currently nothing happens unless a human clicks a button.

**What to build:**
A background async task that:
1. Polls for unprocessed orders (orders with `rto_score = 0.5` i.e. unscored)
2. Runs each through the Order Action Engine pipeline (score → risk path → NBA → auto-cancel check → impulse detection)
3. Logs interventions automatically
4. Triggers communications for high-risk orders (WhatsApp first, voice escalation after timeout)

**Implementation approach:**
```python
# In api/app.py lifespan, after service creation:
import asyncio

async def process_orders_loop():
    while True:
        unprocessed = db.execute(
            "SELECT * FROM orders WHERE rto_score = 0.5 LIMIT 50"
        ).fetchall()
        for order in unprocessed:
            result = order_engine.process_order(dict(order), merchant_config)
            # Update rto_score, log intervention, trigger comms
            ...
        await asyncio.sleep(30)  # Check every 30 seconds

asyncio.create_task(process_orders_loop())
```

**Why it matters:** This is what makes it an "agent" — the system observes, decides, and acts without human intervention. Even if the demo only processes 10 orders automatically, it shows the vision is real.

### P4 — Live Gemini voice calls (3-4 hours)

The voice prompts are already written and beautifully detailed (ported from Project Echo). The Gemini API key exists. The gap is connecting them.

**What to do:**
1. Use `google-generativeai` SDK to create a Gemini Live session
2. Send the phased prompts from `voice_ai_client.py` (they're already built)
3. Capture the transcript and run it through `extract_call_outcome()` (already implemented with regex parsing)
4. Update the order based on the outcome

**Demo scenario:** Show one live call where:
- System detects a high-RTO COD order from ZIBBRI B2C
- Gemini calls the "customer" (could be a team member's phone)
- Speaks with an Indian accent (the prompt enforces this)
- Collects the address or offers COD-to-prepaid conversion
- Extracts the outcome and updates the order

**Why it matters:** A live AI voice call that actually resolves a delivery issue is the kind of demo that wins hackathons. The prompts handle multilingual switching, accent consistency, and structured data extraction — all already coded.


### P5 — The full agentic loop (demo-day goal)

This is the "wow" moment — the complete autonomous workflow the thesis describes:

```
New high-RTO COD order detected
    ↓
Knowledge graph explains WHY it's risky
    ↓
LLM generates merchant-readable explanation
    ↓
NBA model recommends: "address_enrichment_outreach"
    ↓
System sends WhatsApp template to customer
    ↓
No response after 2 hours
    ↓
System escalates to Gemini voice call
    ↓
Gemini collects corrected address (Indian accent, multilingual)
    ↓
Regex parser extracts flat/floor/tower/landmark
    ↓
Order updated, intervention logged, merchant notified
```

Every piece of this pipeline exists in the codebase. The `OutboundOrchestrator` already has the WhatsApp→voice escalation logic with configurable `ESCALATION_WINDOW_HOURS`. The voice prompts handle address collection and COD-to-prepaid conversion. The transcript parser extracts structured data.

The only missing pieces are:
1. Real LLM calls (P1)
2. Trained models (P2)
3. Background processing trigger (P3)
4. Live Gemini voice connection (P4)

Even demoing this with a single order — walking through each step live — would be compelling.

---

## Effort vs Impact Matrix

```
                        HIGH IMPACT
                            │
         P1 (LLM insights) │  P5 (full loop)
              1-2 hrs       │     6-8 hrs
                            │
    ────────────────────────┼────────────────────────
                            │
         P0 (bug fix)       │  P3 (background agent)
              30 min        │     2-3 hrs
                            │
         P2 (train models)  │  P4 (voice calls)
              1-2 hrs       │     3-4 hrs
                            │
                        LOW EFFORT ──────── HIGH EFFORT
```

**Recommended order:** P0 → P1 → P2 → P3 → P4 → P5

P0+P1+P2 can be done in an afternoon and transform the demo from "dashboard with templates" to "AI product with real predictions and LLM-generated insights." P3 adds the agentic behavior. P4+P5 are the hackathon-winning demo moments.

---

## Files Reference

| Priority | Files to modify |
|----------|----------------|
| P0 | `services/order_engine.py` |
| P1 | `ai/insights.py`, `ai/risk_reasoning.py` |
| P2 | `scripts/train_models.py` |
| P3 | `api/app.py` (lifespan), new `services/background_processor.py` |
| P4 | `communication/voice_ai_client.py` |
| P5 | `services/outbound_orchestrator.py` (wire everything together) |
