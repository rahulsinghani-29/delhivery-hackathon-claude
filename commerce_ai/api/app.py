"""FastAPI application setup with dependency injection for Delhivery Commerce AI."""

from __future__ import annotations

import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# Ensure the commerce_ai package root is on sys.path so that
# absolute imports like `from data.db import ...` work when running
# with `uvicorn api.app:app` from the commerce_ai directory.
_pkg_root = str(Path(__file__).resolve().parent.parent)
if _pkg_root not in sys.path:
    sys.path.insert(0, _pkg_root)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: init DB, load data, build graph, create all service instances."""
    from data.db import get_db, init_db
    from data.load_data import load_all

    # --- Database ---
    db_path = os.environ.get("COMMERCE_AI_DB", "commerce_ai.db")
    init_db(db_path)
    db = get_db(db_path)

    # Load sample data if the orders table is empty
    sample_dir = str(Path(__file__).resolve().parent.parent / "data" / "sample")
    row = db.execute("SELECT COUNT(*) FROM orders").fetchone()
    if row[0] == 0 and Path(sample_dir).exists():
        load_all(db, sample_dir)

    # --- AI layer ---
    from ai.scoring import RealizedCommerceScorer
    from ai.risk_reasoning import RiskReasoner
    from ai.next_best_action import NextBestActionPolicy
    from ai.insights import InsightGenerator
    from ai.knowledge_graph import RiskKnowledgeGraph

    scorer = RealizedCommerceScorer()
    risk_reasoner = RiskReasoner()
    nba_policy = NextBestActionPolicy()
    insight_gen = InsightGenerator()

    knowledge_graph = RiskKnowledgeGraph()
    # Build graph lazily on first use — skip on startup to avoid crash on large datasets
    try:
        knowledge_graph.build_graph(db)
    except Exception as e:
        print(f"Warning: Knowledge graph build failed ({e}), will retry on first use")

    # --- Communication layer ---
    from communication.whatsapp_client import WhatsAppClient
    from communication.voice_ai_client import GeminiVoiceClient
    from communication.issue_router import CommunicationIssueRouter

    whatsapp_client = WhatsAppClient()
    voice_ai_client = GeminiVoiceClient()
    issue_router = CommunicationIssueRouter()

    # --- Services ---
    from services.auto_cancel import AutoCancelService
    from services.impulse_detector import ImpulseDetector
    from services.order_engine import OrderActionEngineService
    from services.demand_advisor import DemandAdvisorService
    from services.action_executor import ActionExecutorService
    from services.guardrails import GuardrailsService
    from services.outbound_orchestrator import OutboundOrchestrator

    auto_cancel = AutoCancelService(db)
    impulse_detector = ImpulseDetector(db)

    order_engine = OrderActionEngineService(
        db=db,
        risk_reasoner=risk_reasoner,
        nba_policy=nba_policy,
        insight_gen=insight_gen,
        auto_cancel_service=auto_cancel,
        impulse_detector=impulse_detector,
        knowledge_graph=knowledge_graph,
    )

    demand_advisor = DemandAdvisorService(
        db=db, scorer=scorer, insight_gen=insight_gen
    )

    action_executor = ActionExecutorService(db)
    guardrails = GuardrailsService(db)

    outbound_orchestrator = OutboundOrchestrator(
        db=db,
        whatsapp_client=whatsapp_client,
        voice_ai_client=voice_ai_client,
        issue_router=issue_router,
    )

    # --- Store everything in app.state for dependency injection ---
    app.state.db = db
    app.state.scorer = scorer
    app.state.risk_reasoner = risk_reasoner
    app.state.nba_policy = nba_policy
    app.state.insight_gen = insight_gen
    app.state.knowledge_graph = knowledge_graph
    app.state.whatsapp_client = whatsapp_client
    app.state.voice_ai_client = voice_ai_client
    app.state.issue_router = issue_router
    app.state.auto_cancel = auto_cancel
    app.state.impulse_detector = impulse_detector
    app.state.order_engine = order_engine
    app.state.demand_advisor = demand_advisor
    app.state.action_executor = action_executor
    app.state.guardrails = guardrails
    app.state.outbound_orchestrator = outbound_orchestrator

    yield

    # Shutdown: close DB
    db.close()


# ---------------------------------------------------------------------------
# App creation
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Delhivery Commerce AI",
    version="0.1.0",
    lifespan=lifespan,
)

# CORS for React frontend (local dev + Vercel production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:3000",
        "https://*.vercel.app",
    ],
    allow_origin_regex=r"https://.*\.vercel\.app",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Task 6.4 — Error handling
# ---------------------------------------------------------------------------


@app.exception_handler(404)
async def not_found_handler(request: Request, exc):
    """Custom 404 for unknown resources."""
    detail = getattr(exc, "detail", "Resource not found")
    return JSONResponse(status_code=404, content={"error": detail})


@app.exception_handler(403)
async def forbidden_handler(request: Request, exc):
    """Custom 403 for permission denied."""
    detail = getattr(exc, "detail", "Permission denied")
    return JSONResponse(status_code=403, content={"error": detail})


@app.exception_handler(429)
async def rate_limit_handler(request: Request, exc):
    """Custom 429 for rate limit exceeded."""
    detail = getattr(exc, "detail", "Rate limit exceeded")
    return JSONResponse(status_code=429, content={"error": detail})


@app.exception_handler(422)
async def validation_error_handler(request: Request, exc):
    """Custom 422 for validation errors."""
    detail = getattr(exc, "detail", "Validation error")
    return JSONResponse(status_code=422, content={"error": detail})


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Catch-all for unhandled exceptions."""
    return JSONResponse(
        status_code=500,
        content={"error": f"Internal server error: {type(exc).__name__}"},
    )


# ---------------------------------------------------------------------------
# Include routes
# ---------------------------------------------------------------------------

from api.routes import router  # noqa: E402

app.include_router(router)


# Health check
@app.get("/health")
def health():
    return {"status": "ok"}
