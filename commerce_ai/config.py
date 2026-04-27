"""Centralised configuration — all tuneable thresholds and settings live here.

Override any value via environment variables (e.g. RISK_THRESHOLD=0.35).
"""

from __future__ import annotations

import os


def _float(key: str, default: float) -> float:
    val = os.environ.get(key)
    return float(val) if val is not None else default


def _int(key: str, default: int) -> int:
    val = os.environ.get(key)
    return int(val) if val is not None else default


def _str(key: str, default: str) -> str:
    return os.environ.get(key, default)


def _bool(key: str, default: bool) -> bool:
    val = os.environ.get(key)
    if val is None:
        return default
    return val.lower() in ("1", "true", "yes")


# ---------------------------------------------------------------------------
# RTO risk thresholds
# ---------------------------------------------------------------------------
RISK_THRESHOLD: float = _float("RISK_THRESHOLD", 0.4)
AUTO_CANCEL_THRESHOLD: float = _float("AUTO_CANCEL_THRESHOLD", 0.9)
ADDRESS_QUALITY_THRESHOLD: float = _float("ADDRESS_QUALITY_THRESHOLD", 0.5)
CLUSTER_RTO_THRESHOLD: float = _float("CLUSTER_RTO_THRESHOLD", 0.3)
MIN_CONFIDENCE: float = _float("MIN_CONFIDENCE", 0.6)

# Minimum order count in a graph edge before we trust its weights
GRAPH_MIN_EDGE_ORDERS: int = _int("GRAPH_MIN_EDGE_ORDERS", 10)

# ---------------------------------------------------------------------------
# Default merchant caps (used when no DB row exists)
# ---------------------------------------------------------------------------
DEFAULT_DAILY_CAP: int = _int("DEFAULT_DAILY_CAP", 500)
DEFAULT_HOURLY_CAP: int = _int("DEFAULT_HOURLY_CAP", 100)
DEFAULT_CUSTOMER_DAILY_COMM_CAP: int = _int("DEFAULT_CUSTOMER_DAILY_COMM_CAP", 2)

# ---------------------------------------------------------------------------
# Demand advisor gates
# ---------------------------------------------------------------------------
DEMAND_MIN_PEER_SAMPLE: int = _int("DEMAND_MIN_PEER_SAMPLE", 200)
DEMAND_MAX_CI_WIDTH: float = _float("DEMAND_MAX_CI_WIDTH", 0.15)

# ---------------------------------------------------------------------------
# Outbound orchestration
# ---------------------------------------------------------------------------
ESCALATION_WINDOW_HOURS: float = _float("ESCALATION_WINDOW_HOURS", 2.0)

# ---------------------------------------------------------------------------
# LLM / Ollama
# ---------------------------------------------------------------------------
OLLAMA_HOST: str = _str("OLLAMA_HOST", "http://localhost:11434")
OLLAMA_MODEL: str = _str("OLLAMA_MODEL", "llama3")

# ---------------------------------------------------------------------------
# Query cache TTL (seconds)
# ---------------------------------------------------------------------------
QUERY_CACHE_TTL: int = _int("QUERY_CACHE_TTL", 60)

# ---------------------------------------------------------------------------
# Model paths
# ---------------------------------------------------------------------------
SCORER_MODEL_PATH: str = _str("SCORER_MODEL_PATH", "models/scorer.pkl")
NBA_MODEL_PATH: str = _str("NBA_MODEL_PATH", "models/nba.pkl")
