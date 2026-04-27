"""FastAPI routes for Delhivery Commerce AI."""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

import config as cfg

logger = logging.getLogger(__name__)
router = APIRouter()


# ---------------------------------------------------------------------------
# Request / response schemas
# ---------------------------------------------------------------------------

class ExecuteActionRequest(BaseModel):
    order_id: str
    intervention_type: str
    confidence_score: Optional[float] = None


class TriggerCommunicationRequest(BaseModel):
    issue_type: str  # "address_enrichment" or "cod_to_prepaid"


class UpdatePermissionsRequest(BaseModel):
    intervention_type: str
    is_enabled: bool
    daily_cap: Optional[int] = None
    hourly_cap: Optional[int] = None
    auto_cancel_enabled: Optional[bool] = None
    auto_cancel_threshold: Optional[float] = None
    express_upgrade_enabled: Optional[bool] = None
    impulse_categories: Optional[list[str]] = None


# ---------------------------------------------------------------------------
# Helpers — dependency injection from app.state
# ---------------------------------------------------------------------------

def _get_db(request: Request):
    return request.app.state.db


def _get_order_engine(request: Request):
    return request.app.state.order_engine


def _get_demand_advisor(request: Request):
    return request.app.state.demand_advisor


def _get_action_executor(request: Request):
    return request.app.state.action_executor


def _get_guardrails(request: Request):
    return request.app.state.guardrails


def _get_outbound_orchestrator(request: Request):
    return request.app.state.outbound_orchestrator


def _get_issue_router(request: Request):
    return request.app.state.issue_router


# ---------------------------------------------------------------------------
# Merchant validation helper
# ---------------------------------------------------------------------------

def _validate_merchant(db, merchant_id: str) -> None:
    """Raise 404 if merchant doesn't exist."""
    row = db.execute(
        "SELECT 1 FROM merchants WHERE merchant_id = ?", (merchant_id,)
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"Merchant '{merchant_id}' not found")


# ---------------------------------------------------------------------------
# Task 6.2 — Core merchant endpoints
# ---------------------------------------------------------------------------

@router.get("/merchants/{merchant_id}/snapshot")
def get_merchant_snapshot(
    merchant_id: str,
    db=Depends(_get_db),
):
    """Merchant snapshot with warehouse nodes, distributions, and benchmark gaps."""
    _validate_merchant(db, merchant_id)
    from data.queries import get_merchant_snapshot as _snapshot
    result = _snapshot(db, merchant_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"Merchant '{merchant_id}' not found")
    return result


@router.get("/merchants/{merchant_id}/demand-suggestions")
def get_demand_suggestions(
    merchant_id: str,
    db=Depends(_get_db),
    demand_advisor=Depends(_get_demand_advisor),
):
    """Demand Mix Advisor suggestions for a merchant."""
    _validate_merchant(db, merchant_id)
    return demand_advisor.get_suggestions(merchant_id)


@router.get("/merchants/{merchant_id}/orders/live")
def get_live_orders(
    merchant_id: str,
    db=Depends(_get_db),
    order_engine=Depends(_get_order_engine),
):
    """Live order feed with risk tags, NBA, auto-cancel, and impulse/express status."""
    _validate_merchant(db, merchant_id)
    return order_engine.get_live_feed(merchant_id)


@router.post("/merchants/{merchant_id}/actions/execute")
def execute_action(
    merchant_id: str,
    body: ExecuteActionRequest,
    db=Depends(_get_db),
    action_executor=Depends(_get_action_executor),
    guardrails=Depends(_get_guardrails),
):
    """Execute an intervention on an order."""
    _validate_merchant(db, merchant_id)

    # Permission check
    if not guardrails.check_permission(merchant_id, body.intervention_type):
        raise HTTPException(
            status_code=403,
            detail=f"Permission denied for intervention type '{body.intervention_type}'",
        )

    # Rate limit check
    if not guardrails.check_rate_limit(merchant_id):
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded. Please try again later.",
        )

    result = action_executor.execute(
        merchant_id=merchant_id,
        order_id=body.order_id,
        intervention_type=body.intervention_type,
        confidence_score=body.confidence_score,
    )
    return result


@router.get("/merchants/{merchant_id}/actions/log")
def get_action_log(
    merchant_id: str,
    period_days: int = 30,
    db=Depends(_get_db),
):
    """Intervention history for a merchant."""
    _validate_merchant(db, merchant_id)
    from data.queries import get_intervention_history
    return get_intervention_history(db, merchant_id, period_days)


@router.get("/merchants/{merchant_id}/dashboard")
def get_dashboard(
    merchant_id: str,
    period_days: int = 30,
    db=Depends(_get_db),
):
    """Dashboard metrics and trends for a merchant."""
    _validate_merchant(db, merchant_id)
    from data.queries import (
        get_intervention_counts,
        get_merchant_snapshot as _snapshot,
        get_cohort_benchmarks,
    )
    snapshot = _snapshot(db, merchant_id)
    intervention_counts = get_intervention_counts(db, merchant_id, period_days)
    cohort_benchmarks = get_cohort_benchmarks(db, merchant_id)
    return {
        "merchant_id": merchant_id,
        "snapshot": snapshot,
        "intervention_counts": intervention_counts,
        "cohort_benchmarks": cohort_benchmarks,
        "period_days": period_days,
    }


@router.get("/merchants/{merchant_id}/permissions")
def get_permissions(
    merchant_id: str,
    db=Depends(_get_db),
):
    """Get merchant permission configuration."""
    _validate_merchant(db, merchant_id)
    from data.queries import get_merchant_permissions
    return get_merchant_permissions(db, merchant_id)


@router.put("/merchants/{merchant_id}/permissions")
def update_permissions(
    merchant_id: str,
    body: UpdatePermissionsRequest,
    db=Depends(_get_db),
):
    """Update merchant permission configuration.

    Validates that auto_cancel_threshold > risk_threshold (0.4) when setting auto-cancel config.
    """
    _validate_merchant(db, merchant_id)

    # Auto-cancel threshold validation
    if body.auto_cancel_threshold is not None and body.auto_cancel_threshold <= cfg.RISK_THRESHOLD:
        raise HTTPException(
            status_code=422,
            detail=(
                f"auto_cancel_threshold ({body.auto_cancel_threshold}) must be greater than "
                f"risk threshold ({cfg.RISK_THRESHOLD})"
            ),
        )

    # Upsert the permission row
    existing = db.execute(
        "SELECT 1 FROM merchant_permissions WHERE merchant_id = ? AND intervention_type = ?",
        (merchant_id, body.intervention_type),
    ).fetchone()

    if existing:
        updates = ["is_enabled = ?"]
        params: list = [body.is_enabled]
        if body.daily_cap is not None:
            updates.append("daily_cap = ?")
            params.append(body.daily_cap)
        if body.hourly_cap is not None:
            updates.append("hourly_cap = ?")
            params.append(body.hourly_cap)
        if body.auto_cancel_enabled is not None:
            updates.append("auto_cancel_enabled = ?")
            params.append(body.auto_cancel_enabled)
        if body.auto_cancel_threshold is not None:
            updates.append("auto_cancel_threshold = ?")
            params.append(body.auto_cancel_threshold)
        if body.express_upgrade_enabled is not None:
            updates.append("express_upgrade_enabled = ?")
            params.append(body.express_upgrade_enabled)
        if body.impulse_categories is not None:
            updates.append("impulse_categories = ?")
            params.append(",".join(body.impulse_categories))
        params.extend([merchant_id, body.intervention_type])
        db.execute(
            f"UPDATE merchant_permissions SET {', '.join(updates)} "
            f"WHERE merchant_id = ? AND intervention_type = ?",
            params,
        )
    else:
        db.execute(
            "INSERT INTO merchant_permissions "
            "(merchant_id, intervention_type, is_enabled, daily_cap, hourly_cap, "
            "auto_cancel_enabled, auto_cancel_threshold, express_upgrade_enabled, impulse_categories) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                merchant_id,
                body.intervention_type,
                body.is_enabled,
                body.daily_cap or cfg.DEFAULT_DAILY_CAP,
                body.hourly_cap or cfg.DEFAULT_HOURLY_CAP,
                body.auto_cancel_enabled or False,
                body.auto_cancel_threshold or cfg.AUTO_CANCEL_THRESHOLD,
                body.express_upgrade_enabled or False,
                ",".join(body.impulse_categories) if body.impulse_categories else "fashion,beauty",
            ),
        )
    db.commit()

    from data.queries import get_merchant_permissions
    return get_merchant_permissions(db, merchant_id)


# ---------------------------------------------------------------------------
# Task 6.3 — Communication endpoints
# ---------------------------------------------------------------------------

@router.get("/merchants/{merchant_id}/communications/status")
def get_merchant_communications(
    merchant_id: str,
    db=Depends(_get_db),
):
    """Communication status for all of a merchant's orders."""
    _validate_merchant(db, merchant_id)
    rows = db.execute(
        "SELECT * FROM communication_logs WHERE merchant_id = ? ORDER BY sent_at DESC",
        (merchant_id,),
    ).fetchall()
    return [dict(r) for r in rows]


@router.get("/orders/{order_id}/communications")
def get_order_communications(
    order_id: str,
    db=Depends(_get_db),
    orchestrator=Depends(_get_outbound_orchestrator),
):
    """Communication logs for a specific order."""
    # Verify order exists
    order = db.execute(
        "SELECT 1 FROM orders WHERE order_id = ?", (order_id,)
    ).fetchone()
    if order is None:
        raise HTTPException(status_code=404, detail=f"Order '{order_id}' not found")
    return orchestrator.get_communication_status(order_id)


@router.post("/merchants/{merchant_id}/orders/{order_id}/communications/trigger")
def trigger_communication(
    merchant_id: str,
    order_id: str,
    body: TriggerCommunicationRequest,
    db=Depends(_get_db),
    orchestrator=Depends(_get_outbound_orchestrator),
):
    """Manually trigger outbound communication for an order.

    The order must belong to the specified merchant — prevents cross-merchant
    communication triggers.
    """
    _validate_merchant(db, merchant_id)

    # Fetch the order and verify merchant ownership in one query
    row = db.execute(
        "SELECT * FROM orders WHERE order_id = ? AND merchant_id = ?",
        (order_id, merchant_id),
    ).fetchone()
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"Order '{order_id}' not found for merchant '{merchant_id}'",
        )

    order = dict(row)

    # Validate issue type
    valid_types = {"address_enrichment", "cod_to_prepaid"}
    if body.issue_type not in valid_types:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid issue_type '{body.issue_type}'. Must be one of: {', '.join(valid_types)}",
        )

    logger.info(
        "Communication trigger: merchant=%s order=%s issue=%s",
        merchant_id, order_id, body.issue_type,
    )
    result = orchestrator.trigger_outbound(order, body.issue_type)
    return result


# ---------------------------------------------------------------------------
# Merchant list + demand map (for Client Snapshot page)
# ---------------------------------------------------------------------------

@router.get("/merchants")
def list_merchants(db=Depends(_get_db)):
    """All merchants with order counts — used to populate the client dropdown."""
    from data.queries import get_all_merchants
    try:
        return get_all_merchants(db)
    except Exception as exc:
        import traceback
        logger.error("list_merchants failed: %s\n%s", exc, traceback.format_exc())
        raise


@router.get("/merchants/{merchant_id}/demand-map")
def get_demand_map(
    merchant_id: str,
    category: Optional[str] = None,
    price_band: Optional[str] = None,
    payment_mode: Optional[str] = None,
    db=Depends(_get_db),
):
    """Destination-city order volume + RTO rate — used for the India demand heatmap.

    Optional query params filter by cohort dimensions (category, price_band, payment_mode).
    When no filters are provided, returns the cached full demand map.
    """
    _validate_merchant(db, merchant_id)
    if category or price_band or payment_mode:
        from data.queries import get_demand_map_filtered
        return get_demand_map_filtered(db, merchant_id, category, price_band, payment_mode)
    from data.queries import get_demand_map as _demand_map
    return _demand_map(db, merchant_id)
