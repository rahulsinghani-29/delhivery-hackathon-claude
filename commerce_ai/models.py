"""Pydantic data models, enums, and constants for Delhivery Commerce AI."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Set

from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Task 2.1 — Enums
# ---------------------------------------------------------------------------

class PaymentMode(str, Enum):
    COD = "COD"
    PREPAID = "prepaid"


class DeliveryOutcome(str, Enum):
    DELIVERED = "delivered"
    RTO = "rto"
    PENDING = "pending"


class InterventionType(str, Enum):
    VERIFICATION = "verification"
    CANCELLATION = "cancellation"
    MASKED_CALLING = "masked_calling"
    COD_TO_PREPAID = "cod_to_prepaid"
    PREMIUM_COURIER = "premium_courier"
    MERCHANT_CONFIRMATION = "merchant_confirmation"
    ADDRESS_ENRICHMENT_OUTREACH = "address_enrichment_outreach"
    COD_TO_PREPAID_OUTREACH = "cod_to_prepaid_outreach"
    AUTO_CANCEL = "auto_cancel"
    EXPRESS_UPGRADE = "express_upgrade"
    NO_ACTION = "no_action"


class ActionOwner(str, Enum):
    DELHIVERY = "delhivery"
    MERCHANT = "merchant"


class CommunicationIssueType(str, Enum):
    ADDRESS_ENRICHMENT = "address_enrichment"
    COD_TO_PREPAID = "cod_to_prepaid"


class CommunicationChannel(str, Enum):
    WHATSAPP = "whatsapp"
    VOICE = "voice"


class CommunicationStatus(str, Enum):
    SENT = "sent"
    DELIVERED = "delivered"
    RESPONDED = "responded"
    FAILED = "failed"
    NO_RESPONSE = "no_response"
    CALL_INITIATED = "call_initiated"
    CALL_COMPLETED = "call_completed"
    CALL_FAILED = "call_failed"
    CALL_NO_ANSWER = "call_no_answer"


class CommunicationResolution(str, Enum):
    ADDRESS_UPDATED = "address_updated"
    PAYMENT_CONVERTED = "payment_converted"
    NO_RESOLUTION = "no_resolution"


class ImpulseSignal(str, Enum):
    LATE_NIGHT = "late_night"
    COD_PAYMENT = "cod_payment"
    FIRST_TIME_BUYER = "first_time_buyer"
    HIGH_IMPULSE_CATEGORY = "high_impulse_category"


# ---------------------------------------------------------------------------
# Task 2.2 — Core Models
# ---------------------------------------------------------------------------

class CohortKey(BaseModel):
    category: str
    price_band: str
    payment_mode: PaymentMode
    origin_node: str
    destination_cluster: str


class Order(BaseModel):
    order_id: str
    merchant_id: str
    customer_ucid: str
    category: str
    price_band: str
    payment_mode: PaymentMode
    origin_node: str
    destination_pincode: str
    destination_cluster: str
    address_quality: float  # 0-1
    rto_score: float  # 0-1
    delivery_outcome: DeliveryOutcome
    shipping_mode: str = "surface"
    created_at: datetime


class EnrichedOrder(BaseModel):
    order: Order
    historical_rto_rate: float
    historical_sample_size: int
    peer_avg_rto_rate: float


class RiskTag(BaseModel):
    tag_label: str
    explanation: str


class ActionRecommendation(BaseModel):
    intervention_type: InterventionType
    confidence_score: float  # 0-1
    explanation: str


class ProcessedOrder(BaseModel):
    order: Order
    enrichment: EnrichedOrder
    risk_tag: Optional[RiskTag] = None
    next_best_action: Optional[ActionRecommendation] = None
    nl_explanation: Optional[str] = None
    auto_cancel_result: Optional[AutoCancelResult] = None
    impulse_result: Optional[ImpulseResult] = None
    express_upgrade_result: Optional[ExpressUpgradeResult] = None


class ScoredCohort(BaseModel):
    cohort_key: CohortKey
    realized_commerce_score: float
    is_low_confidence: bool
    order_count: int


class PeerBenchmark(BaseModel):
    cohort_key: CohortKey
    merchant_score: float
    peer_avg_score: float
    peer_sample_size: int
    confidence_interval_width: float
    gap: float


class DemandSuggestion(BaseModel):
    cohort_dimension: str
    recommended_value: str
    expected_score_improvement: float
    peer_benchmark: PeerBenchmark
    nl_explanation: str


class MerchantSnapshot(BaseModel):
    merchant_id: str
    warehouse_nodes: List[dict]
    category_distribution: dict
    price_band_distribution: dict
    payment_mode_distribution: dict
    benchmark_gaps: List[PeerBenchmark]


# ---------------------------------------------------------------------------
# Task 2.3 — Operational Models
# ---------------------------------------------------------------------------

class InterventionLog(BaseModel):
    intervention_id: str
    order_id: str
    merchant_id: str
    intervention_type: InterventionType
    action_owner: ActionOwner
    initiated_by: str
    confidence_score: Optional[float] = None
    outcome: Optional[str] = None
    executed_at: datetime
    completed_at: Optional[datetime] = None


class ExecutionResult(BaseModel):
    success: bool
    intervention_log_id: str
    error_message: Optional[str] = None


class RateLimitStatus(BaseModel):
    daily_used: int
    daily_cap: int
    hourly_used: int
    hourly_cap: int
    is_within_limits: bool


class MerchantPermissions(BaseModel):
    merchant_id: str
    permissions: Dict[str, bool] = {}
    daily_cap: int = 500
    hourly_cap: int = 100
    auto_cancel_enabled: bool = False
    auto_cancel_threshold: float = 0.9
    express_upgrade_enabled: bool = False
    impulse_categories: List[str] = ["fashion", "beauty"]


# ---------------------------------------------------------------------------
# Task 2.4 — Communication Models
# ---------------------------------------------------------------------------

class WhatsAppSendResult(BaseModel):
    message_id: str
    status: str
    error_message: Optional[str] = None


class WhatsAppResponseStatus(BaseModel):
    responded: bool
    response_content: Optional[str] = None
    responded_at: Optional[datetime] = None


class VoiceCallContext(BaseModel):
    order_id: str
    customer_ucid: str
    issue_type: CommunicationIssueType
    current_address: Optional[str] = None
    payment_link_url: Optional[str] = None
    order_summary: dict


class VoiceCallResult(BaseModel):
    call_id: str
    status: str
    resolution: Optional[CommunicationResolution] = None
    transcript_summary: Optional[str] = None


class VoiceCallStatus(BaseModel):
    call_id: str
    status: str
    duration_seconds: Optional[int] = None
    resolution: Optional[CommunicationResolution] = None


class CommunicationLog(BaseModel):
    communication_id: str
    order_id: str
    merchant_id: str
    customer_ucid: str
    issue_type: CommunicationIssueType
    channel: CommunicationChannel
    template_id: Optional[str] = None
    message_id: Optional[str] = None
    status: CommunicationStatus
    customer_response: Optional[str] = None
    resolution: Optional[CommunicationResolution] = None
    sent_at: datetime
    responded_at: Optional[datetime] = None
    escalation_scheduled_at: Optional[datetime] = None


# ---------------------------------------------------------------------------
# Task 2.5 — Auto-Cancel and Impulse Models
# ---------------------------------------------------------------------------

class AutoCancelResult(BaseModel):
    cancelled: bool
    reason: str
    order_id: str
    merchant_id: str
    rto_score: float
    threshold: float
    cancelled_at: Optional[datetime] = None


class ImpulseResult(BaseModel):
    is_impulsive: bool
    matched_signals: List[ImpulseSignal]
    signal_count: int
    order_id: str
    rto_score: float


class ExpressUpgradeResult(BaseModel):
    upgraded: bool
    reason: str
    order_id: str
    merchant_id: str
    rto_score: float
    matched_signals: List[ImpulseSignal]
    original_shipping_mode: str
    new_shipping_mode: Optional[str] = None
    upgraded_at: Optional[datetime] = None


# ---------------------------------------------------------------------------
# Task 2.6 — Action Categorization Constants
# ---------------------------------------------------------------------------

DELHIVERY_EXECUTABLE: Set[InterventionType] = {
    InterventionType.VERIFICATION,
    InterventionType.CANCELLATION,
    InterventionType.MASKED_CALLING,
    InterventionType.PREMIUM_COURIER,
    InterventionType.ADDRESS_ENRICHMENT_OUTREACH,
    InterventionType.COD_TO_PREPAID_OUTREACH,
    InterventionType.AUTO_CANCEL,
    InterventionType.EXPRESS_UPGRADE,
}

MERCHANT_OWNED: Set[InterventionType] = {
    InterventionType.MERCHANT_CONFIRMATION,
}
