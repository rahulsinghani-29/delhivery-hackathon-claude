import { useEffect, useState } from 'react'
import { fetchLiveOrders, executeAction } from '../lib/api'
import StatusTag, { type StatusVariant } from '../components/StatusTag'
import Button from '../components/Button'

const MERCHANT_ID = 'M-8A25EB3E'  // ZIBBRI B2C - 391k real orders

/* ------------------------------------------------------------------ */
/* Types (same ProcessedOrder shape as Orders page)                    */
/* ------------------------------------------------------------------ */

interface Order {
  order_id: string
  rto_score: number
  category: string
  payment_mode: string
  shipping_mode: string
}

interface ActionRecommendation {
  intervention_type: string
  confidence_score: number
  explanation: string
}

interface AutoCancelResult {
  cancelled: boolean
  reason: string
}

interface ImpulseResult {
  is_impulsive: boolean
  matched_signals: string[]
  signal_count: number
}

interface ExpressUpgradeResult {
  upgraded: boolean
  reason: string
}

interface ProcessedOrder {
  order: Order
  risk_tag: { tag_label: string; explanation: string } | null
  next_best_action: ActionRecommendation | null
  auto_cancel_result: AutoCancelResult | null
  impulse_result: ImpulseResult | null
  express_upgrade_result: ExpressUpgradeResult | null
}

/* ------------------------------------------------------------------ */
/* Constants — Delhivery-executable intervention types                  */
/* ------------------------------------------------------------------ */

const DELHIVERY_TYPES = new Set([
  'verification',
  'cancellation',
  'masked_calling',
  'premium_courier',
  'address_enrichment_outreach',
  'cod_to_prepaid_outreach',
  'auto_cancel',
  'express_upgrade',
])

/* ------------------------------------------------------------------ */
/* Helpers                                                             */
/* ------------------------------------------------------------------ */

function interventionLabel(type: string): string {
  const labels: Record<string, string> = {
    verification: 'Verify',
    cancellation: 'Cancel',
    masked_calling: 'Masked Call',
    cod_to_prepaid: 'COD→Prepaid',
    premium_courier: 'Premium',
    merchant_confirmation: 'Confirm',
    address_enrichment_outreach: 'Address WA',
    cod_to_prepaid_outreach: 'Payment WA',
    auto_cancel: 'Auto-Cancel',
    express_upgrade: 'Express ▲',
    no_action: 'No Action',
  }
  return labels[type] ?? type
}

function statusVariant(order: ProcessedOrder): StatusVariant {
  if (order.auto_cancel_result?.cancelled) return 'auto-cancelled'
  if (order.express_upgrade_result?.upgraded) return 'resolved'
  return 'wa-sent'
}

function statusLabel(order: ProcessedOrder): string {
  if (order.auto_cancel_result?.cancelled) return 'Done ✓'
  if (order.express_upgrade_result?.upgraded) return 'Done ✓'
  return 'Pending'
}

/* ------------------------------------------------------------------ */
/* Component                                                           */
/* ------------------------------------------------------------------ */

export default function Actions() {
  const [orders, setOrders] = useState<ProcessedOrder[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [executing, setExecuting] = useState<Set<string>>(new Set())

  useEffect(() => {
    fetchLiveOrders(MERCHANT_ID)
      .then((d) => setOrders(d as ProcessedOrder[]))
      .catch((e) => setError(e.message ?? 'Failed to load actions'))
      .finally(() => setLoading(false))
  }, [])

  const handleExecute = async (orderId: string, interventionType: string) => {
    setExecuting((prev) => new Set(prev).add(orderId))
    try {
      await executeAction(MERCHANT_ID, orderId, interventionType)
    } catch {
      // silently handle — real app would show toast
    } finally {
      setExecuting((prev) => {
        const next = new Set(prev)
        next.delete(orderId)
        return next
      })
    }
  }

  /* Split orders into Delhivery-executable vs Merchant-owned */
  const delhiveryActions: ProcessedOrder[] = []
  const merchantActions: ProcessedOrder[] = []

  for (const o of orders) {
    const nba = o.next_best_action?.intervention_type
    if (o.auto_cancel_result?.cancelled || o.express_upgrade_result?.upgraded) {
      delhiveryActions.push(o)
    } else if (nba && DELHIVERY_TYPES.has(nba)) {
      delhiveryActions.push(o)
    } else if (nba === 'merchant_confirmation') {
      merchantActions.push(o)
    }
  }

  /* Summary counts */
  const executed = delhiveryActions.filter(
    (o) => o.auto_cancel_result?.cancelled || o.express_upgrade_result?.upgraded,
  ).length
  const queued = delhiveryActions.length - executed + merchantActions.length
  const rate = Math.round((executed / Math.max(1, 1)) * 12) // placeholder rate

  if (loading) return <p className="text-sm text-gray-500">Loading...</p>
  if (error) return <p className="text-sm text-danger">{error}</p>

  return (
    <div className="space-y-6">
      <h1 className="text-[28px] font-bold leading-tight text-gray-900">
        Action Console
      </h1>

      {/* Two-column grid */}
      <div className="grid grid-cols-2 gap-6">
        {/* Delhivery-Executable */}
        <div>
          <h2 className="text-base font-semibold text-gray-900 mb-3">
            Delhivery-Executable
          </h2>
          <div className="space-y-2">
            {delhiveryActions.length === 0 && (
              <p className="text-sm text-gray-500">No actions.</p>
            )}
            {delhiveryActions.map((o) => {
              const nba = o.next_best_action?.intervention_type ?? 'auto_cancel'
              const label = o.auto_cancel_result?.cancelled
                ? 'Auto-Cancel'
                : o.express_upgrade_result?.upgraded
                  ? 'Express ▲'
                  : interventionLabel(nba)
              const isDone =
                o.auto_cancel_result?.cancelled || o.express_upgrade_result?.upgraded

              return (
                <div
                  key={o.order.order_id}
                  className="flex items-center justify-between bg-white border border-gray-300 rounded-lg px-4 py-2"
                >
                  <div className="flex items-center gap-3">
                    <span className="font-mono text-xs">{o.order.order_id}</span>
                    <span className="text-sm text-gray-700">{label}</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <StatusTag
                      variant={statusVariant(o)}
                      label={statusLabel(o)}
                    />
                    {!isDone && (
                      <Button
                        variant="primary"
                        className="text-xs h-7 px-3"
                        disabled={executing.has(o.order.order_id)}
                        onClick={() => handleExecute(o.order.order_id, nba)}
                      >
                        {executing.has(o.order.order_id) ? '...' : 'Execute'}
                      </Button>
                    )}
                  </div>
                </div>
              )
            })}
          </div>
        </div>

        {/* Merchant-Owned */}
        <div>
          <h2 className="text-base font-semibold text-gray-900 mb-3">
            Merchant-Owned
          </h2>
          <div className="space-y-2">
            {merchantActions.length === 0 && (
              <p className="text-sm text-gray-500">No actions.</p>
            )}
            {merchantActions.map((o) => (
              <div
                key={o.order.order_id}
                className="flex items-center justify-between bg-white border border-gray-300 rounded-lg px-4 py-2"
              >
                <div className="flex items-center gap-3">
                  <span className="font-mono text-xs">{o.order.order_id}</span>
                  <span className="text-sm text-gray-700">Confirm</span>
                </div>
                <StatusTag variant="wa-sent" label="Pending" />
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Intervention Summary Bar */}
      <div className="bg-gray-100 border border-gray-300 rounded-lg px-5 py-3">
        <p className="text-xs uppercase tracking-wide text-gray-500 mb-1">
          Intervention Summary
        </p>
        <p className="text-sm text-gray-900">
          Today: <span className="font-mono font-semibold">{executed}</span> executed
          {' | '}
          <span className="font-mono font-semibold">{queued}</span> queued
          {' | '}
          Rate: <span className="font-mono font-semibold">{rate}/hr</span> (cap: 100)
        </p>
      </div>
    </div>
  )
}
