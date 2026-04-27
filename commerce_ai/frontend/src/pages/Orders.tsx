import { useEffect, useState } from 'react'
import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  useReactTable,
  type SortingState,
} from '@tanstack/react-table'
import { fetchLiveOrders } from '../lib/api'
import StatusTag, { type StatusVariant } from '../components/StatusTag'

const MERCHANT_ID = 'M-8A25EB3E'  // ZIBBRI B2C - 391k real orders

/* ------------------------------------------------------------------ */
/* Types matching the API ProcessedOrder shape                        */
/* ------------------------------------------------------------------ */

interface Order {
  order_id: string
  rto_score: number
  category: string
  payment_mode: string
  shipping_mode: string
}

interface RiskTag {
  tag_label: string
  explanation: string
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
  risk_tag: RiskTag | null
  next_best_action: ActionRecommendation | null
  auto_cancel_result: AutoCancelResult | null
  impulse_result: ImpulseResult | null
  express_upgrade_result: ExpressUpgradeResult | null
}

/* ------------------------------------------------------------------ */
/* Helpers                                                             */
/* ------------------------------------------------------------------ */

function getRiskVariant(order: ProcessedOrder): StatusVariant | null {
  if (order.auto_cancel_result?.cancelled) return 'auto-cancelled'
  if (!order.risk_tag) return null
  const rto = order.order.rto_score
  if (rto > 0.7) return 'high-risk'
  if (rto > 0.3) return 'medium-risk'
  return 'low-risk'
}

function rtoColor(score: number): string {
  if (score > 0.7) return 'text-danger'
  if (score > 0.3) return 'text-warning'
  return 'text-success'
}

function actionLabel(order: ProcessedOrder): string {
  if (order.auto_cancel_result?.cancelled) return '—'
  if (order.express_upgrade_result?.upgraded) return 'Express ▲'
  if (!order.next_best_action) return '—'
  const t = order.next_best_action.intervention_type
  const labels: Record<string, string> = {
    verification: 'Verify',
    cancellation: 'Cancel',
    masked_calling: 'Masked Call',
    cod_to_prepaid: 'COD→Prepaid',
    premium_courier: 'Premium',
    merchant_confirmation: 'Confirm',
    address_enrichment_outreach: 'WA: Address',
    cod_to_prepaid_outreach: 'WA: Payment',
    auto_cancel: 'Auto-Cancel',
    express_upgrade: 'Express ▲',
    no_action: '—',
  }
  return labels[t] ?? t
}

function commsVariant(order: ProcessedOrder): StatusVariant | null {
  const nba = order.next_best_action?.intervention_type
  if (nba === 'address_enrichment_outreach' || nba === 'cod_to_prepaid_outreach') return 'wa-sent'
  return null
}

/* ------------------------------------------------------------------ */
/* Column definitions                                                  */
/* ------------------------------------------------------------------ */

const columnHelper = createColumnHelper<ProcessedOrder>()

const columns = [
  columnHelper.accessor((row) => row.order.order_id, {
    id: 'order_id',
    header: 'Order ID',
    cell: (info) => (
      <span className="font-mono text-xs truncate max-w-[120px] inline-block">
        {info.getValue()}
      </span>
    ),
  }),
  columnHelper.accessor((row) => row.order.rto_score, {
    id: 'rto_score',
    header: 'RTO Score',
    cell: (info) => {
      const v = info.getValue()
      return <span className={`font-mono ${rtoColor(v ?? 0)}`}>{v != null ? v.toFixed(2) : '—'}</span>
    },
  }),
  columnHelper.display({
    id: 'risk_tag',
    header: 'Risk Tag',
    cell: ({ row }) => {
      const variant = getRiskVariant(row.original)
      if (!variant) return <span className="text-gray-400">—</span>
      return <StatusTag variant={variant} />
    },
  }),
  columnHelper.display({
    id: 'action',
    header: 'Action',
    cell: ({ row }) => {
      const label = actionLabel(row.original)
      return <span className="text-sm">{label}</span>
    },
  }),
  columnHelper.display({
    id: 'impulse',
    header: 'Impulse',
    cell: ({ row }) => {
      if (row.original.impulse_result?.is_impulsive) {
        return <StatusTag variant="impulsive" />
      }
      return <span className="text-gray-400">—</span>
    },
  }),
  columnHelper.display({
    id: 'comms',
    header: 'Comms',
    cell: ({ row }) => {
      const v = commsVariant(row.original)
      if (!v) return <span className="text-gray-400">—</span>
      return <StatusTag variant={v} />
    },
  }),
]

/* ------------------------------------------------------------------ */
/* Component                                                           */
/* ------------------------------------------------------------------ */

export default function Orders() {
  const [data, setData] = useState<ProcessedOrder[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [sorting, setSorting] = useState<SortingState>([
    { id: 'rto_score', desc: true },
  ])

  useEffect(() => {
    fetchLiveOrders(MERCHANT_ID)
      .then((d) => setData(d as ProcessedOrder[]))
      .catch((e) => setError(e.message ?? 'Failed to load orders'))
      .finally(() => setLoading(false))
  }, [])

  const table = useReactTable({
    data,
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
  })

  if (loading) return <p className="text-sm text-gray-500">Loading...</p>
  if (error) return <p className="text-sm text-danger">{error}</p>

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-[28px] font-bold leading-tight text-gray-900">
          Live Order Feed
        </h1>
        <span className="text-xs text-gray-500">
          Sort: RTO Score {sorting[0]?.desc ? '▼' : '▲'}
        </span>
      </div>

      <div className="bg-white border border-gray-300 rounded-lg overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            {table.getHeaderGroups().map((hg) => (
              <tr key={hg.id} className="bg-gray-100 text-xs uppercase text-gray-700">
                {hg.headers.map((header) => (
                  <th
                    key={header.id}
                    className={`text-left px-4 py-2 ${
                      header.column.getCanSort() ? 'cursor-pointer select-none' : ''
                    }`}
                    onClick={header.column.getToggleSortingHandler()}
                  >
                    <span className="flex items-center gap-1">
                      {flexRender(header.column.columnDef.header, header.getContext())}
                      {header.column.getIsSorted() === 'asc' && '▲'}
                      {header.column.getIsSorted() === 'desc' && '▼'}
                    </span>
                  </th>
                ))}
              </tr>
            ))}
          </thead>
          <tbody>
            {table.getRowModel().rows.map((row, i) => (
              <tr
                key={row.id}
                className={`hover:bg-gray-100 transition-colors ${
                  i % 2 === 0 ? 'bg-white' : 'bg-gray-50'
                }`}
              >
                {row.getVisibleCells().map((cell) => (
                  <td key={cell.id} className="px-4 py-2">
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </td>
                ))}
              </tr>
            ))}
            {data.length === 0 && (
              <tr>
                <td colSpan={columns.length} className="px-4 py-6 text-center text-gray-500">
                  No live orders.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
