import { useEffect, useState, useMemo } from 'react'
import { MapContainer, TileLayer, CircleMarker, Tooltip as MapTooltip } from 'react-leaflet'
import 'leaflet/dist/leaflet.css'
import { PieChart, Pie, Cell, Tooltip, Legend, ResponsiveContainer } from 'recharts'
import { fetchMerchants, fetchSnapshot, fetchDemandMap } from '../lib/api'
import MetricCard from '../components/MetricCard'
import Button from '../components/Button'

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface Merchant {
  merchant_id: string
  name: string
  order_count: number
}

interface BenchmarkGap {
  category: string
  price_band: string
  payment_mode: string
  order_count: number
  merchant_rto_rate: number
  peer_rto_rate: number
  peer_merchant_count?: number
  peer_total_orders: number
  rto_gap: number
}

interface SnapshotData {
  merchant_id: string
  name: string | null
  warehouse_nodes: Record<string, unknown>[]
  category_distribution: Record<string, number>
  price_band_distribution: Record<string, number>
  payment_mode_distribution: Record<string, number>
  benchmark_gaps: BenchmarkGap[]
}

interface DemandCity {
  city: string
  order_count: number
  rto_count: number
  rto_rate: number
}

// ---------------------------------------------------------------------------
// India city coordinate lookup
// Destination clusters are slugified city names (lowercase, underscores for spaces)
// ---------------------------------------------------------------------------

const CITY_COORDS: Record<string, [number, number]> = {
  mumbai: [19.076, 72.877],
  delhi: [28.614, 77.209],
  new_delhi: [28.614, 77.209],
  bangalore: [12.972, 77.594],
  bengaluru: [12.972, 77.594],
  hyderabad: [17.385, 78.487],
  chennai: [13.083, 80.271],
  kolkata: [22.573, 88.364],
  pune: [18.520, 73.857],
  ahmedabad: [23.023, 72.571],
  jaipur: [26.913, 75.787],
  surat: [21.170, 72.831],
  lucknow: [26.847, 80.946],
  kanpur: [26.449, 80.332],
  nagpur: [21.145, 79.088],
  indore: [22.719, 75.857],
  bhopal: [23.259, 77.413],
  visakhapatnam: [17.687, 83.218],
  agra: [27.177, 78.008],
  vadodara: [22.307, 73.181],
  ludhiana: [30.901, 75.857],
  patna: [25.594, 85.137],
  nashik: [20.006, 73.790],
  faridabad: [28.408, 77.318],
  meerut: [28.985, 77.706],
  rajkot: [22.304, 70.802],
  varanasi: [25.318, 82.974],
  coimbatore: [11.017, 76.956],
  kochi: [9.931, 76.267],
  guwahati: [26.145, 91.736],
  chandigarh: [30.733, 76.779],
  bhubaneswar: [20.296, 85.825],
  dehradun: [30.316, 78.032],
  amritsar: [31.634, 74.872],
  prayagraj: [25.436, 81.846],
  allahabad: [25.436, 81.846],
  jabalpur: [23.181, 79.986],
  gwalior: [26.218, 78.183],
  ranchi: [23.344, 85.310],
  raipur: [21.251, 81.630],
  jodhpur: [26.239, 73.024],
  madurai: [9.925, 78.120],
  thiruvananthapuram: [8.524, 76.937],
  trivandrum: [8.524, 76.937],
  vijayawada: [16.506, 80.648],
  thane: [19.218, 72.978],
  navi_mumbai: [19.033, 73.030],
  gurugram: [28.459, 77.027],
  gurgaon: [28.459, 77.027],
  noida: [28.535, 77.391],
  ghaziabad: [28.669, 77.454],
  mysore: [12.296, 76.639],
  mysuru: [12.296, 76.639],
  hubli: [15.365, 75.124],
  belgaum: [15.850, 74.498],
  belagavi: [15.850, 74.498],
  mangalore: [12.914, 74.856],
  kota: [25.214, 75.865],
  siliguri: [26.727, 88.395],
  warangal: [17.978, 79.594],
  guntur: [16.301, 80.443],
  salem: [11.664, 78.146],
  tiruppur: [11.109, 77.341],
  bhilai: [21.217, 81.433],
  bhiwandi: [19.296, 73.063],
  tiruchirappalli: [10.791, 78.705],
}

function getCityCoords(slug: string): [number, number] | null {
  const lower = slug.toLowerCase().replace(/\s+/g, '_')
  if (CITY_COORDS[lower]) return CITY_COORDS[lower]
  const noUnderscore = lower.replace(/_/g, '')
  for (const [k, v] of Object.entries(CITY_COORDS)) {
    if (k.replace(/_/g, '') === noUnderscore) return v
  }
  return null
}

// ---------------------------------------------------------------------------
// Pie chart colors
// ---------------------------------------------------------------------------

const PIE_COLORS = ['#EE3C26', '#2563EB', '#16A34A', '#F59E0B', '#8B5CF6', '#EC4899', '#0EA5E9']

// ---------------------------------------------------------------------------
// Gap row color — deep red for large gaps, green for outperformance
// ---------------------------------------------------------------------------

function gapBadgeClass(gap: number): string {
  if (gap > 10) return 'bg-red-700 text-white'
  if (gap > 5) return 'bg-red-500 text-white'
  if (gap > 2) return 'bg-red-100 text-red-800'
  if (gap < -5) return 'bg-green-100 text-green-800'
  if (gap < -2) return 'bg-green-50 text-green-700'
  return 'bg-gray-100 text-gray-600'
}

function cardBorderClass(gap: number): string {
  if (gap > 5) return 'border-red-300'
  if (gap < -2) return 'border-green-200'
  return 'border-gray-200'
}

function gapLabel(gap: number): string {
  const sign = gap >= 0 ? '+' : ''
  return `${sign}${gap.toFixed(1)}pp`
}

// ---------------------------------------------------------------------------
// Natural-language insight generator (template-based)
// ---------------------------------------------------------------------------

function generateInsight(b: BenchmarkGap): string {
  const cohort = `${b.payment_mode} ${b.category} (${b.price_band}-value)`
  const myRTO = b.merchant_rto_rate.toFixed(1)
  const peerRTO = b.peer_rto_rate.toFixed(1)
  const gapAbs = Math.abs(b.rto_gap).toFixed(1)
  const peerOrders = b.peer_total_orders.toLocaleString()

  if (b.rto_gap > 10) {
    return `Your ${cohort} orders have a ${myRTO}% RTO rate — ${gapAbs}pp higher than the ${peerRTO}% average seen across ${peerOrders} peer orders. This is a high-priority cohort requiring immediate action (verification calls, prepaid nudges, or auto-cancel review).`
  }
  if (b.rto_gap > 5) {
    return `${cohort} orders show a ${myRTO}% RTO rate, which is ${gapAbs}pp above the peer benchmark of ${peerRTO}% (based on ${peerOrders} peer orders). Consider targeted interventions for these orders.`
  }
  if (b.rto_gap > 2) {
    return `${cohort} orders are slightly above peer average: your ${myRTO}% vs ${peerRTO}% across ${peerOrders} peer orders (gap: ${gapAbs}pp). Worth monitoring.`
  }
  if (b.rto_gap < -5) {
    return `${cohort} is a strength — your ${myRTO}% RTO rate is ${gapAbs}pp better than the peer average of ${peerRTO}% (${peerOrders} peer orders). Keep optimising this cohort.`
  }
  if (b.rto_gap < -2) {
    return `${cohort} is performing well: your ${myRTO}% RTO rate beats the peer average of ${peerRTO}% by ${gapAbs}pp (${peerOrders} peer orders).`
  }
  return `${cohort} orders are on par with peers: your ${myRTO}% vs ${peerRTO}% average across ${peerOrders} peer orders (gap: ${gapAbs}pp).`
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export default function Snapshot() {
  const [merchants, setMerchants] = useState<Merchant[]>([])
  const [selectedId, setSelectedId] = useState<string>('')
  const [data, setData] = useState<SnapshotData | null>(null)
  const [demandMap, setDemandMap] = useState<DemandCity[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const [filterCategory, setFilterCategory] = useState<string>('')
  const [filterPriceBand, setFilterPriceBand] = useState<string>('')
  const [filterPayment, setFilterPayment] = useState<string>('')

  useEffect(() => {
    fetchMerchants()
      .then((d) => {
        const list = d as Merchant[]
        setMerchants(list)
        if (list.length > 0) {
          setSelectedId(list[0].merchant_id)
        }
      })
      .catch(() => {})
  }, [])

  const load = (id: string) => {
    if (!id) return
    setLoading(true)
    setError(null)
    setData(null)
    setDemandMap([])
    Promise.all([fetchSnapshot(id), fetchDemandMap(id)])
      .then(([snap, map]) => {
        setData(snap as SnapshotData)
        setDemandMap(map as DemandCity[])
      })
      .catch((e) => setError(e.message ?? 'Failed to load snapshot'))
      .finally(() => setLoading(false))
  }

  useEffect(() => { if (selectedId) load(selectedId) }, [selectedId])

  const categories = useMemo(
    () => [...new Set((data?.benchmark_gaps ?? []).map((b) => b.category))].sort(),
    [data],
  )
  const priceBands = useMemo(
    () => [...new Set((data?.benchmark_gaps ?? []).map((b) => b.price_band))].sort(),
    [data],
  )
  const paymentModes = useMemo(
    () => [...new Set((data?.benchmark_gaps ?? []).map((b) => b.payment_mode))].sort(),
    [data],
  )

  const filteredGaps = useMemo(() => {
    return (data?.benchmark_gaps ?? []).filter((b) => {
      if (filterCategory && b.category !== filterCategory) return false
      if (filterPriceBand && b.price_band !== filterPriceBand) return false
      if (filterPayment && b.payment_mode !== filterPayment) return false
      return true
    })
  }, [data, filterCategory, filterPriceBand, filterPayment])

  const hasActiveFilter = !!(filterCategory || filterPriceBand || filterPayment)

  // Re-fetch demand map when filters change
  useEffect(() => {
    if (!selectedId) return
    const filters = {
      category: filterCategory || undefined,
      price_band: filterPriceBand || undefined,
      payment_mode: filterPayment || undefined,
    }
    fetchDemandMap(selectedId, filters)
      .then((map) => setDemandMap(map as DemandCity[]))
      .catch(() => {})
  }, [selectedId, filterCategory, filterPriceBand, filterPayment])

  // Filtered metrics derived from benchmark gaps
  const filteredOrderCount = useMemo(
    () => filteredGaps.reduce((s, b) => s + b.order_count, 0),
    [filteredGaps],
  )

  // Aggregate filtered gaps by category for the pie chart
  const categoryData = useMemo(() => {
    if (hasActiveFilter) {
      const byCategory: Record<string, number> = {}
      for (const b of filteredGaps) {
        byCategory[b.category] = (byCategory[b.category] ?? 0) + b.order_count
      }
      return Object.entries(byCategory).map(([name, value]) => ({ name, value }))
    }
    return Object.entries(data?.category_distribution ?? {}).map(([name, value]) => ({ name, value }))
  }, [data, filteredGaps, hasActiveFilter])

  const mapCities = useMemo(() => {
    const maxOrders = Math.max(...demandMap.map((c) => c.order_count), 1)
    return demandMap
      .map((c) => ({ ...c, coords: getCityCoords(c.city) }))
      .filter((c) => c.coords !== null)
      .map((c) => ({ ...c, radius: 6 + 22 * (c.order_count / maxOrders) }))
  }, [demandMap])

  const totalOrders = useMemo(
    () => hasActiveFilter
      ? filteredOrderCount
      : Object.values(data?.category_distribution ?? {}).reduce((s, v) => s + v, 0),
    [data, hasActiveFilter, filteredOrderCount],
  )

  const selectedMerchant = merchants.find((m) => m.merchant_id === selectedId)

  if (loading) return <p className="text-sm text-gray-500">Loading...</p>
  if (error) return <p className="text-sm text-red-600">{error}</p>
  if (!data) return null

  return (
    <div className="space-y-6">

      {/* Header */}
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-[28px] font-bold leading-tight text-gray-900">Client Snapshot</h1>
          {selectedMerchant?.name && (
            <p className="text-sm text-gray-500 mt-0.5">{selectedMerchant.name}</p>
          )}
        </div>
        <div className="flex items-center gap-3">
          <select
            className="text-sm border border-gray-300 rounded-md px-3 py-1.5 bg-white focus:outline-none focus:ring-1 focus:ring-gray-400 max-w-[280px]"
            value={selectedId}
            onChange={(e) => setSelectedId(e.target.value)}
          >
            {merchants.length === 0 && <option value={selectedId}>{selectedId}</option>}
            {merchants.map((m) => (
              <option key={m.merchant_id} value={m.merchant_id}>
                {m.name ?? m.merchant_id} ({m.order_count.toLocaleString()})
              </option>
            ))}
          </select>
          <Button variant="secondary" onClick={() => load(selectedId)}>
            Refresh
          </Button>
        </div>
      </div>

      {/* Filters — control metrics, charts, map, and benchmark list */}
      <div className="flex flex-wrap items-center gap-3">
        <span className="text-xs font-medium text-gray-500 uppercase tracking-wide">Filter by</span>
        <select
          className="text-xs border border-gray-300 rounded px-2 py-1.5 bg-white focus:outline-none"
          value={filterCategory}
          onChange={(e) => setFilterCategory(e.target.value)}
        >
          <option value="">All Categories</option>
          {categories.map((c) => <option key={c} value={c}>{c}</option>)}
        </select>
        <select
          className="text-xs border border-gray-300 rounded px-2 py-1.5 bg-white focus:outline-none"
          value={filterPriceBand}
          onChange={(e) => setFilterPriceBand(e.target.value)}
        >
          <option value="">All Price Bands</option>
          {priceBands.map((p) => <option key={p} value={p}>{p}</option>)}
        </select>
        <select
          className="text-xs border border-gray-300 rounded px-2 py-1.5 bg-white focus:outline-none"
          value={filterPayment}
          onChange={(e) => setFilterPayment(e.target.value)}
        >
          <option value="">All Payment Modes</option>
          {paymentModes.map((m) => <option key={m} value={m}>{m}</option>)}
        </select>
        {hasActiveFilter && (
          <button
            className="text-xs text-gray-400 underline"
            onClick={() => { setFilterCategory(''); setFilterPriceBand(''); setFilterPayment('') }}
          >
            Clear filters
          </button>
        )}
      </div>

      {/* Metric cards */}
      <div className="grid grid-cols-4 gap-4">
        <MetricCard label="Destination Cities" value={demandMap.length.toLocaleString()} />
        <MetricCard label={hasActiveFilter ? 'Filtered Orders' : 'Total Orders'} value={totalOrders.toLocaleString()} />
        <MetricCard
          label={hasActiveFilter ? 'Filtered Avg RTO' : 'Avg RTO Rate'}
          value={
            filteredGaps.length > 0
              ? `${(filteredGaps.reduce((s, b) => s + b.merchant_rto_rate * b.order_count, 0) / Math.max(filteredOrderCount, 1)).toFixed(1)}%`
              : '—'
          }
        />
        <MetricCard label="Cohorts Shown" value={`${filteredGaps.length} / ${(data?.benchmark_gaps ?? []).length}`} />
      </div>

      {/* Charts row */}
      <div className="grid grid-cols-2 gap-6">

        {/* Category pie */}
        <div className="bg-white border border-gray-300 rounded-lg p-4">
          <p className="text-xs text-gray-500 uppercase tracking-wide mb-3">
            Category Distribution{hasActiveFilter ? ' (filtered)' : ''}
          </p>
          {categoryData.length === 0 ? (
            <p className="text-sm text-gray-400 py-8 text-center">No data</p>
          ) : (
            <ResponsiveContainer width="100%" height={260}>
              <PieChart>
                <Pie
                  data={categoryData}
                  dataKey="value"
                  nameKey="name"
                  cx="50%"
                  cy="50%"
                  outerRadius={90}
                  paddingAngle={2}
                  label={({ percent }) =>
                    percent > 0.05 ? `${(percent * 100).toFixed(0)}%` : ''
                  }
                  labelLine={false}
                >
                  {categoryData.map((_, i) => (
                    <Cell key={i} fill={PIE_COLORS[i % PIE_COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip formatter={(v: number) => v.toLocaleString()} />
                <Legend />
              </PieChart>
            </ResponsiveContainer>
          )}
        </div>

        {/* India demand heatmap */}
        <div className="bg-white border border-gray-300 rounded-lg p-4">
          <p className="text-xs text-gray-500 uppercase tracking-wide mb-3">Demand Heatmap — India</p>
          <div style={{ height: 260, borderRadius: 6, overflow: 'hidden' }}>
            <MapContainer
              center={[22.5, 82.5]}
              zoom={4}
              style={{ height: '100%', width: '100%' }}
              zoomControl={false}
              scrollWheelZoom={false}
            >
              <TileLayer
                url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
              />
              {mapCities.map((c) => (
                <CircleMarker
                  key={c.city}
                  center={c.coords as [number, number]}
                  radius={c.radius}
                  pathOptions={{
                    fillColor: c.rto_rate > 30 ? '#EE3C26' : c.rto_rate > 15 ? '#F59E0B' : '#2563EB',
                    fillOpacity: 0.72,
                    color: 'white',
                    weight: 1,
                  }}
                >
                  <MapTooltip>
                    <strong>{c.city.replace(/_/g, ' ')}</strong><br />
                    {c.order_count.toLocaleString()} orders · {c.rto_rate}% RTO
                  </MapTooltip>
                </CircleMarker>
              ))}
            </MapContainer>
          </div>
          <div className="flex gap-4 mt-2 text-xs text-gray-500">
            <span className="flex items-center gap-1.5">
              <span className="inline-block w-2.5 h-2.5 rounded-full bg-blue-600" /> &lt;15% RTO
            </span>
            <span className="flex items-center gap-1.5">
              <span className="inline-block w-2.5 h-2.5 rounded-full bg-amber-500" /> 15–30%
            </span>
            <span className="flex items-center gap-1.5">
              <span className="inline-block w-2.5 h-2.5 rounded-full bg-red-600" /> &gt;30%
            </span>
          </div>
        </div>
      </div>

      {/* Peer Benchmark Insights */}
      <div>
        <div className="flex items-center justify-between mb-3">
          <h2 className="text-xl font-semibold text-gray-900">Peer Benchmark Insights</h2>
          <span className="text-xs text-gray-400">
            Peers with &lt;50 orders per cohort excluded · sorted by gap
          </span>
        </div>

        {/* Filters moved to top of page */}

        {filteredGaps.length === 0 ? (
          <p className="text-sm text-gray-500 py-4">No benchmark data for selected filters.</p>
        ) : (
          <div className="space-y-3">
            {filteredGaps.map((b, i) => (
              <div
                key={i}
                className={`rounded-lg border bg-white p-4 ${cardBorderClass(b.rto_gap)}`}
              >
                <div className="flex flex-wrap items-center gap-2 mb-2">
                  <span className="text-xs font-semibold px-2 py-0.5 bg-gray-100 text-gray-800 rounded">
                    {b.category}
                  </span>
                  <span className="text-xs px-2 py-0.5 bg-gray-50 text-gray-600 rounded border border-gray-200">
                    {b.price_band}-value
                  </span>
                  <span className="text-xs px-2 py-0.5 bg-gray-50 text-gray-600 rounded border border-gray-200">
                    {b.payment_mode}
                  </span>
                  <span className={`ml-auto text-xs font-bold px-2.5 py-0.5 rounded ${gapBadgeClass(b.rto_gap)}`}>
                    {gapLabel(b.rto_gap)}
                  </span>
                </div>

                <p className="text-sm text-gray-800 leading-relaxed mb-3">
                  {generateInsight(b)}
                </p>

                <div className="flex flex-wrap gap-6 text-xs">
                  <span className="text-gray-500">
                    My RTO: <strong className="text-gray-900">{b.merchant_rto_rate}%</strong>
                  </span>
                  <span className="text-gray-500">
                    Peer avg: <strong className="text-gray-900">{b.peer_rto_rate}%</strong>
                  </span>
                  <span className="text-gray-500">
                    Peer orders: <strong className="text-gray-900">{b.peer_total_orders.toLocaleString()}</strong>
                  </span>
                  <span className="text-gray-500">
                    My orders: <strong className="text-gray-900">{b.order_count.toLocaleString()}</strong>
                  </span>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}
