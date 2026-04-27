import { useState, useEffect, useCallback, useMemo } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer,
} from 'recharts'
import { MapContainer, TileLayer, CircleMarker, Tooltip as MapTooltip } from 'react-leaflet'
import 'leaflet/dist/leaflet.css'
import {
  fetchClients, fetchBenchmark, fetchDemandMap,
  type ClientSummary, type BenchmarkResult, type DemandCity,
} from './lib/api'

export default function App() {
  const [clients, setClients] = useState<ClientSummary[]>([])
  const [selectedId, setSelectedId] = useState('')
  const [result, setResult] = useState<BenchmarkResult | null>(null)
  const [demandMap, setDemandMap] = useState<DemandCity[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  useEffect(() => {
    fetchClients()
      .then(setClients)
      .catch(() => setError('Failed to load clients'))
  }, [])

  const runBenchmark = useCallback(async () => {
    if (!selectedId) return
    setLoading(true)
    setError('')
    setResult(null)
    setDemandMap([])
    try {
      const [data, map] = await Promise.all([
        fetchBenchmark(selectedId),
        fetchDemandMap(selectedId),
      ])
      setResult(data)
      setDemandMap(map)
    } catch {
      setError('Failed to load benchmark data')
    } finally {
      setLoading(false)
    }
  }, [selectedId])

  return (
    <div className="min-h-screen bg-gray-50">
      <header className="bg-gray-900 text-white px-6 py-4">
        <div className="max-w-[1440px] mx-auto flex items-center gap-3">
          <div className="w-8 h-8 bg-delhivery-red rounded flex items-center justify-center text-sm font-bold">D</div>
          <div>
            <h1 className="text-lg font-semibold leading-tight">Client Benchmark Report</h1>
            <p className="text-xs text-gray-500">COD RTO Performance Analysis — Peer Comparison</p>
          </div>
        </div>
      </header>

      <main className="max-w-[1440px] mx-auto px-6 py-6">
        <div className="flex items-end gap-3 mb-6">
          <div className="flex-1 max-w-md">
            <label className="block text-xs font-medium text-gray-700 mb-1">Select Client</label>
            <select
              value={selectedId}
              onChange={(e) => setSelectedId(e.target.value)}
              className="w-full h-9 px-3 text-sm border border-gray-300 rounded bg-white focus:outline-none focus:border-delhivery-red"
            >
              <option value="">— Choose a client —</option>
              {[...clients].sort((a, b) => {
                if (a.category === 'Others' && b.category !== 'Others') return 1
                if (a.category !== 'Others' && b.category === 'Others') return -1
                return a.name.localeCompare(b.name)
              }).map((c) => (
                <option key={c.client_id} value={c.client_id}>
                  {c.name} ({c.category})
                </option>
              ))}
            </select>
          </div>
          <button
            onClick={runBenchmark}
            disabled={!selectedId || loading}
            className="h-9 px-5 text-sm font-medium text-white bg-delhivery-red rounded hover:bg-delhivery-red-dark disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
          >
            {loading ? 'Loading…' : 'Run Analysis'}
          </button>
        </div>

        {error && (
          <div className="mb-4 px-4 py-2 text-sm text-danger bg-danger-light border border-danger rounded">
            {error}
          </div>
        )}

        {result && <BenchmarkResults data={result} demandMap={demandMap} />}
      </main>
    </div>
  )
}

function rto1(v: number | undefined): string {
  return (v ?? 0).toFixed(1)
}


// ── City Coords ──
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


// ── Results ──
function BenchmarkResults({ data, demandMap }: { data: BenchmarkResult; demandMap: DemandCity[] }) {
  const { client, peers, peer_average, overall_assessment, strengths, improvement_areas, peer_learnings } = data

  const mapCities = useMemo(() => {
    const maxOrders = Math.max(...demandMap.map((c) => c.order_count), 1)
    return demandMap
      .map((c) => {
        const rto = Math.min(c.rto_rate, 100)
        return { ...c, rto_rate: rto, coords: getCityCoords(c.city) }
      })
      .filter((c) => c.coords !== null)
      .map((c) => ({ ...c, radius: Math.min(20, Math.max(4, 4 + 16 * (c.order_count / maxOrders))) }))
  }, [demandMap])

  return (
    <div className="space-y-6">
      {/* Client card */}
      <ClientCard client={client} peerAvgRto={peer_average.rto_rate} />

      {/* India Demand Heatmap */}
      {mapCities.length > 0 && (
        <section className="bg-white border border-gray-300 rounded-lg p-5">
          <h2 className="text-base font-semibold mb-3">COD Demand Heatmap — India</h2>
          <div style={{ height: 350, borderRadius: 6, overflow: 'hidden' }}>
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
                    {c.order_count.toLocaleString()} orders · {c.rto_rate}% COD RTO
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
        </section>
      )}

      {/* Overall Assessment */}
      <section className="bg-white border border-gray-300 rounded-lg p-5">
        <h2 className="text-base font-semibold mb-2">Executive Summary</h2>
        <p className="text-sm text-gray-800 leading-relaxed">{overall_assessment}</p>
      </section>

      {/* Strengths — green cards */}
      {strengths.length > 0 && (
        <section>
          <h2 className="text-base font-semibold mb-3 text-green-800">✦ What You're Doing Well</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            {strengths.map((s, i) => (
              <div key={i} className="bg-green-50 border border-green-200 rounded-lg p-4">
                <div className="text-sm font-semibold text-green-900 mb-1">{s.area}</div>
                <p className="text-sm text-green-800">{s.detail}</p>
              </div>
            ))}
          </div>
        </section>
      )}

      {/* Improvement Areas — amber/red cards */}
      {improvement_areas.length > 0 && (
        <section>
          <h2 className="text-base font-semibold mb-3 text-amber-800">✦ Where You Can Improve</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            {improvement_areas.map((item, i) => (
              <div
                key={i}
                className={`border rounded-lg p-4 ${
                  item.priority === 'high'
                    ? 'bg-red-50 border-red-200'
                    : item.priority === 'medium'
                    ? 'bg-amber-50 border-amber-200'
                    : 'bg-yellow-50 border-yellow-200'
                }`}
              >
                <div className="flex items-center gap-2 mb-1">
                  <span className={`text-sm font-semibold ${
                    item.priority === 'high' ? 'text-red-900' : 'text-amber-900'
                  }`}>
                    {item.area}
                  </span>
                  <span className={`text-[10px] font-bold uppercase px-1.5 py-0.5 rounded ${
                    item.priority === 'high'
                      ? 'bg-red-200 text-red-800'
                      : item.priority === 'medium'
                      ? 'bg-amber-200 text-amber-800'
                      : 'bg-yellow-200 text-yellow-800'
                  }`}>
                    {item.priority}
                  </span>
                </div>
                <p className={`text-sm ${
                  item.priority === 'high' ? 'text-red-800' : 'text-amber-800'
                }`}>
                  {item.detail}
                </p>
              </div>
            ))}
          </div>
        </section>
      )}

      {/* Peer Learnings — blue cards */}
      {peer_learnings.length > 0 && (
        <section>
          <h2 className="text-base font-semibold mb-3 text-blue-800">✦ What Peers Do Differently</h2>
          <div className="space-y-2">
            {peer_learnings.map((learning, i) => (
              <div key={i} className="bg-blue-50 border border-blue-200 rounded-lg px-4 py-3">
                <p className="text-sm text-blue-900">{learning}</p>
              </div>
            ))}
          </div>
        </section>
      )}

      {/* Peer comparison table */}
      <PeerTable client={client} peers={peers} peerAverage={peer_average} />

      {/* Charts */}
      <ComparisonBarChart client={client} peerAvg={peer_average} />

      {/* City RTO with pincode breakdown */}
      <CityRtoSection client={client} peerAvg={peer_average} />
    </div>
  )
}


// ── Client Card ──
function ClientCard({ client, peerAvgRto }: { client: BenchmarkResult['client']; peerAvgRto: number }) {
  const gap = +(client.rto_rate - peerAvgRto).toFixed(1)
  const isWorse = gap > 0

  return (
    <div className="bg-white border border-gray-300 rounded-lg p-4">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <h2 className="text-lg font-semibold">{client.name}</h2>
          <div className="flex items-center gap-2 mt-1">
            <span className="text-xs px-2 py-0.5 rounded bg-gray-100 text-gray-700 font-medium">
              {client.category}
            </span>
            <span className="text-xs px-2 py-0.5 rounded bg-gray-100 text-gray-700 font-medium">
              ₹{client.avg_price_range}
            </span>
            {client.client_type && (
              <span className="text-xs px-2 py-0.5 rounded bg-blue-50 text-blue-700 font-medium">
                {client.client_type}
              </span>
            )}
            <span className="text-xs text-gray-500">
              {client.order_count.toLocaleString()} orders
            </span>
            <span className="text-xs text-gray-400">
              (Jan–Jun 2025)
            </span>
          </div>
          <div className="flex items-center gap-4 mt-3 text-xs text-gray-600">
            <span>COD: {rto1(client.cod_pct)}%</span>
            <span>Avg ₹{client.avg_order_value}</span>
            <span>Manifest: {client.avg_manifest_latency}d</span>
            <span className="group relative cursor-help">
              Address Corrected: {rto1(client.addr_corrected_pct)}%
              <span className="invisible group-hover:visible absolute bottom-full left-0 mb-1 w-64 p-2 text-xs bg-gray-900 text-white rounded shadow-lg z-10">
                % of orders where the delivery address was corrected by Delhivery's address fix system before delivery
              </span>
            </span>
          </div>
        </div>
        <div className="text-right">
          <div className="text-2xl font-bold font-mono">{rto1(client.rto_rate)}%</div>
          <div className="text-xs text-gray-500">COD RTO Rate</div>
          <div className={`text-sm font-medium mt-0.5 ${isWorse ? 'text-danger' : 'text-success'}`}>
            {isWorse ? '▲' : '▼'} {Math.abs(gap).toFixed(1)}pp vs peers
          </div>
        </div>
      </div>
    </div>
  )
}

// ── Peer Table (no high_risk_buyer_pct, renamed Addr Fixed → Address Corrected) ──
function PeerTable({ client, peers, peerAverage }: {
  client: BenchmarkResult['client']
  peers: BenchmarkResult['peers']
  peerAverage: BenchmarkResult['peer_average']
}) {
  return (
    <section className="bg-white border border-gray-300 rounded-lg overflow-hidden">
      <div className="px-4 py-3 border-b border-gray-300">
        <h2 className="text-base font-semibold">Peer Comparison</h2>
        <p className="text-xs text-gray-500 mt-0.5">Top 5 peers in same category (matched by AOV, ranked by volume)</p>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-gray-100 text-xs text-gray-700 uppercase tracking-wide">
              <th className="text-left px-4 py-2 font-medium">Client</th>
              <th className="text-right px-4 py-2 font-medium">Orders</th>
              <th className="text-right px-4 py-2 font-medium">COD RTO %</th>
              <th className="text-right px-4 py-2 font-medium">COD %</th>
              <th className="text-right px-4 py-2 font-medium">Avg Value</th>
              <th className="text-right px-4 py-2 font-medium group relative cursor-help">
                Address Corrected %
                <span className="invisible group-hover:visible absolute bottom-full right-0 mb-1 w-64 p-2 text-xs bg-gray-900 text-white rounded shadow-lg z-10 normal-case tracking-normal font-normal">
                  % of orders where the delivery address was corrected by Delhivery's address fix system before delivery
                </span>
              </th>
              <th className="text-right px-4 py-2 font-medium">Manifest Days</th>
              <th className="text-right px-4 py-2 font-medium">Repeat %</th>
            </tr>
          </thead>
          <tbody>
            <tr className="bg-delhivery-red/5 font-medium border-b border-gray-300">
              <td className="px-4 py-2"><span className="text-delhivery-red">★</span> {client.name}</td>
              <td className="text-right px-4 py-2 font-mono">{client.order_count}</td>
              <td className="text-right px-4 py-2 font-mono">{rto1(client.rto_rate)}%</td>
              <td className="text-right px-4 py-2 font-mono">{rto1(client.cod_pct)}%</td>
              <td className="text-right px-4 py-2 font-mono">₹{client.avg_order_value}</td>
              <td className="text-right px-4 py-2 font-mono">{rto1(client.addr_corrected_pct)}%</td>
              <td className="text-right px-4 py-2 font-mono">{client.avg_manifest_latency}</td>
              <td className="text-right px-4 py-2 font-mono">{rto1(client.repeat_buyer_pct)}%</td>
            </tr>
            {peers.map((p, i) => (
              <tr key={p.client_id} className={i % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                <td className="px-4 py-2 text-gray-700">{p.name}</td>
                <td className="text-right px-4 py-2 font-mono text-gray-700">{p.order_count}</td>
                <td className="text-right px-4 py-2 font-mono">{rto1(p.rto_rate)}%</td>
                <td className="text-right px-4 py-2 font-mono text-gray-700">{rto1(p.cod_pct)}%</td>
                <td className="text-right px-4 py-2 font-mono text-gray-700">₹{p.avg_order_value}</td>
                <td className="text-right px-4 py-2 font-mono text-gray-700">{rto1(p.addr_corrected_pct)}%</td>
                <td className="text-right px-4 py-2 font-mono text-gray-700">{p.avg_manifest_latency}</td>
                <td className="text-right px-4 py-2 font-mono text-gray-700">{rto1(p.repeat_buyer_pct)}%</td>
              </tr>
            ))}
            <tr className="bg-gray-100 font-medium border-t border-gray-300">
              <td className="px-4 py-2 text-gray-500 italic">Peer Average</td>
              <td className="text-right px-4 py-2 font-mono text-gray-500">—</td>
              <td className="text-right px-4 py-2 font-mono">{rto1(peerAverage.rto_rate)}%</td>
              <td className="text-right px-4 py-2 font-mono text-gray-500">{rto1(peerAverage.cod_pct)}%</td>
              <td className="text-right px-4 py-2 font-mono text-gray-500">₹{peerAverage.avg_order_value}</td>
              <td className="text-right px-4 py-2 font-mono text-gray-500">{rto1(peerAverage.addr_corrected_pct)}%</td>
              <td className="text-right px-4 py-2 font-mono text-gray-500">{peerAverage.avg_manifest_latency}</td>
              <td className="text-right px-4 py-2 font-mono text-gray-500">{rto1(peerAverage.repeat_buyer_pct)}%</td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>
  )
}


// ── Comparison Bar Chart (no Risk & Latency chart) ──
function ComparisonBarChart({ client, peerAvg }: { client: BenchmarkResult['client']; peerAvg: BenchmarkResult['peer_average'] }) {
  const data = [
    { name: 'COD RTO %', client: +rto1(client.rto_rate), peers: +rto1(peerAvg.rto_rate) },
    { name: 'COD %', client: +rto1(client.cod_pct), peers: +rto1(peerAvg.cod_pct) },
    { name: 'Address Corrected %', client: +rto1(client.addr_corrected_pct), peers: +rto1(peerAvg.addr_corrected_pct) },
    { name: 'Repeat %', client: +rto1(client.repeat_buyer_pct), peers: +rto1(peerAvg.repeat_buyer_pct) },
    { name: 'Manifest Days', client: client.avg_manifest_latency, peers: peerAvg.avg_manifest_latency },
  ]

  return (
    <section className="bg-white border border-gray-300 rounded-lg p-4">
      <h3 className="text-sm font-semibold mb-3">Client vs Peer Average</h3>
      <ResponsiveContainer width="100%" height={280}>
        <BarChart data={data} barGap={2}>
          <CartesianGrid strokeDasharray="3 3" stroke="#D1D1D1" />
          <XAxis dataKey="name" tick={{ fontSize: 11 }} />
          <YAxis tick={{ fontSize: 12 }} />
          <Tooltip contentStyle={{ fontSize: 12 }} />
          <Legend wrapperStyle={{ fontSize: 12 }} />
          <Bar dataKey="client" name="Client" fill="#F5D06E" radius={[2, 2, 0, 0]} />
          <Bar dataKey="peers" name="Peer Avg" fill="#93C5FD" radius={[2, 2, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </section>
  )
}

// ── City RTO Section with horizontal bars + pincode breakdown ──
function CityRtoSection({ client, peerAvg }: { client: BenchmarkResult['client']; peerAvg: BenchmarkResult['peer_average'] }) {
  const cities = Object.keys(client.city_rto || {})
  if (cities.length === 0) return null

  const data = cities.map((city) => ({
    name: city,
    client: +(client.city_rto?.[city] ?? 0).toFixed(1),
    peers: +(peerAvg.city_rto?.[city] ?? 0).toFixed(1),
  }))

  type PinInfo = { client_rto: number; peer_rto: number; client_orders: number }
  const pincodeData: Record<string, Record<string, PinInfo>> = (client.city_pincode_rto || {}) as Record<string, Record<string, PinInfo>>

  return (
    <section className="bg-white border border-gray-300 rounded-lg p-4">
      <h3 className="text-sm font-semibold mb-3">COD RTO by Top Destination Cities</h3>
      <ResponsiveContainer width="100%" height={Math.max(200, cities.length * 50)}>
        <BarChart data={data} layout="vertical" barGap={2}>
          <CartesianGrid strokeDasharray="3 3" stroke="#D1D1D1" />
          <XAxis type="number" tick={{ fontSize: 12 }} unit="%" />
          <YAxis dataKey="name" type="category" tick={{ fontSize: 11 }} width={120} />
          <Tooltip contentStyle={{ fontSize: 12 }} />
          <Legend wrapperStyle={{ fontSize: 12 }} />
          <Bar dataKey="client" name="Client" fill="#F5D06E" radius={[0, 2, 2, 0]} />
          <Bar dataKey="peers" name="Peer Avg" fill="#93C5FD" radius={[0, 2, 2, 0]} />
        </BarChart>
      </ResponsiveContainer>

      {/* Pincode breakdown below chart */}
      {Object.keys(pincodeData).length > 0 && (
        <div className="mt-4 border-t border-gray-200 pt-4">
          <h4 className="text-xs font-semibold text-gray-600 uppercase tracking-wide mb-3">
            Pincode-Level COD RTO (Top Cities)
          </h4>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {Object.entries(pincodeData).map(([city, pins]) => (
              <div key={city} className="bg-gray-50 rounded-lg p-3">
                <div className="text-sm font-semibold text-gray-800 mb-2">{city}</div>
                <div className="space-y-1.5">
                  {Object.entries(pins).map(
                    ([pin, info]) => (
                      <div key={pin} className="flex items-center justify-between text-xs">
                        <span className="text-gray-600 font-mono">{pin}</span>
                        <div className="flex items-center gap-2">
                          <span className="text-gray-500">({info.client_orders} orders)</span>
                          <span className={`font-medium ${
                            info.peer_rto >= 0 && info.client_rto > info.peer_rto + 3
                              ? 'text-red-600'
                              : 'text-gray-700'
                          }`}>
                            {info.client_rto.toFixed(1)}%
                          </span>
                          {info.peer_rto >= 0 && (
                            <span className="text-gray-400">
                              vs {info.peer_rto.toFixed(1)}% peers
                            </span>
                          )}
                        </div>
                      </div>
                    )
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </section>
  )
}
