import { useState, useEffect, useCallback } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer,
} from 'recharts'
import {
  fetchClients, fetchBenchmark,
  type ClientSummary, type BenchmarkResult,
} from './lib/api'

export default function App() {
  const [clients, setClients] = useState<ClientSummary[]>([])
  const [selectedId, setSelectedId] = useState('')
  const [result, setResult] = useState<BenchmarkResult | null>(null)
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
    try {
      const data = await fetchBenchmark(selectedId)
      setResult(data)
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
              {clients.map((c) => (
                <option key={c.client_id} value={c.client_id}>
                  {c.name} ({c.category}, {c.rto_rate.toFixed(1)}% COD RTO, {c.order_count} orders)
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

        {result && <BenchmarkResults data={result} />}
      </main>
    </div>
  )
}

function rto1(v: number | undefined): string {
  return (v ?? 0).toFixed(1)
}


// ── Results ──
function BenchmarkResults({ data }: { data: BenchmarkResult }) {
  const { client, peers, peer_average, overall_assessment, strengths, improvement_areas, peer_learnings } = data

  return (
    <div className="space-y-6">
      {/* Client card */}
      <ClientCard client={client} peerAvgRto={peer_average.rto_rate} />

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
          <Bar dataKey="client" name="Client" fill="#EE3C26" radius={[2, 2, 0, 0]} />
          <Bar dataKey="peers" name="Peer Avg" fill="#2563EB" radius={[2, 2, 0, 0]} />
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
          <Bar dataKey="client" name="Client" fill="#EE3C26" radius={[0, 2, 2, 0]} />
          <Bar dataKey="peers" name="Peer Avg" fill="#2563EB" radius={[0, 2, 2, 0]} />
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
