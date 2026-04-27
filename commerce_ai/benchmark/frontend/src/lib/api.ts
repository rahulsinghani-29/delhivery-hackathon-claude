const BASE = '/api'

export interface ClientSummary {
  client_id: string
  name: string
  category: string
  avg_price_range: string
  client_type: string
  order_count: number
  rto_rate: number
}

export interface ClientMetrics {
  rto_rate: number
  cod_pct: number
  cod_rto_rate: number
  prepaid_rto_rate: number
  avg_order_value: number
  addr_corrected_pct: number
  avg_manifest_latency: number
  repeat_buyer_pct: number
  payment_rto: Record<string, number>
  city_rto: Record<string, number>
  state_rto: Record<string, number>
  city_pincode_rto?: Record<string, Record<string, { client_rto: number; peer_rto: number; client_orders: number }>>
}

export interface PeerMetrics extends ClientMetrics {
  client_id: string
  name: string
  order_count: number
}

export interface DiagnosisStrength {
  area: string
  detail: string
}

export interface DiagnosisImprovement {
  area: string
  detail: string
  priority: 'high' | 'medium' | 'low'
}

export interface BenchmarkResult {
  client: ClientSummary & ClientMetrics
  peers: PeerMetrics[]
  peer_average: ClientMetrics
  overall_assessment: string
  strengths: DiagnosisStrength[]
  improvement_areas: DiagnosisImprovement[]
  peer_learnings: string[]
}

export async function fetchClients(): Promise<ClientSummary[]> {
  const res = await fetch(`${BASE}/clients`)
  if (!res.ok) throw new Error('Failed to fetch clients')
  return res.json()
}

export async function fetchBenchmark(clientId: string): Promise<BenchmarkResult> {
  const res = await fetch(`${BASE}/clients/${clientId}/benchmark`)
  if (!res.ok) throw new Error('Failed to fetch benchmark')
  return res.json()
}
