// In dev, Vite proxies /api to localhost:8000.
// In production (Vercel), VITE_API_URL points to the Railway backend.
const API_BASE = import.meta.env.VITE_API_URL || '/api'

async function request<T>(url: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${url}`, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  })
  if (!res.ok) {
    const body = await res.json().catch(() => ({}))
    throw new Error(body.detail ?? `Request failed: ${res.status}`)
  }
  return res.json()
}

export async function fetchMerchants() {
  return request('/merchants')
}

export async function fetchSnapshot(merchantId: string) {
  return request(`/merchants/${merchantId}/snapshot`)
}

export async function fetchDemandMap(
  merchantId: string,
  filters?: { category?: string; price_band?: string; payment_mode?: string },
) {
  const params = new URLSearchParams()
  if (filters?.category) params.set('category', filters.category)
  if (filters?.price_band) params.set('price_band', filters.price_band)
  if (filters?.payment_mode) params.set('payment_mode', filters.payment_mode)
  const qs = params.toString()
  return request(`/merchants/${merchantId}/demand-map${qs ? `?${qs}` : ''}`)
}

export async function fetchDemandSuggestions(merchantId: string) {
  return request(`/merchants/${merchantId}/demand-suggestions`)
}

export async function fetchLiveOrders(merchantId: string) {
  return request(`/merchants/${merchantId}/orders/live`)
}

export async function executeAction(
  merchantId: string,
  orderId: string,
  interventionType: string,
) {
  return request(`/merchants/${merchantId}/actions/execute`, {
    method: 'POST',
    body: JSON.stringify({ order_id: orderId, intervention_type: interventionType }),
  })
}

export async function fetchActionLog(merchantId: string) {
  return request(`/merchants/${merchantId}/actions/log`)
}

export async function fetchDashboard(merchantId: string) {
  return request(`/merchants/${merchantId}/dashboard`)
}

export async function fetchPermissions(merchantId: string) {
  return request(`/merchants/${merchantId}/permissions`)
}

export async function updatePermissions(merchantId: string, data: Record<string, unknown>) {
  return request(`/merchants/${merchantId}/permissions`, {
    method: 'PUT',
    body: JSON.stringify(data),
  })
}

export async function fetchCommunicationStatus(merchantId: string) {
  return request(`/merchants/${merchantId}/communications/status`)
}

export async function fetchOrderCommunications(orderId: string) {
  return request(`/orders/${orderId}/communications`)
}

export async function triggerCommunication(orderId: string, issueType: string) {
  return request(`/orders/${orderId}/communications/trigger`, {
    method: 'POST',
    body: JSON.stringify({ issue_type: issueType }),
  })
}
