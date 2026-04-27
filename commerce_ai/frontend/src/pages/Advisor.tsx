import { useEffect, useState } from 'react'
import { fetchDemandSuggestions } from '../lib/api'
import InsightBlock from '../components/InsightBlock'

const MERCHANT_ID = 'M-8A25EB3E'  // ZIBBRI B2C - 391k real orders

interface Suggestion {
  cohort_dimension: string
  recommended_value: string
  expected_score_improvement: number
  peer_benchmark: {
    peer_sample_size: number
    confidence_interval_width: number
  }
  nl_explanation: string
}

export default function Advisor() {
  const [suggestions, setSuggestions] = useState<Suggestion[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    fetchDemandSuggestions(MERCHANT_ID)
      .then((d) => setSuggestions(d as Suggestion[]))
      .catch((e) => setError(e.message ?? 'Failed to load suggestions'))
      .finally(() => setLoading(false))
  }, [])

  if (loading) return <p className="text-sm text-gray-500">Loading...</p>
  if (error) return <p className="text-sm text-danger">{error}</p>

  return (
    <div className="space-y-6">
      <h1 className="text-[28px] font-bold leading-tight text-gray-900">
        Demand Mix Advisor
      </h1>

      {suggestions.length === 0 ? (
        <p className="text-sm text-gray-500">
          No high-confidence suggestions available at this time.
        </p>
      ) : (
        <div className="flex flex-col gap-6">
          {suggestions.map((s, i) => (
            <div
              key={i}
              className="bg-white border border-gray-300 rounded-lg p-5"
            >
              {/* Header */}
              <div className="flex items-center justify-between mb-4">
                <span className="text-base font-semibold text-gray-900">
                  {s.cohort_dimension}: {s.recommended_value}
                </span>
                <span className="text-sm font-mono text-green-600">
                  {s.expected_score_improvement != null ? `+${s.expected_score_improvement.toFixed(2)} ▲` : '—'}
                </span>
              </div>

              {/* NL Insight */}
              <InsightBlock text={s.nl_explanation} />

              {/* Footer */}
              <p className="text-xs text-gray-500 mt-3">
                Peer sample: {s.peer_benchmark?.peer_sample_size ?? '—'} orders
                {s.peer_benchmark?.confidence_interval_width != null
                  ? ` | CI: ±${Math.round(s.peer_benchmark.confidence_interval_width)}pp`
                  : ''}
              </p>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
