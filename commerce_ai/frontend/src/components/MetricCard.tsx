interface MetricCardProps {
  label: string
  value: string | number
  delta?: number
  deltaLabel?: string
}

export default function MetricCard({
  label,
  value,
  delta,
  deltaLabel,
}: MetricCardProps) {
  return (
    <div className="bg-white border border-gray-300 rounded-lg p-4">
      <p className="text-xs text-gray-500 uppercase tracking-wide">{label}</p>
      <p className="text-2xl font-bold text-gray-900 font-mono mt-1">
        {value}
      </p>
      {delta !== undefined && (
        <p
          className={`text-sm mt-1 ${
            delta >= 0 ? 'text-success' : 'text-danger'
          }`}
        >
          {delta >= 0 ? '▲' : '▼'} {Math.abs(delta).toFixed(2)}
          {deltaLabel && (
            <span className="text-gray-500 ml-1">{deltaLabel}</span>
          )}
        </p>
      )}
    </div>
  )
}

export type { MetricCardProps }
