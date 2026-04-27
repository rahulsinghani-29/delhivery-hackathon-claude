type StatusVariant =
  | 'high-risk'
  | 'medium-risk'
  | 'low-risk'
  | 'auto-cancelled'
  | 'express-upgrade'
  | 'wa-sent'
  | 'voice-scheduled'
  | 'resolved'
  | 'no-response'
  | 'impulsive'

const variantStyles: Record<StatusVariant, string> = {
  'high-risk': 'bg-danger-light text-danger',
  'medium-risk': 'bg-warning-light text-warning',
  'low-risk': 'bg-success-light text-success',
  'auto-cancelled': 'bg-danger-light text-danger border border-danger',
  'express-upgrade': 'bg-info-light text-info',
  'wa-sent': 'bg-info-light text-info',
  'voice-scheduled': 'bg-warning-light text-warning',
  resolved: 'bg-success-light text-success',
  'no-response': 'bg-gray-100 text-gray-700',
  impulsive: 'bg-info-light text-info border border-dashed border-info',
}

const variantLabels: Record<StatusVariant, string> = {
  'high-risk': 'High Risk',
  'medium-risk': 'Medium Risk',
  'low-risk': 'Low Risk',
  'auto-cancelled': 'Auto-Cancelled',
  'express-upgrade': 'Express Upgrade',
  'wa-sent': 'WA Sent',
  'voice-scheduled': 'Voice Scheduled',
  resolved: 'Resolved',
  'no-response': 'No Response',
  impulsive: 'Impulsive',
}

interface StatusTagProps {
  variant: StatusVariant
  label?: string
}

export default function StatusTag({ variant, label }: StatusTagProps) {
  return (
    <span
      className={`inline-block text-xs font-medium px-2 py-0.5 rounded ${variantStyles[variant]}`}
    >
      {label ?? variantLabels[variant]}
    </span>
  )
}

export type { StatusVariant, StatusTagProps }
