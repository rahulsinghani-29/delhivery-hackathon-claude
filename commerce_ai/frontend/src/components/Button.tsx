import { ButtonHTMLAttributes } from 'react'

type ButtonVariant = 'primary' | 'secondary' | 'danger' | 'ghost'

const variantStyles: Record<ButtonVariant, string> = {
  primary: 'bg-delhivery-red text-white hover:bg-delhivery-red-dark',
  secondary:
    'bg-white text-gray-900 border border-gray-300 hover:border-gray-500',
  danger: 'bg-danger-light text-danger border border-danger hover:bg-danger/10',
  ghost: 'bg-transparent text-delhivery-red hover:bg-gray-100',
}

interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant
}

export default function Button({
  variant = 'primary',
  className = '',
  children,
  ...props
}: ButtonProps) {
  return (
    <button
      className={`h-9 px-4 text-sm font-medium rounded transition-colors ${variantStyles[variant]} ${className}`}
      {...props}
    >
      {children}
    </button>
  )
}

export type { ButtonVariant, ButtonProps }
