import { NavLink } from 'react-router-dom'

const navItems = [
  {
    to: '/snapshot',
    label: 'Client Snapshot',
    icon: (
      <svg width="18" height="18" viewBox="0 0 18 18" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
        <rect x="2" y="10" width="3" height="6" />
        <rect x="7.5" y="6" width="3" height="10" />
        <rect x="13" y="2" width="3" height="14" />
      </svg>
    ),
  },
  {
    to: '/advisor',
    label: 'Advisor',
    icon: (
      <svg width="18" height="18" viewBox="0 0 18 18" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
        <path d="M9 2v1" />
        <path d="M9 15v1" />
        <circle cx="9" cy="9" r="3" />
        <path d="M9 2a7 7 0 0 1 0 14" />
        <path d="M6.5 6.5L5 5" />
        <path d="M12 12l-1.5-1.5" />
      </svg>
    ),
  },
  {
    to: '/orders',
    label: 'Orders',
    icon: (
      <svg width="18" height="18" viewBox="0 0 18 18" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
        <line x1="3" y1="4" x2="15" y2="4" />
        <line x1="3" y1="7.5" x2="15" y2="7.5" />
        <line x1="3" y1="11" x2="15" y2="11" />
        <line x1="3" y1="14.5" x2="11" y2="14.5" />
      </svg>
    ),
  },
  {
    to: '/actions',
    label: 'Actions',
    icon: (
      <svg width="18" height="18" viewBox="0 0 18 18" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
        <polyline points="10,2 6,10 12,10 8,16" />
      </svg>
    ),
  },
]

export default function Sidebar() {
  return (
    <aside className="fixed inset-y-0 left-0 w-60 bg-gray-900 flex flex-col">
      <div className="px-6 py-5">
        <span className="text-white text-lg font-semibold tracking-tight">
          Commerce AI
        </span>
        <span className="block text-xs text-gray-500">by Delhivery</span>
      </div>

      <nav className="flex-1 px-3 space-y-1">
        {navItems.map((item) => (
          <NavLink
            key={item.to}
            to={item.to}
            className={({ isActive }) =>
              `flex items-center gap-3 px-3 py-2 rounded text-sm font-medium transition-colors ${
                isActive
                  ? 'text-white border-l-2 border-delhivery-red'
                  : 'text-gray-500 hover:text-gray-300'
              }`
            }
          >
            {item.icon}
            {item.label}
          </NavLink>
        ))}
      </nav>

      <div className="px-3 pb-4">
        <NavLink
          to="/settings"
          className="flex items-center gap-3 px-3 py-2 rounded text-sm font-medium text-gray-500 hover:text-gray-300"
        >
          <svg width="18" height="18" viewBox="0 0 18 18" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
            <circle cx="9" cy="9" r="2.5" />
            <path d="M9 1.5v2M9 14.5v2M1.5 9h2M14.5 9h2M3.4 3.4l1.4 1.4M13.2 13.2l1.4 1.4M3.4 14.6l1.4-1.4M13.2 4.8l1.4-1.4" />
          </svg>
          Settings
        </NavLink>
      </div>
    </aside>
  )
}
