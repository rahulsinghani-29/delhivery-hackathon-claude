import { Routes, Route, Navigate } from 'react-router-dom'
import Sidebar from './components/Sidebar'
import Snapshot from './pages/Snapshot'
import Advisor from './pages/Advisor'
import Orders from './pages/Orders'
import Actions from './pages/Actions'

const MERCHANT_ID = 'M-8A25EB3E'  // ZIBBRI B2C - 391k real orders

function Placeholder({ title }: { title: string }) {
  return (
    <div>
      <h1 className="text-[28px] font-bold leading-tight text-gray-900">
        {title}
      </h1>
      <p className="mt-2 text-sm text-gray-500">
        Merchant: {MERCHANT_ID} — Screen will be built in a subsequent task.
      </p>
    </div>
  )
}

export default function App() {
  return (
    <div className="flex min-h-screen">
      <Sidebar />
      <main className="ml-60 flex-1 p-8">
        <Routes>
          <Route path="/" element={<Navigate to="/snapshot" replace />} />
          <Route path="/snapshot" element={<Snapshot />} />
          <Route path="/advisor" element={<Advisor />} />
          <Route path="/orders" element={<Orders />} />
          <Route path="/actions" element={<Actions />} />
          <Route
            path="/settings"
            element={<Placeholder title="Settings" />}
          />
        </Routes>
      </main>
    </div>
  )
}
