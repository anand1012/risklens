import { useState } from 'react'
import { NavLink, Outlet } from 'react-router-dom'
import ChatPanel from './ChatPanel'

const NAV = [
  { to: '/',            label: 'Catalog',    icon: '⬡' },
  { to: '/governance',  label: 'Governance', icon: '◈' },
  { to: '/assets',      label: 'Assets',     icon: '◻' },
  { to: '/risk',        label: 'FRTB Risk',  icon: '◉' },
]

export default function Layout() {
  const [chatOpen, setChatOpen] = useState(false)

  return (
    <div className="flex h-full overflow-hidden">
      {/* Sidebar */}
      <aside className="w-56 flex-shrink-0 bg-slate-900 border-r border-slate-800 flex flex-col">
        <div className="px-5 py-5 border-b border-slate-800">
          <span className="text-brand-500 font-bold text-lg tracking-tight">RiskLens</span>
          <p className="text-slate-500 text-xs mt-0.5">FRTB Data Catalog</p>
        </div>
        <nav className="flex-1 px-3 py-4 space-y-1">
          {NAV.map(({ to, label, icon }) => (
            <NavLink
              key={to}
              to={to}
              end={to === '/'}
              className={({ isActive }) =>
                `flex items-center gap-2.5 px-3 py-2 rounded-lg text-sm font-medium transition-colors ${
                  isActive
                    ? 'bg-brand-600/20 text-brand-400'
                    : 'text-slate-400 hover:text-slate-100 hover:bg-slate-800'
                }`
              }
            >
              <span className="text-base">{icon}</span>
              {label}
            </NavLink>
          ))}
        </nav>
        <div className="px-3 pb-4">
          <button
            onClick={() => setChatOpen(true)}
            className="w-full flex items-center gap-2.5 px-3 py-2 rounded-lg text-sm font-medium
                       bg-brand-600 hover:bg-brand-700 text-white transition-colors"
          >
            <span>✦</span>
            Ask RiskLens AI
          </button>
        </div>
      </aside>

      {/* Main content — overflow-hidden so each view controls its own scroll */}
      <main className="flex-1 overflow-hidden min-h-0 flex flex-col">
        <Outlet />
      </main>

      {/* Chat panel */}
      <ChatPanel open={chatOpen} onClose={() => setChatOpen(false)} />
    </div>
  )
}
