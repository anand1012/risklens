import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import { fetchAssets } from '../api'
import { DomainBadge, FreshnessBadge } from '../components/Badges'

export default function Assets() {
  const navigate = useNavigate()
  const [domain, setDomain] = useState('')

  const { data: assets = [], isLoading } = useQuery({
    queryKey: ['assets-gold', domain],
    queryFn: () => fetchAssets({ layer: 'gold', domain: domain || undefined, limit: 200 }),
  })

  const domains = [...new Set(assets.map((a) => a.domain).filter(Boolean))]

  const totalRows = assets.reduce((sum, a) => sum + (a.row_count ?? 0), 0)
  const breaches = assets.filter((a) => a.breach_flag).length
  const staleOrCritical = assets.filter(
    (a) => a.freshness_status === 'stale' || a.freshness_status === 'critical'
  ).length

  return (
    <div className="flex flex-col h-full overflow-auto">
      {/* Header */}
      <div className="px-6 py-5 border-b border-slate-800">
        <h1 className="text-xl font-semibold text-slate-100">Gold Assets</h1>
        <p className="text-slate-500 text-sm mt-0.5">Business-ready data — aggregated and validated</p>
      </div>

      {/* Summary cards */}
      <div className="px-6 py-5 grid grid-cols-4 gap-4">
        <SummaryCard label="Total assets" value={assets.length.toString()} />
        <SummaryCard label="Total rows" value={totalRows.toLocaleString()} />
        <SummaryCard label="SLA breaches" value={breaches.toString()} alert={breaches > 0} />
        <SummaryCard label="Stale / critical" value={staleOrCritical.toString()} alert={staleOrCritical > 0} />
      </div>

      {/* Filters */}
      <div className="px-6 pb-4 flex gap-3">
        <select
          value={domain}
          onChange={(e) => setDomain(e.target.value)}
          className="input text-sm"
        >
          <option value="">All domains</option>
          {domains.map((d) => <option key={d} value={d}>{d}</option>)}
        </select>
      </div>

      {/* Asset grid */}
      {isLoading ? (
        <div className="flex items-center justify-center h-32 text-slate-500">Loading…</div>
      ) : (
        <div className="px-6 pb-6 grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
          {assets.map((a) => (
            <div
              key={a.asset_id}
              className="card hover:border-brand-600/50 transition-colors cursor-pointer group"
              onClick={() => navigate(`/?asset=${a.asset_id}`)}
            >
              <div className="flex items-start justify-between mb-3">
                <div className="min-w-0">
                  <p className="font-mono text-sm font-semibold text-slate-100 truncate">{a.name}</p>
                  <p className="text-xs text-slate-500 mt-0.5">{a.type}</p>
                </div>
                <FreshnessBadge status={a.freshness_status} />
              </div>

              {a.description && (
                <p className="text-xs text-slate-400 leading-relaxed mb-3 line-clamp-2">
                  {a.description}
                </p>
              )}

              <div className="flex items-center justify-between">
                <DomainBadge domain={a.domain} />
                <div className="text-right">
                  <p className="text-xs text-slate-500">rows</p>
                  <p className="text-sm font-semibold font-mono text-slate-200">
                    {a.row_count?.toLocaleString() ?? '—'}
                  </p>
                </div>
              </div>

              <div className="mt-3 pt-3 border-t border-slate-800 flex items-center justify-between">
                <span className="text-xs text-slate-500">{a.owner_name ?? 'Unowned'}</span>
                <button
                  onClick={(e) => { e.stopPropagation(); navigate(`/lineage/${a.asset_id}`) }}
                  className="text-xs text-brand-400 hover:text-brand-300 transition-colors opacity-0 group-hover:opacity-100"
                >
                  View lineage →
                </button>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

function SummaryCard({ label, value, alert }: { label: string; value: string; alert?: boolean }) {
  return (
    <div className="card">
      <p className="text-xs text-slate-500">{label}</p>
      <p className={`text-2xl font-bold font-mono mt-1 ${alert ? 'text-red-400' : 'text-slate-100'}`}>
        {value}
      </p>
    </div>
  )
}
