import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { fetchSla, fetchOwnership, fetchQuality } from '../api'
import { DomainBadge, LayerBadge, FreshnessBadge, BreachBadge } from '../components/Badges'
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell,
} from 'recharts'

type Tab = 'sla' | 'quality' | 'ownership'

export default function Governance() {
  const [tab, setTab] = useState<Tab>('sla')
  const [breachOnly, setBreachOnly] = useState(false)

  return (
    <div className="flex flex-col h-full overflow-auto">
      <div className="px-6 py-5 border-b border-slate-800">
        <h1 className="text-xl font-semibold text-slate-100">Governance</h1>
        <p className="text-slate-500 text-sm mt-0.5">SLA health, data quality, and ownership</p>
      </div>

      {/* Tabs */}
      <div className="px-6 pt-4 flex gap-1 border-b border-slate-800">
        {(['sla', 'quality', 'ownership'] as Tab[]).map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={`px-4 py-2 text-sm font-medium rounded-t-lg transition-colors capitalize ${
              tab === t
                ? 'bg-slate-800 text-slate-100 border-b-2 border-brand-500'
                : 'text-slate-500 hover:text-slate-300'
            }`}
          >
            {t === 'sla' ? 'SLA Status' : t === 'quality' ? 'Data Quality' : 'Ownership'}
          </button>
        ))}
      </div>

      <div className="flex-1 overflow-auto px-6 py-5">
        {tab === 'sla' && <SlaTab breachOnly={breachOnly} setBreachOnly={setBreachOnly} />}
        {tab === 'quality' && <QualityTab />}
        {tab === 'ownership' && <OwnershipTab />}
      </div>
    </div>
  )
}

function SlaTab({ breachOnly, setBreachOnly }: { breachOnly: boolean; setBreachOnly: (v: boolean) => void }) {
  const { data = [], isLoading } = useQuery({
    queryKey: ['sla', breachOnly],
    queryFn: () => fetchSla(breachOnly),
  })

  const breaches = data.filter((r) => r.breach_flag).length
  const total = data.length

  return (
    <div className="space-y-5">
      {/* Summary cards */}
      <div className="grid grid-cols-3 gap-4">
        <StatCard label="Total assets" value={total} />
        <StatCard label="SLA breaches" value={breaches} danger={breaches > 0} />
        <StatCard label="On time" value={total - breaches} good />
      </div>

      <div className="flex items-center gap-3">
        <label className="flex items-center gap-2 text-sm text-slate-400 cursor-pointer">
          <input
            type="checkbox"
            checked={breachOnly}
            onChange={(e) => setBreachOnly(e.target.checked)}
            className="rounded border-slate-600 bg-slate-800 text-brand-500"
          />
          Breaches only
        </label>
      </div>

      {isLoading ? (
        <div className="text-slate-500 text-sm">Loading…</div>
      ) : (
        <table className="w-full text-sm">
          <thead>
            <tr className="text-left text-xs text-slate-500 uppercase tracking-wider border-b border-slate-800">
              <th className="pb-2 pr-4">Asset</th>
              <th className="pb-2 pr-4">Domain</th>
              <th className="pb-2 pr-4">Layer</th>
              <th className="pb-2 pr-4">Status</th>
              <th className="pb-2 pr-4">Breach duration</th>
              <th className="pb-2">Checked</th>
            </tr>
          </thead>
          <tbody>
            {data.map((r) => (
              <tr key={r.asset_id} className="border-b border-slate-800/60 hover:bg-slate-800/30">
                <td className="py-2.5 pr-4 font-mono text-xs text-slate-200">{r.name}</td>
                <td className="py-2.5 pr-4"><DomainBadge domain={r.domain} /></td>
                <td className="py-2.5 pr-4"><LayerBadge layer={r.layer} /></td>
                <td className="py-2.5 pr-4"><BreachBadge breach={r.breach_flag} /></td>
                <td className="py-2.5 pr-4 text-slate-400 text-xs">
                  {r.breach_duration_mins != null ? `${r.breach_duration_mins} min` : '—'}
                </td>
                <td className="py-2.5 text-slate-500 text-xs">
                  {r.checked_at ? new Date(r.checked_at).toLocaleString() : '—'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  )
}

function QualityTab() {
  const { data = [], isLoading } = useQuery({
    queryKey: ['quality'],
    queryFn: () => fetchQuality(),
  })

  const chartData = ['fresh', 'stale', 'critical'].map((status) => ({
    name: status,
    count: data.filter((d) => d.freshness_status === status).length,
  }))
  const chartColors: Record<string, string> = {
    fresh: '#4ade80', stale: '#facc15', critical: '#f87171',
  }

  return (
    <div className="space-y-6">
      {/* Freshness chart */}
      <div className="card">
        <h3 className="text-sm font-semibold text-slate-300 mb-4">Freshness Distribution</h3>
        <ResponsiveContainer width="100%" height={160}>
          <BarChart data={chartData} barSize={40}>
            <XAxis dataKey="name" tick={{ fill: '#94a3b8', fontSize: 12 }} axisLine={false} tickLine={false} />
            <YAxis tick={{ fill: '#94a3b8', fontSize: 12 }} axisLine={false} tickLine={false} />
            <Tooltip
              contentStyle={{ background: '#1e293b', border: '1px solid #334155', borderRadius: 8 }}
              labelStyle={{ color: '#f1f5f9' }}
              itemStyle={{ color: '#94a3b8' }}
            />
            <Bar dataKey="count" radius={[4, 4, 0, 0]}>
              {chartData.map((entry) => (
                <Cell key={entry.name} fill={chartColors[entry.name]} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>

      {isLoading ? (
        <div className="text-slate-500 text-sm">Loading…</div>
      ) : (
        <table className="w-full text-sm">
          <thead>
            <tr className="text-left text-xs text-slate-500 uppercase tracking-wider border-b border-slate-800">
              <th className="pb-2 pr-4">Asset</th>
              <th className="pb-2 pr-4">Domain</th>
              <th className="pb-2 pr-4">Freshness</th>
              <th className="pb-2 pr-4">Null rate</th>
              <th className="pb-2 pr-4">Dup rate</th>
              <th className="pb-2">Schema drift</th>
            </tr>
          </thead>
          <tbody>
            {data.map((r) => (
              <tr key={r.asset_id} className="border-b border-slate-800/60 hover:bg-slate-800/30">
                <td className="py-2.5 pr-4 font-mono text-xs text-slate-200">{r.name}</td>
                <td className="py-2.5 pr-4"><DomainBadge domain={r.domain} /></td>
                <td className="py-2.5 pr-4"><FreshnessBadge status={r.freshness_status} /></td>
                <td className="py-2.5 pr-4 text-xs text-slate-400">
                  {r.null_rate != null ? `${(r.null_rate * 100).toFixed(1)}%` : '—'}
                </td>
                <td className="py-2.5 pr-4 text-xs text-slate-400">
                  {r.duplicate_rate != null ? `${(r.duplicate_rate * 100).toFixed(1)}%` : '—'}
                </td>
                <td className="py-2.5 text-xs">
                  {r.schema_drift != null
                    ? r.schema_drift
                      ? <span className="text-red-400">⚠ Yes</span>
                      : <span className="text-green-400">✓ No</span>
                    : '—'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  )
}

function OwnershipTab() {
  const [team, setTeam] = useState('')
  const { data = [], isLoading } = useQuery({
    queryKey: ['ownership', team],
    queryFn: () => fetchOwnership(team || undefined),
  })

  const teams = [...new Set(data.map((r) => r.team).filter(Boolean))]

  return (
    <div className="space-y-4">
      <div className="flex gap-3">
        <select
          value={team}
          onChange={(e) => setTeam(e.target.value)}
          className="input text-sm"
        >
          <option value="">All teams</option>
          {teams.map((t) => <option key={t} value={t}>{t}</option>)}
        </select>
      </div>

      {isLoading ? (
        <div className="text-slate-500 text-sm">Loading…</div>
      ) : (
        <table className="w-full text-sm">
          <thead>
            <tr className="text-left text-xs text-slate-500 uppercase tracking-wider border-b border-slate-800">
              <th className="pb-2 pr-4">Asset</th>
              <th className="pb-2 pr-4">Domain</th>
              <th className="pb-2 pr-4">Layer</th>
              <th className="pb-2 pr-4">Owner</th>
              <th className="pb-2 pr-4">Team</th>
              <th className="pb-2">Steward</th>
            </tr>
          </thead>
          <tbody>
            {data.map((r) => (
              <tr key={r.asset_id} className="border-b border-slate-800/60 hover:bg-slate-800/30">
                <td className="py-2.5 pr-4 font-mono text-xs text-slate-200">{r.name}</td>
                <td className="py-2.5 pr-4"><DomainBadge domain={r.domain} /></td>
                <td className="py-2.5 pr-4"><LayerBadge layer={r.layer} /></td>
                <td className="py-2.5 pr-4 text-slate-300 text-xs">{r.owner_name ?? '—'}</td>
                <td className="py-2.5 pr-4 text-slate-400 text-xs">{r.team ?? '—'}</td>
                <td className="py-2.5 text-slate-500 text-xs">{r.steward ?? '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  )
}

function StatCard({ label, value, danger, good }: {
  label: string; value: number; danger?: boolean; good?: boolean
}) {
  return (
    <div className="card">
      <p className="text-xs text-slate-500">{label}</p>
      <p className={`text-3xl font-bold mt-1 ${danger ? 'text-red-400' : good ? 'text-green-400' : 'text-slate-100'}`}>
        {value}
      </p>
    </div>
  )
}
