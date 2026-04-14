import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer,
} from 'recharts'
import {
  fetchRiskSummary, fetchCapitalCharge,
  fetchBacktesting, fetchPlat, fetchRfet,
  fetchDates, fetchTrend,
} from '../api'
import type { RiskSummaryRow, CapitalChargeRow, BacktestingRow, PlatRow, RfetRow } from '../types'

type Tab = 'summary' | 'capital' | 'backtesting' | 'plat' | 'rfet'

const TABS: { id: Tab; label: string; bcbs: string }[] = [
  { id: 'summary',     label: 'Risk Summary',   bcbs: '' },
  { id: 'capital',     label: 'Capital Charge', bcbs: '¶180-186' },
  { id: 'backtesting', label: 'Back-Testing',   bcbs: '¶351-368' },
  { id: 'plat',        label: 'PLAT',           bcbs: '¶329-345' },
  { id: 'rfet',        label: 'RFET',           bcbs: '¶76-80' },
]

function fmt(n: number | null | undefined, decimals = 2): string {
  if (n == null) return '—'
  return n.toLocaleString(undefined, { minimumFractionDigits: decimals, maximumFractionDigits: decimals })
}

function fmtM(n: number | null | undefined): string {
  if (n == null) return '—'
  return `$${(n / 1_000_000).toFixed(1)}M`
}

function ZoneBadge({ zone }: { zone: string | null }) {
  if (!zone) return <span className="text-slate-500">—</span>
  const cls =
    zone === 'GREEN' ? 'bg-emerald-500/15 text-emerald-400 border border-emerald-500/30' :
    zone === 'AMBER' ? 'bg-amber-500/15  text-amber-400  border border-amber-500/30'  :
                       'bg-red-500/15    text-red-400    border border-red-500/30'
  return <span className={`px-2 py-0.5 rounded text-xs font-semibold ${cls}`}>{zone}</span>
}

function PassBadge({ pass }: { pass: boolean | null }) {
  if (pass == null) return <span className="text-slate-500">—</span>
  return pass
    ? <span className="px-2 py-0.5 rounded text-xs font-semibold bg-emerald-500/15 text-emerald-400 border border-emerald-500/30">PASS</span>
    : <span className="px-2 py-0.5 rounded text-xs font-semibold bg-red-500/15 text-red-400 border border-red-500/30">FAIL</span>
}

// ── Trend chart ───────────────────────────────────────────────────────────────

function TrendChart() {
  const { data = [], isLoading } = useQuery({
    queryKey: ['risk-trend'],
    queryFn: fetchTrend,
    staleTime: 300_000,
  })

  if (isLoading) return <div className="card h-[200px] flex items-center justify-center text-slate-500 text-sm">Loading trend…</div>
  if (!data.length) return null

  const chartData = data.map((r) => ({
    date: r.calc_date.slice(5),   // MM-DD
    capital: r.total_capital_usd != null ? +(r.total_capital_usd / 1_000_000).toFixed(1) : 0,
    es:      r.total_es_scaled    != null ? +(r.total_es_scaled    / 1_000_000).toFixed(1) : 0,
  }))

  return (
    <div className="card">
      <p className="text-xs font-semibold text-slate-400 mb-4">
        Total Capital Charge — All Desks (USD M)
      </p>
      <ResponsiveContainer width="100%" height={180}>
        <AreaChart data={chartData} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
          <defs>
            <linearGradient id="capGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%"  stopColor="#6366f1" stopOpacity={0.35} />
              <stop offset="95%" stopColor="#6366f1" stopOpacity={0} />
            </linearGradient>
            <linearGradient id="esGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%"  stopColor="#06b6d4" stopOpacity={0.2} />
              <stop offset="95%" stopColor="#06b6d4" stopOpacity={0} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
          <XAxis
            dataKey="date"
            tick={{ fill: '#64748b', fontSize: 10 }}
            axisLine={false} tickLine={false}
            interval={Math.max(0, Math.floor(chartData.length / 8) - 1)}
          />
          <YAxis
            tick={{ fill: '#64748b', fontSize: 10 }}
            axisLine={false} tickLine={false}
            width={44}
            tickFormatter={(v) => `$${v}M`}
          />
          <Tooltip
            contentStyle={{ background: '#0f172a', border: '1px solid #334155', borderRadius: 8, fontSize: 11 }}
            labelStyle={{ color: '#94a3b8' }}
            formatter={(v: number, name: string) => [`$${v}M`, name === 'capital' ? 'Capital' : 'ES 97.5%']}
          />
          <Area type="monotone" dataKey="capital" stroke="#6366f1" strokeWidth={2} fill="url(#capGrad)" dot={false} activeDot={{ r: 3 }} />
          <Area type="monotone" dataKey="es"      stroke="#06b6d4" strokeWidth={1.5} fill="url(#esGrad)" dot={false} activeDot={{ r: 3 }} />
        </AreaChart>
      </ResponsiveContainer>
      <div className="flex gap-4 mt-2">
        <span className="flex items-center gap-1.5 text-xs text-slate-500"><span className="w-3 h-0.5 bg-indigo-500 inline-block" /> Capital Charge</span>
        <span className="flex items-center gap-1.5 text-xs text-slate-500"><span className="w-3 h-0.5 bg-cyan-500 inline-block" /> ES 97.5% Scaled</span>
      </div>
    </div>
  )
}

// ── Summary tab ───────────────────────────────────────────────────────────────

function SummaryTab() {
  const { data = [], isLoading } = useQuery({
    queryKey: ['risk-summary'],
    queryFn: () => fetchRiskSummary(),
  })

  const firm = data.find((r) => r.desk === 'FIRM')
  const desks = data.filter((r) => r.desk !== 'FIRM')
  const platFailing = desks.filter((r) => r.plat_pass === false).length

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <KpiCard label="Firm ES 97.5% (scaled)" value={fmtM(firm?.es_975_scaled)} />
        <KpiCard label="Firm Capital Charge" value={fmtM(firm?.capital_charge_usd)} highlight />
        <KpiCard label="Traffic Light" value={firm?.traffic_light_zone ?? '—'} zone={firm?.traffic_light_zone ?? null} />
        <KpiCard label="PLAT Failing Desks" value={String(platFailing)} alert={platFailing > 0} />
      </div>

      <TrendChart />

      {isLoading ? <Spinner /> : (
        <Table
          cols={['Desk', 'Risk Class', 'ES 97.5% 1d', 'ES Scaled', 'Capital Charge', 'Traffic Light', 'PLAT', 'Exceptions (250d)']}
          rows={desks.map((r) => [
            <span className="font-mono text-slate-200">{r.desk}</span>,
            <span className="text-slate-400">{r.risk_class ?? '—'}</span>,
            fmt(r.es_975_1d),
            fmt(r.es_975_scaled),
            fmtM(r.capital_charge_usd),
            <ZoneBadge zone={r.traffic_light_zone ?? null} />,
            <PassBadge pass={r.plat_pass} />,
            <span className={r.exception_count_250d != null && r.exception_count_250d >= 5 ? 'text-amber-400' : ''}>{r.exception_count_250d ?? '—'}</span>,
          ])}
        />
      )}
    </div>
  )
}

// ── Capital tab ───────────────────────────────────────────────────────────────

function CapitalTab() {
  const { data = [], isLoading } = useQuery({
    queryKey: ['risk-capital'],
    queryFn: () => fetchCapitalCharge(),
  })

  const total = data.reduce((s, r) => s + (r.capital_charge_usd ?? 0), 0)

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-3 gap-4">
        <KpiCard label="Total Capital Charge" value={fmtM(total)} highlight />
        <KpiCard label="Desks" value={String(data.filter(r => r.desk !== 'FIRM').length)} />
        <KpiCard label="Max Multiplier" value={fmt(Math.max(...data.map(r => r.capital_multiplier ?? 0)), 2)} />
      </div>
      {isLoading ? <Spinner /> : (
        <Table
          cols={['Desk', 'Risk Class', 'LH (days)', 'ES 1d', 'ES Scaled', 'Reg Floor', 'Capital Charge', 'Multiplier', 'Zone']}
          rows={data.map((r) => [
            <span className="font-mono text-slate-200">{r.desk}</span>,
            r.risk_class,
            String(r.liquidity_horizon),
            fmt(r.es_975_1d),
            fmt(r.es_975_scaled),
            fmtM(r.regulatory_floor),
            <span className="font-semibold text-slate-100">{fmtM(r.capital_charge_usd)}</span>,
            fmt(r.capital_multiplier),
            <ZoneBadge zone={r.traffic_light_zone ?? null} />,
          ])}
        />
      )}
    </div>
  )
}

// ── Back-testing tab ──────────────────────────────────────────────────────────

function BacktestingTab() {
  const { data: dates = [] } = useQuery({
    queryKey: ['backtesting-dates'],
    queryFn: () => fetchDates('backtesting'),
    staleTime: 300_000,
  })

  const [calcDate, setCalcDate] = useState('')
  const activeDate = calcDate || dates[0]

  const { data = [], isLoading } = useQuery({
    queryKey: ['risk-backtesting', activeDate],
    queryFn: () => fetchBacktesting(undefined, undefined, activeDate),
    enabled: !!activeDate,
  })

  const red   = data.filter((r) => r.traffic_light_zone === 'RED').length
  const amber = data.filter((r) => r.traffic_light_zone === 'AMBER').length

  return (
    <div className="space-y-6">
      <div className="flex items-start gap-4">
        <div className="grid grid-cols-3 gap-4 flex-1">
          <KpiCard label="RED zone desks"   value={String(red)}   alert={red > 0} />
          <KpiCard label="AMBER zone desks" value={String(amber)} alert={amber > 0} />
          <KpiCard label="GREEN zone desks" value={String(data.filter(r => r.traffic_light_zone === 'GREEN').length)} />
        </div>
        <div className="flex-shrink-0">
          <label className="block text-xs text-slate-500 mb-1">Calc Date</label>
          <select
            value={calcDate}
            onChange={(e) => setCalcDate(e.target.value)}
            className="input text-sm w-36"
          >
            {dates.map((d) => (
              <option key={d} value={d}>{d}</option>
            ))}
          </select>
        </div>
      </div>
      {isLoading ? <Spinner /> : (
        <Table
          cols={['Desk', 'VaR 99% 1d', 'Hyp PnL', 'Actual PnL', 'Hyp Exception', 'Act Exception', 'Exceptions (250d)', 'Zone', 'Multiplier']}
          rows={data.map((r) => [
            <span className="font-mono text-slate-200">{r.desk}</span>,
            fmt(r.var_99_1d),
            fmt(r.hypothetical_pnl),
            fmt(r.actual_pnl),
            r.hypothetical_exception ? <span className="text-red-400">Yes</span> : 'No',
            r.actual_exception ? <span className="text-red-400">Yes</span> : 'No',
            <span className={r.exception_count_250d != null && r.exception_count_250d >= 5 ? 'text-amber-400 font-semibold' : ''}>{r.exception_count_250d ?? '—'}</span>,
            <ZoneBadge zone={r.traffic_light_zone ?? null} />,
            fmt(r.capital_multiplier),
          ])}
        />
      )}
    </div>
  )
}

// ── PLAT tab ──────────────────────────────────────────────────────────────────

function PlatTab() {
  const { data = [], isLoading } = useQuery({
    queryKey: ['risk-plat'],
    queryFn: () => fetchPlat(),
  })

  const failing = data.filter((r) => !r.plat_pass).length

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-3 gap-4">
        <KpiCard label="Failing desks" value={String(failing)} alert={failing > 0} />
        <KpiCard label="Passing desks" value={String(data.filter(r => r.plat_pass).length)} />
        <KpiCard label="Avg UPL ratio" value={fmt(data.reduce((s,r) => s+(r.upl_ratio??0),0)/(data.length||1))} />
      </div>
      {isLoading ? <Spinner /> : (
        <Table
          cols={['Desk', 'PLAT', 'UPL Ratio', 'UPL Pass', 'Spearman', 'Spearman Pass', 'KS Stat', 'KS Pass', 'Obs Count', 'Notes']}
          rows={data.map((r) => [
            <span className="font-mono text-slate-200">{r.desk}</span>,
            <PassBadge pass={r.plat_pass} />,
            fmt(r.upl_ratio),
            <PassBadge pass={r.upl_pass} />,
            fmt(r.spearman_correlation),
            <PassBadge pass={r.spearman_pass} />,
            fmt(r.ks_statistic),
            <PassBadge pass={r.ks_pass} />,
            String(r.observation_count ?? '—'),
            <span className="text-xs text-slate-400">{r.notes ?? ''}</span>,
          ])}
        />
      )}
    </div>
  )
}

// ── RFET tab ──────────────────────────────────────────────────────────────────

function RfetTab() {
  const [riskClass, setRiskClass] = useState('')

  const { data = [], isLoading } = useQuery({
    queryKey: ['risk-rfet', riskClass],
    queryFn: () => fetchRfet(undefined, riskClass || undefined),
  })

  const classes = [...new Set(data.map((r) => r.risk_class))]
  const failing = data.filter((r) => !r.rfet_pass).length

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-3 gap-4">
        <KpiCard label="Failing risk factors" value={String(failing)} alert={failing > 0} />
        <KpiCard label="Eligible for IMA" value={String(data.filter(r => r.eligible_for_ima).length)} />
        <KpiCard label="Total risk factors" value={String(data.length)} />
      </div>

      <div className="flex gap-3">
        <select value={riskClass} onChange={(e) => setRiskClass(e.target.value)} className="input text-sm">
          <option value="">All risk classes</option>
          {classes.map((c) => <option key={c} value={c}>{c}</option>)}
        </select>
      </div>

      {isLoading ? <Spinner /> : (
        <Table
          cols={['Risk Factor', 'Risk Class', 'Obs 12m', '12m Pass', 'Obs 90d', '90d Pass', 'RFET', 'IMA Eligible', 'Staleness (days)', 'Failure Reason']}
          rows={data.map((r) => [
            <span className="font-mono text-sm text-slate-200">{r.risk_factor_id}</span>,
            <span className="text-slate-400">{r.risk_class}</span>,
            String(r.obs_12m_count),
            <PassBadge pass={r.obs_12m_pass} />,
            String(r.obs_90d_count),
            <PassBadge pass={r.obs_90d_pass} />,
            <PassBadge pass={r.rfet_pass} />,
            <PassBadge pass={r.eligible_for_ima} />,
            r.staleness_days != null ? String(r.staleness_days) : '—',
            <span className="text-xs text-red-400">{r.failure_reason ?? ''}</span>,
          ])}
        />
      )}
    </div>
  )
}

// ── Shared components ─────────────────────────────────────────────────────────

function KpiCard({ label, value, highlight, alert, zone }: {
  label: string; value: string; highlight?: boolean; alert?: boolean; zone?: string | null
}) {
  const valueClass =
    alert      ? 'text-red-400' :
    highlight  ? 'text-brand-400' :
    zone === 'RED'   ? 'text-red-400' :
    zone === 'AMBER' ? 'text-amber-400' :
    zone === 'GREEN' ? 'text-emerald-400' :
    'text-slate-100'

  return (
    <div className="card">
      <p className="text-xs text-slate-500">{label}</p>
      <p className={`text-2xl font-bold font-mono mt-1 ${valueClass}`}>{value}</p>
    </div>
  )
}

function Table({ cols, rows }: { cols: string[]; rows: (React.ReactNode)[][] }) {
  if (rows.length === 0) {
    return <div className="text-slate-500 text-sm py-6 text-center">No data for this period.</div>
  }
  return (
    <div className="overflow-x-auto rounded-lg border border-slate-800">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-slate-800 bg-slate-900/60">
            {cols.map((c) => (
              <th key={c} className="px-4 py-3 text-left text-xs font-semibold text-slate-400 whitespace-nowrap">{c}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i} className="border-b border-slate-800/60 hover:bg-slate-800/30 transition-colors">
              {row.map((cell, j) => (
                <td key={j} className="px-4 py-3 text-slate-300 whitespace-nowrap">{cell}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function Spinner() {
  return <div className="flex items-center justify-center h-32 text-slate-500">Loading…</div>
}

// ── Main view ─────────────────────────────────────────────────────────────────

export default function Risk() {
  const [tab, setTab] = useState<Tab>('summary')

  return (
    <div className="flex flex-col h-full overflow-auto">
      {/* Header */}
      <div className="px-6 py-5 border-b border-slate-800">
        <h1 className="text-xl font-semibold text-slate-100">FRTB IMA Risk Dashboard</h1>
        <p className="text-slate-500 text-sm mt-0.5">BCBS 457 — Internal Models Approach capital metrics</p>
      </div>

      {/* Tab bar */}
      <div className="px-6 border-b border-slate-800 flex gap-1 pt-3">
        {TABS.map(({ id, label, bcbs }) => (
          <button
            key={id}
            onClick={() => setTab(id)}
            className={`px-4 py-2 text-sm font-medium rounded-t transition-colors flex items-center gap-1.5 ${
              tab === id
                ? 'bg-slate-800 text-slate-100 border-b-2 border-brand-500'
                : 'text-slate-400 hover:text-slate-200'
            }`}
          >
            {label}
            {bcbs && <span className="text-xs text-slate-500">{bcbs}</span>}
          </button>
        ))}
      </div>

      {/* Tab content */}
      <div className="flex-1 px-6 py-6 overflow-auto">
        {tab === 'summary'     && <SummaryTab />}
        {tab === 'capital'     && <CapitalTab />}
        {tab === 'backtesting' && <BacktestingTab />}
        {tab === 'plat'        && <PlatTab />}
        {tab === 'rfet'        && <RfetTab />}
      </div>
    </div>
  )
}
