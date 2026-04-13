export function LayerBadge({ layer }: { layer: string }) {
  const colors: Record<string, string> = {
    bronze: 'bg-amber-900/50 text-amber-400',
    silver: 'bg-slate-700 text-slate-300',
    gold:   'bg-yellow-900/50 text-yellow-400',
  }
  return (
    <span className={`badge ${colors[layer] ?? 'bg-slate-700 text-slate-400'}`}>
      {layer}
    </span>
  )
}

export function DomainBadge({ domain }: { domain: string }) {
  const colors: Record<string, string> = {
    risk:        'bg-red-900/50 text-red-400',
    market_data: 'bg-blue-900/50 text-blue-400',
    reference:   'bg-purple-900/50 text-purple-400',
    regulatory:  'bg-green-900/50 text-green-400',
    lineage:     'bg-teal-900/50 text-teal-400',
  }
  return (
    <span className={`badge ${colors[domain] ?? 'bg-slate-700 text-slate-400'}`}>
      {domain.replace('_', ' ')}
    </span>
  )
}

export function FreshnessBadge({ status }: { status: string | null }) {
  if (!status) return null
  const colors: Record<string, string> = {
    fresh:    'bg-green-900/50 text-green-400',
    stale:    'bg-yellow-900/50 text-yellow-400',
    critical: 'bg-red-900/50 text-red-400',
  }
  return (
    <span className={`badge ${colors[status] ?? 'bg-slate-700 text-slate-400'}`}>
      {status}
    </span>
  )
}

export function BreachBadge({ breach }: { breach: boolean | null }) {
  if (breach === null) return null
  return breach
    ? <span className="badge bg-red-900/50 text-red-400">SLA breach</span>
    : <span className="badge bg-green-900/50 text-green-400">On time</span>
}
