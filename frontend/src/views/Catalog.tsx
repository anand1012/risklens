import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import { fetchAssets, fetchAsset, fetchSchema } from '../api'
import { LayerBadge, DomainBadge, FreshnessBadge, BreachBadge } from '../components/Badges'
import type { AssetDetail, SchemaColumn } from '../types'

const DOMAINS = ['', 'risk', 'market_data', 'reference', 'regulatory']
const LAYERS  = ['', 'bronze', 'silver', 'gold']
const TYPES   = ['', 'table', 'feed', 'report', 'model']

export default function Catalog() {
  const navigate = useNavigate()
  const [domain, setDomain] = useState('')
  const [layer, setLayer]   = useState('')
  const [type, setType]     = useState('')
  const [search, setSearch] = useState('')
  const [selectedId, setSelectedId] = useState<string | null>(null)

  const { data: assets = [], isLoading } = useQuery({
    queryKey: ['assets', domain, layer, type],
    queryFn: () => fetchAssets({ domain: domain || undefined, layer: layer || undefined, type: type || undefined }),
  })

  const filtered = search
    ? assets.filter((a) =>
        a.name.toLowerCase().includes(search.toLowerCase()) ||
        a.description?.toLowerCase().includes(search.toLowerCase())
      )
    : assets

  return (
    <div className="flex h-full">
      {/* Table pane */}
      <div className="flex-1 flex flex-col overflow-hidden">
        {/* Header */}
        <div className="px-6 py-5 border-b border-slate-800">
          <h1 className="text-xl font-semibold text-slate-100">Data Catalog</h1>
          <p className="text-sm text-slate-500 mt-0.5">{filtered.length} assets</p>
        </div>

        {/* Filters */}
        <div className="px-6 py-3 border-b border-slate-800 flex gap-3 flex-wrap">
          <input
            className="input w-64"
            placeholder="Search assets…"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
          <Select value={domain} onChange={setDomain} options={DOMAINS} label="Domain" />
          <Select value={layer}  onChange={setLayer}  options={LAYERS}  label="Layer" />
          <Select value={type}   onChange={setType}   options={TYPES}   label="Type" />
        </div>

        {/* Table */}
        <div className="flex-1 overflow-auto">
          {isLoading ? (
            <div className="flex items-center justify-center h-32 text-slate-500">Loading…</div>
          ) : (
            <table className="w-full text-sm">
              <thead className="sticky top-0 bg-slate-900 z-10">
                <tr className="text-left text-slate-500 text-xs uppercase tracking-wider border-b border-slate-800">
                  <th className="px-6 py-3">Name</th>
                  <th className="px-3 py-3">Domain</th>
                  <th className="px-3 py-3">Layer</th>
                  <th className="px-3 py-3">Owner</th>
                  <th className="px-3 py-3">Freshness</th>
                  <th className="px-3 py-3">SLA</th>
                  <th className="px-3 py-3 text-right">Rows</th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((a) => (
                  <tr
                    key={a.asset_id}
                    className={`table-row ${selectedId === a.asset_id ? 'bg-slate-800' : ''}`}
                    onClick={() => setSelectedId(a.asset_id)}
                  >
                    <td className="px-6 py-3">
                      <div className="font-mono text-slate-100 text-xs">{a.name}</div>
                      {a.description && (
                        <div className="text-slate-500 text-xs mt-0.5 truncate max-w-xs">
                          {a.description}
                        </div>
                      )}
                    </td>
                    <td className="px-3 py-3"><DomainBadge domain={a.domain} /></td>
                    <td className="px-3 py-3"><LayerBadge layer={a.layer} /></td>
                    <td className="px-3 py-3 text-slate-400 text-xs">{a.owner_name ?? '—'}</td>
                    <td className="px-3 py-3"><FreshnessBadge status={a.freshness_status} /></td>
                    <td className="px-3 py-3"><BreachBadge breach={a.breach_flag} /></td>
                    <td className="px-3 py-3 text-right text-slate-400 text-xs font-mono">
                      {a.row_count?.toLocaleString() ?? '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>

      {/* Detail drawer */}
      {selectedId && (
        <AssetDrawer
          assetId={selectedId}
          onClose={() => setSelectedId(null)}
          onLineage={(id) => navigate(`/lineage/${id}`)}
        />
      )}
    </div>
  )
}

function Select({
  value, onChange, options, label,
}: { value: string; onChange: (v: string) => void; options: string[]; label: string }) {
  return (
    <select
      value={value}
      onChange={(e) => onChange(e.target.value)}
      className="input text-sm"
    >
      {options.map((o) => (
        <option key={o} value={o}>{o || `All ${label}s`}</option>
      ))}
    </select>
  )
}

function AssetDrawer({
  assetId, onClose, onLineage,
}: { assetId: string; onClose: () => void; onLineage: (id: string) => void }) {
  const { data: asset, isLoading: loadingAsset } = useQuery({
    queryKey: ['asset', assetId],
    queryFn: () => fetchAsset(assetId),
  })
  const { data: schema = [] } = useQuery({
    queryKey: ['schema', assetId],
    queryFn: () => fetchSchema(assetId),
  })

  return (
    <aside className="w-96 flex-shrink-0 border-l border-slate-800 bg-slate-900 flex flex-col overflow-hidden">
      <div className="flex items-center justify-between px-5 py-4 border-b border-slate-800">
        <span className="font-semibold text-slate-100 text-sm">Asset Detail</span>
        <button onClick={onClose} className="btn-ghost text-lg leading-none">✕</button>
      </div>

      {loadingAsset ? (
        <div className="flex items-center justify-center h-24 text-slate-500 text-sm">Loading…</div>
      ) : asset ? (
        <div className="flex-1 overflow-y-auto">
          <DrawerSection title="Overview">
            <Row label="Name"><span className="font-mono text-xs">{asset.name}</span></Row>
            <Row label="Type">{asset.type}</Row>
            <Row label="Domain"><DomainBadge domain={asset.domain} /></Row>
            <Row label="Layer"><LayerBadge layer={asset.layer} /></Row>
            {asset.description && (
              <p className="text-slate-400 text-xs leading-relaxed mt-2">{asset.description}</p>
            )}
            {asset.tags?.length > 0 && (
              <div className="flex flex-wrap gap-1 mt-2">
                {asset.tags.map((t) => (
                  <span key={t} className="badge bg-slate-700 text-slate-300">{t}</span>
                ))}
              </div>
            )}
          </DrawerSection>

          <DrawerSection title="Ownership">
            <Row label="Owner">{asset.owner_name ?? '—'}</Row>
            <Row label="Team">{asset.team ?? '—'}</Row>
            <Row label="Steward">{asset.steward ?? '—'}</Row>
            {asset.email && <Row label="Email"><a href={`mailto:${asset.email}`} className="text-brand-400 text-xs">{asset.email}</a></Row>}
          </DrawerSection>

          <DrawerSection title="Quality">
            <Row label="Freshness"><FreshnessBadge status={asset.freshness_status} /></Row>
            <Row label="Null rate">{asset.null_rate != null ? `${(asset.null_rate * 100).toFixed(1)}%` : '—'}</Row>
            <Row label="Duplicate rate">{asset.duplicate_rate != null ? `${(asset.duplicate_rate * 100).toFixed(1)}%` : '—'}</Row>
            <Row label="Schema drift">{asset.schema_drift != null ? (asset.schema_drift ? '⚠ Yes' : '✓ No') : '—'}</Row>
          </DrawerSection>

          <DrawerSection title="SLA">
            <Row label="Status"><BreachBadge breach={asset.breach_flag} /></Row>
            {asset.breach_duration_mins != null && (
              <Row label="Breach duration">{asset.breach_duration_mins} min</Row>
            )}
          </DrawerSection>

          {schema.length > 0 && (
            <DrawerSection title={`Schema (${schema.length} columns)`}>
              <SchemaTable columns={schema} />
            </DrawerSection>
          )}

          <div className="px-5 pb-5">
            <button
              onClick={() => onLineage(assetId)}
              className="btn-primary w-full justify-center"
            >
              View Lineage →
            </button>
          </div>
        </div>
      ) : null}
    </aside>
  )
}

function DrawerSection({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="px-5 py-4 border-b border-slate-800">
      <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-3">{title}</h3>
      {children}
    </div>
  )
}

function Row({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex justify-between items-center py-1">
      <span className="text-xs text-slate-500">{label}</span>
      <span className="text-xs text-slate-300">{children}</span>
    </div>
  )
}

function SchemaTable({ columns }: { columns: SchemaColumn[] }) {
  return (
    <div className="overflow-x-auto -mx-1">
      <table className="w-full text-xs">
        <thead>
          <tr className="text-slate-500 text-left border-b border-slate-800">
            <th className="pb-1 pr-2">Column</th>
            <th className="pb-1 pr-2">Type</th>
            <th className="pb-1">Nullable</th>
          </tr>
        </thead>
        <tbody>
          {columns.map((c) => (
            <tr key={c.column_name} className="border-b border-slate-800/50">
              <td className="py-1 pr-2 font-mono text-slate-200">{c.column_name}</td>
              <td className="py-1 pr-2 text-slate-400">{c.data_type}</td>
              <td className="py-1 text-slate-500">{c.nullable ? 'Y' : 'N'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
