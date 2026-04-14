import type {
  Asset, AssetDetail, SchemaColumn,
  LineageGraph, SlaRecord, OwnershipRecord, QualityScore, SearchResult,
  RiskSummaryRow, CapitalChargeRow, BacktestingRow, PlatRow, RfetRow, EsRow,
} from './types'

const BASE = '/api'

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`)
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
  return res.json() as Promise<T>
}

// Catalog
export const fetchAssets = (params?: {
  domain?: string; layer?: string; type?: string; limit?: number
}) => {
  const q = new URLSearchParams()
  if (params?.domain) q.set('domain', params.domain)
  if (params?.layer) q.set('layer', params.layer)
  if (params?.type) q.set('type', params.type)
  if (params?.limit) q.set('limit', String(params.limit))
  return get<Asset[]>(`/assets${q.size ? `?${q}` : ''}`)
}

export const fetchAsset = (id: string) => get<AssetDetail>(`/assets/${id}`)
export const fetchSchema = (id: string) => get<SchemaColumn[]>(`/assets/${id}/schema`)

// Lineage
export const fetchLineageGraph = (assetId: string, hops = 2) =>
  get<LineageGraph>(`/lineage/graph/${assetId}?hops=${hops}`)

// Governance
export const fetchSla = (breachesOnly = false) =>
  get<SlaRecord[]>(`/governance/sla${breachesOnly ? '?breaches_only=true' : ''}`)

export const fetchOwnership = (team?: string) =>
  get<OwnershipRecord[]>(`/governance/ownership${team ? `?team=${team}` : ''}`)

export const fetchQuality = (params?: { freshness_status?: string; schema_drift?: boolean }) => {
  const q = new URLSearchParams()
  if (params?.freshness_status) q.set('freshness_status', params.freshness_status)
  if (params?.schema_drift !== undefined) q.set('schema_drift', String(params.schema_drift))
  return get<QualityScore[]>(`/governance/quality${q.size ? `?${q}` : ''}`)
}

// FRTB Risk
export const fetchRiskSummary  = (trade_date?: string, desk?: string) => {
  const q = new URLSearchParams()
  if (trade_date) q.set('trade_date', trade_date)
  if (desk)       q.set('desk', desk)
  return get<RiskSummaryRow[]>(`/risk/summary${q.size ? `?${q}` : ''}`)
}
export const fetchCapitalCharge = (trade_date?: string, risk_class?: string) => {
  const q = new URLSearchParams()
  if (trade_date)  q.set('trade_date', trade_date)
  if (risk_class)  q.set('risk_class', risk_class)
  return get<CapitalChargeRow[]>(`/risk/capital${q.size ? `?${q}` : ''}`)
}
export const fetchBacktesting = (trade_date?: string, zone?: string) => {
  const q = new URLSearchParams()
  if (trade_date) q.set('trade_date', trade_date)
  if (zone)       q.set('zone', zone)
  return get<BacktestingRow[]>(`/risk/backtesting${q.size ? `?${q}` : ''}`)
}
export const fetchPlat = (trade_date?: string) => {
  const q = new URLSearchParams()
  if (trade_date) q.set('trade_date', trade_date)
  return get<PlatRow[]>(`/risk/plat${q.size ? `?${q}` : ''}`)
}
export const fetchRfet = (rfet_date?: string, risk_class?: string) => {
  const q = new URLSearchParams()
  if (rfet_date)  q.set('rfet_date', rfet_date)
  if (risk_class) q.set('risk_class', risk_class)
  return get<RfetRow[]>(`/risk/rfet${q.size ? `?${q}` : ''}`)
}
export const fetchEs = (trade_date?: string, risk_class?: string) => {
  const q = new URLSearchParams()
  if (trade_date) q.set('trade_date', trade_date)
  if (risk_class) q.set('risk_class', risk_class)
  return get<EsRow[]>(`/risk/es${q.size ? `?${q}` : ''}`)
}

// Search
export const fetchSearch = (query: string, topK = 10) =>
  get<SearchResult[]>(`/search?q=${encodeURIComponent(query)}&top_k=${topK}`)

// Chat (SSE)
export function streamChat(
  query: string,
  onSource: (sources: import('./types').ChatSource[]) => void,
  onToken: (token: string) => void,
  onDone: () => void,
  onError: (err: Error) => void,
  topK = 8,
): () => void {
  let cancelled = false

  fetch(`${BASE}/chat`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query, top_k: topK }),
  })
    .then(async (res) => {
      if (!res.ok || !res.body) throw new Error(`Chat API error ${res.status}`)
      const reader = res.body.getReader()
      const decoder = new TextDecoder()
      let buf = ''

      while (true) {
        const { done, value } = await reader.read()
        if (done || cancelled) break
        buf += decoder.decode(value, { stream: true })

        const lines = buf.split('\n')
        buf = lines.pop() ?? ''

        for (const line of lines) {
          if (!line.startsWith('data: ')) continue
          const payload = line.slice(6)
          if (payload === '__done__') { onDone(); return }
          if (payload.startsWith('__sources__')) {
            try { onSource(JSON.parse(payload.slice(11))) } catch {}
          } else {
            // Tokens are JSON-encoded on the backend to preserve newlines
            try { onToken(JSON.parse(payload)) } catch { onToken(payload) }
          }
        }
      }
    })
    .catch((e) => { if (!cancelled) onError(e) })

  return () => { cancelled = true }
}
