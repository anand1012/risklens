export interface Asset {
  asset_id: string
  name: string
  type: string
  domain: string
  layer: string
  description: string
  tags: string[]
  row_count: number
  size_bytes: number
  updated_at: string
  owner_name: string
  team: string
  freshness_status: 'fresh' | 'stale' | 'critical' | null
  null_rate: number | null
  breach_flag: boolean | null
}

export interface AssetDetail extends Asset {
  steward: string
  email: string
  schema_drift: boolean | null
  duplicate_rate: number | null
  last_checked: string | null
  expected_refresh: string | null
  actual_refresh: string | null
  breach_duration_mins: number | null
}

export interface SchemaColumn {
  column_name: string
  data_type: string
  nullable: boolean
  description: string | null
  sample_value: string | null
}

export interface LineageNode {
  node_id: string
  name: string
  type: string
  domain: string
  layer: string
  metadata: Record<string, unknown> | null
}

export interface EdgeStory {
  title: string
  what: string
  business_impact: string
  frequency: string
  owner: string
}

export interface LineageEdge {
  edge_id: string
  from_node_id: string
  to_node_id: string
  relationship: string
  pipeline_job: string | null
  story?: EdgeStory | null
}

export interface LineageGraph {
  root_id: string
  hops: number
  nodes: LineageNode[]
  edges: LineageEdge[]
}

export interface SlaRecord {
  asset_id: string
  name: string
  domain: string
  layer: string
  expected_refresh: string | null
  actual_refresh: string | null
  breach_flag: boolean
  breach_duration_mins: number | null
  checked_at: string | null
}

export interface OwnershipRecord {
  asset_id: string
  name: string
  domain: string
  layer: string
  owner_name: string
  team: string
  steward: string | null
  email: string | null
  assigned_date: string | null
}

export interface QualityScore {
  asset_id: string
  name: string
  domain: string
  layer: string
  null_rate: number | null
  schema_drift: boolean | null
  freshness_status: 'fresh' | 'stale' | 'critical' | null
  duplicate_rate: number | null
  last_checked: string | null
}

export interface SearchResult {
  chunk_id: string
  asset_id: string
  source_type: string
  domain: string
  score: number
  name: string
  snippet: string
}

export interface ChatSource {
  chunk_id: string
  asset_id: string
  source_type: string
  domain: string
  score: number
  name: string
}

// ── FRTB IMA Risk types ───────────────────────────────────────────────────────

export interface RiskSummaryRow {
  calc_date: string
  desk: string
  risk_class: string | null
  liquidity_horizon: number | null
  var_99_1d: number | null
  var_99_10d: number | null
  traffic_light_zone: 'GREEN' | 'AMBER' | 'RED' | null
  exception_count_250d: number | null
  es_975_1d: number | null
  es_975_10d: number | null
  es_975_scaled: number | null
  plat_pass: boolean | null
  upl_ratio: number | null
  spearman_correlation: number | null
  ks_statistic: number | null
  plat_notes: string | null
  capital_charge_usd: number | null
  capital_multiplier: number | null
  trade_date: string
}

export interface CapitalChargeRow {
  calc_date: string
  desk: string
  risk_class: string
  liquidity_horizon: number
  es_975_1d: number | null
  es_975_scaled: number | null
  traffic_light_zone: 'GREEN' | 'AMBER' | 'RED' | null
  capital_multiplier: number | null
  regulatory_floor: number | null
  capital_charge_usd: number | null
  exception_count_250d: number | null
  trade_date: string
}

export interface BacktestingRow {
  calc_date: string
  desk: string
  var_99_1d: number | null
  hypothetical_pnl: number | null
  actual_pnl: number | null
  hypothetical_exception: number
  actual_exception: number
  exception_count_250d: number | null
  traffic_light_zone: 'GREEN' | 'AMBER' | 'RED' | null
  capital_multiplier: number | null
  method: string | null
  trade_date: string
}

export interface PlatRow {
  calc_date: string
  desk: string
  observation_count: number | null
  upl_ratio: number | null
  upl_pass: boolean | null
  spearman_correlation: number | null
  spearman_pass: boolean | null
  ks_statistic: number | null
  ks_pass: boolean | null
  plat_pass: boolean | null
  notes: string | null
  trade_date: string
}

export interface RfetRow {
  rfet_date: string
  risk_factor_id: string
  risk_class: string
  obs_12m_count: number
  obs_90d_count: number
  obs_12m_pass: boolean
  obs_90d_pass: boolean
  rfet_pass: boolean
  eligible_for_ima: boolean
  last_observation_date: string | null
  staleness_days: number | null
  failure_reason: string | null
}

export interface EsRow {
  calc_date: string
  desk: string
  risk_class: string
  liquidity_horizon: number
  es_975_1d: number | null
  es_975_10d: number | null
  es_975_scaled: number | null
  trade_date: string
}

export interface ChatMessage {
  id: string
  role: 'user' | 'assistant'
  content: string
  sources?: ChatSource[]
  streaming?: boolean
}
