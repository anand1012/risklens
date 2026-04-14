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

export interface ChatMessage {
  id: string
  role: 'user' | 'assistant'
  content: string
  sources?: ChatSource[]
  streaming?: boolean
}
