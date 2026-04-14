import { useState, useCallback, useMemo } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  Handle,
  useNodesState,
  useEdgesState,
  type Node,
  type Edge,
  type NodeProps,
  type Connection,
  MarkerType,
  Position,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import dagre from '@dagrejs/dagre'
import { fetchLineageGraph } from '../api'
import type { LineageNode } from '../types'

// ── Constants ───────────────────────────────────────────────────────────────
const NODE_W = 210
const NODE_H = 70

const LAYER_BORDER: Record<string, string> = {
  bronze: '#b45309',
  silver: '#475569',
  gold:   '#d97706',
}

const TYPE_ICON: Record<string, string> = {
  source: '◉', pipeline: '⬡', table: '◻', report: '◈', model: '⬠',
}

const LAYER_COLOR: Record<string, string> = {
  bronze: '#fbbf24',
  silver: '#94a3b8',
  gold:   '#fcd34d',
}

// ── Custom node (handles on left + right for LR flow) ──────────────────────
function LineageNodeComponent({ data }: NodeProps) {
  const node = data.node as LineageNode
  return (
    <>
      <Handle
        type="target"
        position={Position.Left}
        style={{ background: '#6366f1', width: 8, height: 8, border: '2px solid #0f172a' }}
      />
      <div className="flex flex-col gap-1.5 px-3 py-2.5">
        <div className="flex items-center gap-1.5">
          <span style={{ color: '#818cf8', fontSize: 13 }}>{TYPE_ICON[node.type] ?? '◻'}</span>
          <span className="font-mono text-xs font-semibold text-slate-100 leading-tight" style={{ maxWidth: 148, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            {node.name}
          </span>
        </div>
        <div className="flex items-center gap-1.5">
          <span className="text-[10px] font-medium" style={{ color: LAYER_COLOR[node.layer] ?? '#64748b' }}>
            {node.layer}
          </span>
          <span className="text-[10px] text-slate-600">·</span>
          <span className="text-[10px] text-slate-500">{node.domain}</span>
        </div>
      </div>
      <Handle
        type="source"
        position={Position.Right}
        style={{ background: '#6366f1', width: 8, height: 8, border: '2px solid #0f172a' }}
      />
    </>
  )
}

const nodeTypes = { lineageNode: LineageNodeComponent }

// ── Dagre LR layout ─────────────────────────────────────────────────────────
function applyDagreLayout(
  nodes: LineageNode[],
  rawEdges: import('../types').LineageEdge[],
): { flowNodes: Node[]; flowEdges: Edge[] } {
  const g = new dagre.graphlib.Graph()
  g.setDefaultEdgeLabel(() => ({}))
  g.setGraph({ rankdir: 'LR', nodesep: 60, ranksep: 120, marginx: 40, marginy: 40 })

  nodes.forEach((n) => g.setNode(n.node_id, { width: NODE_W, height: NODE_H }))
  rawEdges.forEach((e) => g.setEdge(e.from_node_id, e.to_node_id))
  dagre.layout(g)

  const flowNodes: Node[] = nodes.map((n) => {
    const pos = g.node(n.node_id)
    return {
      id: n.node_id,
      type: 'lineageNode',
      position: { x: pos.x - NODE_W / 2, y: pos.y - NODE_H / 2 },
      data: { node: n },
      style: {
        background: '#0f172a',
        border: `1.5px solid ${LAYER_BORDER[n.layer] ?? '#334155'}`,
        borderRadius: 10,
        color: '#f1f5f9',
        width: NODE_W,
        minHeight: NODE_H,
        padding: 0,
      },
    }
  })

  const flowEdges: Edge[] = rawEdges.map((e) => ({
    id: e.edge_id,
    source: e.from_node_id,
    target: e.to_node_id,
    type: 'smoothstep',
    animated: true,
    label: e.relationship,
    labelStyle: { fill: '#475569', fontSize: 10, fontFamily: 'monospace' },
    labelBgStyle: { fill: '#0f172a', fillOpacity: 0.9 },
    style: { stroke: '#6366f1', strokeWidth: 1.5 },
    markerEnd: { type: MarkerType.ArrowClosed, color: '#6366f1', width: 14, height: 14 },
  }))

  return { flowNodes, flowEdges }
}

// ── Main view ───────────────────────────────────────────────────────────────
export default function Lineage() {
  const { assetId } = useParams<{ assetId: string }>()
  const navigate = useNavigate()
  const [hops, setHops] = useState(2)
  const [searchId, setSearchId] = useState(assetId ?? '')
  const [activeId, setActiveId] = useState(assetId ?? '')

  const { data: graph, isLoading, isError } = useQuery({
    queryKey: ['lineage', activeId, hops],
    queryFn: () => fetchLineageGraph(activeId, hops),
    enabled: !!activeId,
  })

  const { flowNodes, flowEdges } = useMemo(
    () => graph ? applyDagreLayout(graph.nodes, graph.edges) : { flowNodes: [], flowEdges: [] },
    [graph],
  )

  const [nodes, , onNodesChange] = useNodesState(flowNodes)
  const [edges, , onEdgesChange] = useEdgesState(flowEdges)

  const rfNodes = graph ? flowNodes : nodes
  const rfEdges = graph ? flowEdges : edges

  const onConnect = useCallback((_c: Connection) => {}, [])

  return (
    <div className="flex flex-col" style={{ height: '100%', minHeight: '100vh' }}>
      {/* Header */}
      <div className="px-6 py-5 border-b border-slate-800 flex items-center gap-4">
        <div>
          <h1 className="text-xl font-semibold text-slate-100">Data Lineage</h1>
          {activeId && <p className="text-slate-500 text-xs mt-0.5 font-mono">{activeId}</p>}
        </div>
        <div className="flex items-center gap-3 ml-auto">
          <input
            className="input w-56 text-sm font-mono"
            placeholder="Asset / node ID…"
            value={searchId}
            onChange={(e) => setSearchId(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter') {
                setActiveId(searchId)
                navigate(`/lineage/${searchId}`)
              }
            }}
          />
          <div className="flex items-center gap-2">
            <span className="text-xs text-slate-500">Hops</span>
            {[1, 2, 3, 4].map((n) => (
              <button
                key={n}
                onClick={() => setHops(n)}
                className={`w-7 h-7 rounded text-xs font-medium transition-colors ${
                  hops === n ? 'bg-brand-600 text-white' : 'bg-slate-800 text-slate-400 hover:bg-slate-700'
                }`}
              >
                {n}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Canvas */}
      <div className="flex-1 relative" style={{ minHeight: 0, height: '100%' }}>
        {isLoading && (
          <div className="absolute inset-0 flex items-center justify-center text-slate-500 z-10">
            Loading lineage graph…
          </div>
        )}
        {isError && (
          <div className="absolute inset-0 flex items-center justify-center text-red-400 z-10">
            Node not found or no lineage data.
          </div>
        )}
        {!activeId && (
          <div className="absolute inset-0 flex items-center justify-center text-slate-500 z-10">
            Enter an asset ID above to explore lineage.
          </div>
        )}

        <ReactFlow
          nodes={rfNodes}
          edges={rfEdges}
          nodeTypes={nodeTypes}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          fitView
          fitViewOptions={{ padding: 0.15 }}
          className="bg-slate-950"
          proOptions={{ hideAttribution: true }}
        >
          <Background color="#1e293b" gap={24} size={1} />
          <Controls className="[&>button]:bg-slate-800 [&>button]:border-slate-700 [&>button]:text-slate-300" />
          <MiniMap
            nodeColor={(n) => {
              const b = (n.style?.border as string) ?? ''
              return b.includes('#b45309') ? '#92400e' : b.includes('#d97706') ? '#78350f' : '#334155'
            }}
            className="bg-slate-900 border border-slate-800 rounded-xl"
          />
        </ReactFlow>

        {graph && (
          <div className="absolute top-4 left-4 card text-xs space-y-1 pointer-events-none">
            <div className="text-slate-500 font-semibold mb-2">Graph stats</div>
            <div className="text-slate-400">{graph.nodes.length} nodes · {graph.edges.length} edges</div>
            <div className="text-slate-400">{hops} hop{hops > 1 ? 's' : ''} radius</div>
          </div>
        )}
      </div>
    </div>
  )
}
