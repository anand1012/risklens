import { useState, useCallback } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  addEdge,
  type Node,
  type Edge,
  type Connection,
  MarkerType,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { fetchLineageGraph } from '../api'
import type { LineageNode } from '../types'
import { LayerBadge, DomainBadge } from '../components/Badges'

const LAYER_COLOR: Record<string, string> = {
  bronze: '#92400e',
  silver: '#334155',
  gold:   '#78350f',
}
const TYPE_SHAPE: Record<string, string> = {
  source:   '◉',
  pipeline: '⬡',
  table:    '◻',
  report:   '◈',
}

function toFlow(nodes: LineageNode[], rawEdges: import('../types').LineageEdge[]) {
  const cols = 3
  const flowNodes: Node[] = nodes.map((n, i) => ({
    id: n.node_id,
    position: { x: (i % cols) * 280, y: Math.floor(i / cols) * 130 },
    data: { label: <NodeLabel node={n} /> },
    style: {
      background: LAYER_COLOR[n.layer] ?? '#1e293b',
      border: '1px solid #334155',
      borderRadius: 10,
      padding: '8px 12px',
      color: '#f1f5f9',
      fontSize: 12,
      minWidth: 180,
    },
  }))

  const flowEdges: Edge[] = rawEdges.map((e) => ({
    id: e.edge_id,
    source: e.from_node_id,
    target: e.to_node_id,
    label: e.relationship,
    animated: true,
    style: { stroke: '#6366f1', strokeWidth: 1.5 },
    labelStyle: { fill: '#94a3b8', fontSize: 10 },
    markerEnd: { type: MarkerType.ArrowClosed, color: '#6366f1' },
  }))

  return { flowNodes, flowEdges }
}

function NodeLabel({ node }: { node: LineageNode }) {
  return (
    <div>
      <div className="flex items-center gap-1.5 mb-1">
        <span className="text-brand-400">{TYPE_SHAPE[node.type] ?? '◻'}</span>
        <span className="font-mono text-xs font-semibold text-slate-100 truncate max-w-[140px]">{node.name}</span>
      </div>
      <div className="flex gap-1">
        <span className="badge bg-slate-700/80 text-slate-400 text-[10px]">{node.layer}</span>
        <span className="badge bg-slate-700/80 text-slate-400 text-[10px]">{node.domain}</span>
      </div>
    </div>
  )
}

export default function Lineage() {
  // ReactFlow needs an explicit pixel height on its container — flex-1 alone isn't enough
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

  const { flowNodes, flowEdges } = graph
    ? toFlow(graph.nodes, graph.edges)
    : { flowNodes: [], flowEdges: [] }

  const [nodes, , onNodesChange] = useNodesState(flowNodes)
  const [edges, , onEdgesChange] = useEdgesState(flowEdges)

  // Re-init when graph changes
  const rfNodes = graph ? flowNodes : nodes
  const rfEdges = graph ? flowEdges : edges

  const onConnect = useCallback((c: Connection) => {}, [])

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

      {/* Flow canvas — needs explicit height for ReactFlow to render */}
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
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          fitView
          className="bg-slate-950"
          proOptions={{ hideAttribution: true }}
        >
          <Background color="#334155" gap={24} size={1} />
          <Controls className="[&>button]:bg-slate-800 [&>button]:border-slate-700 [&>button]:text-slate-300" />
          <MiniMap
            nodeColor={(n) => (n.style?.background as string) ?? '#1e293b'}
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
