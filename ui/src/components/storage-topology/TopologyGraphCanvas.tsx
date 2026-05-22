import '@xyflow/react/dist/style.css'

import {
  Background,
  type ColorMode,
  Controls,
  type Edge,
  Handle,
  MarkerType,
  type Node,
  type NodeProps,
  type NodeTypes,
  PanOnScrollMode,
  Position,
  ReactFlow,
} from '@xyflow/react'
import { Database } from 'lucide-react'
import { type ReactNode, useMemo, useSyncExternalStore } from 'react'
import { StatusBadge, type StatusTone } from '@/components/app/StatusBadge'
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from '@/components/ui/empty'
import {
  bucketIssueTone,
  dataSetDisplayLabel,
  observabilityStatusTone,
  type StorageTopologyEdge,
  type StorageTopologyGraph,
  type StorageTopologyNode,
  type StorageTopologyNodeKind,
  type StorageTopologySelection,
  storageTopologyGraphLayout,
} from '@/lib/storage-topology'
import { cn, formatNumber } from '@/lib/utils'

type TopologyFlowNodeData = { topologyNode: StorageTopologyNode }
type LaneFlowNodeData = { label: string; className: string }
type FlowEdgeData = { topologyEdge: StorageTopologyEdge }
type TopologyFlowNode = Node<TopologyFlowNodeData, StorageTopologyNodeKind>
type LaneFlowNode = Node<LaneFlowNodeData, 'lane'>
type FlowNode = TopologyFlowNode | LaneFlowNode
type FlowEdge = Edge<FlowEdgeData, 'smoothstep'>
type TopologyFlowNodeProps = NodeProps<TopologyFlowNode>
type LaneFlowNodeProps = NodeProps<LaneFlowNode>

const graphNodeTypes = {
  bucket: BucketGraphNode,
  'data-set': DataSetGraphNode,
  provider: ProviderGraphNode,
  lane: LaneHeaderGraphNode,
} as NodeTypes

export default function TopologyGraphCanvas({
  graph,
  selection,
  onSelectionChange,
}: {
  graph: StorageTopologyGraph
  selection: StorageTopologySelection | null
  onSelectionChange: (selection: StorageTopologySelection | null) => void
}) {
  const colorMode = useReactFlowColorMode()
  const topologyNodes = useMemo<TopologyFlowNode[]>(
    () =>
      graph.nodes.map((node) => ({
        id: node.id,
        type: node.kind,
        position: { x: node.x, y: node.y },
        data: { topologyNode: node },
        selected: selection?.type === 'node' && selection.id === node.id,
        draggable: false,
        connectable: false,
        selectable: true,
        sourcePosition: Position.Right,
        targetPosition: Position.Left,
      })),
    [graph.nodes, selection]
  )
  const laneNodes = useMemo<LaneFlowNode[]>(
    () => [
      laneHeaderNode('lane:buckets', 'Buckets', graph.buckets[0]?.x ?? storageTopologyGraphLayout.bucketX, 'w-56'),
      laneHeaderNode(
        'lane:data-sets',
        'Data Sets',
        graph.dataSets[0]?.x ?? storageTopologyGraphLayout.dataSetX,
        'w-64'
      ),
      laneHeaderNode(
        'lane:providers',
        'Providers',
        graph.providers[0]?.x ?? storageTopologyGraphLayout.providerX,
        'w-56'
      ),
    ],
    [graph.buckets, graph.dataSets, graph.providers]
  )
  const flowNodes = useMemo<FlowNode[]>(() => [...laneNodes, ...topologyNodes], [laneNodes, topologyNodes])
  const flowEdges = useMemo<FlowEdge[]>(
    () =>
      graph.edges.map((edge) => {
        const stroke = toneStroke(edge.tone)
        return {
          id: edge.id,
          type: 'smoothstep',
          source: edge.source,
          target: edge.target,
          data: { topologyEdge: edge },
          selected: selection?.type === 'edge' && selection.id === edge.id,
          selectable: true,
          reconnectable: false,
          focusable: true,
          interactionWidth: 24,
          style: { stroke, strokeWidth: selection?.type === 'edge' && selection.id === edge.id ? 3 : 2 },
          markerEnd: { type: MarkerType.ArrowClosed, color: stroke },
        }
      }),
    [graph.edges, selection]
  )

  if (graph.nodes.length === 0) {
    return (
      <div className="flex h-full min-h-0 items-center justify-center p-6">
        <TopologyEmpty title="No topology" description="No observations match the current filters." />
      </div>
    )
  }

  return (
    <div className="flex h-full min-h-0 min-w-0 flex-col">
      <div className="min-h-0 flex-1">
        <ReactFlow<FlowNode, FlowEdge>
          nodes={flowNodes}
          edges={flowEdges}
          nodeTypes={graphNodeTypes}
          fitView
          fitViewOptions={{ padding: 0.04, minZoom: 0.12, maxZoom: 1.25 }}
          minZoom={0.12}
          maxZoom={1.8}
          colorMode={colorMode}
          nodesDraggable={false}
          nodesConnectable={false}
          edgesReconnectable={false}
          connectOnClick={false}
          elementsSelectable
          panOnDrag
          panOnScroll
          panOnScrollMode={PanOnScrollMode.Vertical}
          zoomOnScroll={false}
          zoomOnPinch
          zoomOnDoubleClick={false}
          onPaneClick={() => onSelectionChange(null)}
          onNodeClick={(_, node) => {
            if (isTopologyFlowNode(node)) {
              onSelectionChange({
                type: 'node',
                id: node.data.topologyNode.id,
                kind: node.data.topologyNode.kind,
              })
            }
          }}
          onEdgeClick={(_, edge) => {
            if (edge.data?.topologyEdge) {
              onSelectionChange({ type: 'edge', id: edge.data.topologyEdge.id, kind: edge.data.topologyEdge.kind })
            }
          }}
          proOptions={{ hideAttribution: true }}
          className="bg-muted/10"
        >
          <Background color="var(--border)" gap={20} />
          <Controls showInteractive={false} />
        </ReactFlow>
      </div>
    </div>
  )
}

function BucketGraphNode({ data, selected }: TopologyFlowNodeProps) {
  const node = data.topologyNode
  return (
    <GraphNodeShell node={node} selected={selected} className="h-24 w-56 gap-1.5 p-2.5">
      <Handle type="source" position={Position.Right} className="opacity-0" />
      <GraphNodeHeader eyebrow="Bucket" title={node.label} />
      <div className="flex flex-wrap gap-1">
        <StatusBadge tone={bucketIssueTone(node.data.issueCount ?? 0, node.tone)}>
          {issueCountLabel(node.data.issueCount ?? 0)}
        </StatusBadge>
        <StatusBadge tone="neutral">{formatNumber(node.data.replicaCount ?? 0)} replicas</StatusBadge>
      </div>
    </GraphNodeShell>
  )
}

function DataSetGraphNode({ data, selected }: TopologyFlowNodeProps) {
  const node = data.topologyNode
  return (
    <GraphNodeShell node={node} selected={selected} className="h-24 w-64 gap-1.5 p-2.5">
      <Handle type="target" position={Position.Left} className="opacity-0" />
      <Handle type="source" position={Position.Right} className="opacity-0" />
      <GraphNodeHeader eyebrow={node.label} title={dataSetDisplayLabel(node.data)} />
      <div className="flex flex-wrap gap-1">
        <StatusBadge tone={observabilityStatusTone(node.data.status ?? 'unknown')}>{node.data.status}</StatusBadge>
      </div>
    </GraphNodeShell>
  )
}

function ProviderGraphNode({ data, selected }: TopologyFlowNodeProps) {
  const node = data.topologyNode
  return (
    <GraphNodeShell node={node} selected={selected} className="h-24 w-56 gap-1.5 p-2.5">
      <Handle type="target" position={Position.Left} className="opacity-0" />
      <GraphNodeHeader eyebrow="Provider" title={`#${node.data.providerID}`} />
      <div className="flex flex-wrap gap-1">
        <StatusBadge tone={observabilityStatusTone(node.data.status ?? 'unknown')}>{node.data.status}</StatusBadge>
      </div>
    </GraphNodeShell>
  )
}

function LaneHeaderGraphNode({ data }: LaneFlowNodeProps) {
  return (
    <div
      className={cn(
        'rounded-md border bg-card/90 px-4 py-2 text-sm font-semibold text-muted-foreground shadow-xs',
        data.className
      )}
    >
      {data.label}
    </div>
  )
}

function laneHeaderNode(id: string, label: string, x: number, className: string): LaneFlowNode {
  return {
    id,
    type: 'lane',
    position: { x, y: -70 },
    data: { label, className },
    draggable: false,
    connectable: false,
    selectable: false,
    focusable: false,
  }
}

function isTopologyFlowNode(node: FlowNode): node is TopologyFlowNode {
  return 'topologyNode' in node.data
}

function useReactFlowColorMode(): ColorMode {
  return useSyncExternalStore(subscribeDocumentColorMode, readDocumentColorMode, () => 'light')
}

function subscribeDocumentColorMode(onStoreChange: () => void) {
  if (typeof document === 'undefined') return () => {}
  const observer = new MutationObserver(onStoreChange)
  observer.observe(document.documentElement, { attributes: true, attributeFilter: ['class'] })
  return () => observer.disconnect()
}

function readDocumentColorMode(): ColorMode {
  if (typeof document === 'undefined') return 'light'
  return document.documentElement.classList.contains('dark') ? 'dark' : 'light'
}

function GraphNodeShell({
  node,
  selected,
  className,
  children,
}: {
  node: StorageTopologyNode
  selected?: boolean
  className?: string
  children: ReactNode
}) {
  return (
    <div
      className={cn(
        'flex flex-col gap-2 rounded-md border bg-card p-3 text-sm shadow-xs',
        nodeToneClasses(node.tone),
        selected && 'ring-2 ring-ring',
        className
      )}
    >
      {children}
    </div>
  )
}

function GraphNodeHeader({ eyebrow, title }: { eyebrow: string; title: string }) {
  return (
    <div className="flex min-w-0 flex-col gap-1">
      <span className="text-xs text-muted-foreground">{eyebrow}</span>
      <span className="truncate font-medium">{title}</span>
    </div>
  )
}

function nodeToneClasses(tone: StatusTone) {
  switch (tone) {
    case 'success':
      return 'border-[color:var(--status-success-border)] bg-[var(--status-success-bg)]'
    case 'warning':
      return 'border-[color:var(--status-warning-border)] bg-[var(--status-warning-bg)]'
    case 'danger':
      return 'border-[color:var(--status-danger-border)] bg-[var(--status-danger-bg)]'
    case 'info':
      return 'border-[color:var(--status-info-border)] bg-[var(--status-info-bg)]'
    case 'neutral':
      return 'border-border bg-card'
  }
}

function toneStroke(tone: StatusTone) {
  switch (tone) {
    case 'success':
      return 'var(--status-success)'
    case 'warning':
      return 'var(--status-warning)'
    case 'danger':
      return 'var(--status-danger)'
    case 'info':
      return 'var(--status-info)'
    case 'neutral':
      return 'var(--muted-foreground)'
  }
}

function TopologyEmpty({ title, description }: { title: string; description: string }) {
  return (
    <Empty className="min-h-56 border">
      <EmptyHeader>
        <EmptyMedia variant="icon">
          <Database />
        </EmptyMedia>
        <EmptyTitle>{title}</EmptyTitle>
        <EmptyDescription>{description}</EmptyDescription>
      </EmptyHeader>
    </Empty>
  )
}

function issueCountLabel(count: number) {
  return `${formatNumber(count)} ${count === 1 ? 'issue' : 'issues'}`
}
