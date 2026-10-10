import '@xyflow/react/dist/style.css'

import {
  Background,
  type ColorMode,
  Controls,
  type Edge,
  type FitViewOptions,
  Handle,
  MarkerType,
  type Node,
  type NodeProps,
  type NodeTypes,
  PanOnScrollMode,
  Position,
  ReactFlow,
  type ReactFlowInstance,
} from '@xyflow/react'
import { Database, Gauge } from 'lucide-react'
import { type ReactNode, useEffect, useMemo, useRef, useSyncExternalStore } from 'react'
import type { ObservabilitySignal } from '@/api/client'
import { StatusBadge, type StatusTone } from '@/components/app/StatusBadge'
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from '@/components/ui/empty'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { activePiecesValue } from '@/lib/data-set-storage-health'
import { providerRegistryLabel } from '@/lib/provider-display'
import {
  bucketIssueTone,
  countLabel,
  dataSetDisplayLabel,
  freshnessLabel,
  observabilitySignalDetails,
  type StorageTopologyEdge,
  type StorageTopologyGraph,
  type StorageTopologyNode,
  type StorageTopologyNodeKind,
  type StorageTopologySelection,
  storageTopologyGraphLayout,
} from '@/lib/storage-topology'
import { cn } from '@/lib/utils'
import { TopologySignalBadge, UploadSpeedText } from './TopologyStatus'

type TopologyFlowNodeData = { topologyNode: StorageTopologyNode }
type LaneFlowNodeData = { label: string }
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

const fitViewOptions: FitViewOptions<FlowNode> = { padding: 0.04, minZoom: 0.12, maxZoom: 1.25 }

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
  const containerRef = useRef<HTMLDivElement>(null)
  const flowRef = useRef<ReactFlowInstance<FlowNode, FlowEdge> | null>(null)
  const hasNodes = graph.nodes.length > 0
  // A different set of nodes, such as after a filter change, starts from a fresh fit;
  // a refresh that keeps the same nodes keeps the operator's pan and zoom.
  const layoutKey = useMemo(() => graph.nodes.map((node) => node.id).join('|'), [graph.nodes])
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
        ariaLabel: node.data.path,
        sourcePosition: Position.Right,
        targetPosition: Position.Left,
      })),
    [graph.nodes, selection]
  )
  const laneNodes = useMemo<LaneFlowNode[]>(
    () => [
      laneHeaderNode('lane:buckets', 'Buckets', graph.buckets[0]?.x ?? storageTopologyGraphLayout.bucketX),
      laneHeaderNode('lane:data-sets', 'Data sets', graph.dataSets[0]?.x ?? storageTopologyGraphLayout.dataSetX),
      laneHeaderNode('lane:providers', 'Providers', graph.providers[0]?.x ?? storageTopologyGraphLayout.providerX),
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
          ariaLabel: edge.data.path,
          interactionWidth: 24,
          style: { stroke, strokeWidth: selection?.type === 'edge' && selection.id === edge.id ? 3 : 2 },
          markerEnd: { type: MarkerType.ArrowClosed, color: stroke },
        }
      }),
    [graph.edges, selection]
  )

  useEffect(() => {
    const container = containerRef.current
    if (!hasNodes || !container) return
    let frame = 0
    const observer = new ResizeObserver(() => {
      cancelAnimationFrame(frame)
      frame = requestAnimationFrame(() => {
        void flowRef.current?.fitView(fitViewOptions)
      })
    })
    observer.observe(container)
    return () => {
      cancelAnimationFrame(frame)
      observer.disconnect()
    }
  }, [hasNodes])

  if (!hasNodes) {
    return (
      <div className="flex h-full min-h-0 items-center justify-center p-6">
        <TopologyEmpty title="No topology" description="No observations match the current filters." />
      </div>
    )
  }

  return (
    <div className="flex h-full min-h-0 min-w-0 flex-col">
      <div ref={containerRef} className="min-h-0 flex-1">
        <ReactFlow<FlowNode, FlowEdge>
          key={layoutKey}
          nodes={flowNodes}
          edges={flowEdges}
          nodeTypes={graphNodeTypes}
          onInit={(instance) => {
            flowRef.current = instance
          }}
          fitView
          fitViewOptions={fitViewOptions}
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
  const issueCount = node.data.issueCount ?? 0
  return (
    <GraphNodeShell node={node} selected={selected}>
      <Handle type="source" position={Position.Right} className="opacity-0" />
      <GraphNodeTitle title={node.label}>
        <StatusBadge tone={bucketIssueTone(issueCount, node.tone)}>
          {issueCount > 0 ? countLabel(issueCount, 'issue') : 'Healthy'}
        </StatusBadge>
      </GraphNodeTitle>
      <GraphNodeLine>
        {countLabel(node.data.replicaCount ?? 0, 'replica')} ·{' '}
        {countLabel(node.data.providerIDs?.length ?? 0, 'provider')}
      </GraphNodeLine>
    </GraphNodeShell>
  )
}

function DataSetGraphNode({ data, selected }: TopologyFlowNodeProps) {
  const node = data.topologyNode
  const status = node.data.status ?? 'unknown'
  const signal = node.data.signal
  const problem = status === 'available' ? undefined : dataSetCardProblem(signal)
  return (
    <GraphNodeShell node={node} selected={selected}>
      <Handle type="target" position={Position.Left} className="opacity-0" />
      <Handle type="source" position={Position.Right} className="opacity-0" />
      <GraphNodeTitle title={node.label}>
        <TopologySignalBadge status={status} signal={signal} />
      </GraphNodeTitle>
      <GraphNodeLine>{dataSetDisplayLabel(node.data)}</GraphNodeLine>
      {problem ? (
        <GraphNodeLine className={toneTextClass(node.tone)}>{problem}</GraphNodeLine>
      ) : (
        <GraphNodeLine>
          Pieces:{' '}
          {activePiecesValue({
            active_piece_count: node.data.activePieceCount,
            has_active_pieces: node.data.hasActivePieces,
          })}
        </GraphNodeLine>
      )}
    </GraphNodeShell>
  )
}

function ProviderGraphNode({ data, selected }: TopologyFlowNodeProps) {
  const node = data.topologyNode
  const status = node.data.status ?? 'unknown'
  const registry = providerRegistryLabel(node.data.providerID ?? '')
  const identity = [node.label === registry ? undefined : registry, node.data.location].filter(Boolean).join(' · ')
  return (
    <GraphNodeShell node={node} selected={selected}>
      <Handle type="target" position={Position.Left} className="opacity-0" />
      <GraphNodeTitle title={node.label}>
        <TopologySignalBadge status={status} signal={node.data.signal} />
      </GraphNodeTitle>
      {identity && (
        <GraphNodeLine>
          {node.data.location ? (
            <Tooltip>
              <TooltipTrigger asChild>
                <span>{identity}</span>
              </TooltipTrigger>
              <TooltipContent>Provider-declared location</TooltipContent>
            </Tooltip>
          ) : (
            identity
          )}
        </GraphNodeLine>
      )}
      <GraphNodeLine icon={<Gauge className="size-3.5 shrink-0" aria-hidden="true" />}>
        <UploadSpeedText test={node.data.uploadSpeedTest} />
      </GraphNodeLine>
    </GraphNodeShell>
  )
}

function LaneHeaderGraphNode({ data }: LaneFlowNodeProps) {
  return (
    <div className="w-64 border-b border-border px-1 pb-2 text-sm font-semibold text-muted-foreground">
      {data.label}
    </div>
  )
}

function laneHeaderNode(id: string, label: string, x: number): LaneFlowNode {
  return {
    id,
    type: 'lane',
    position: { x, y: -70 },
    data: { label },
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
  children,
}: {
  node: StorageTopologyNode
  selected?: boolean
  children: ReactNode
}) {
  return (
    <div
      className={cn(
        'flex h-24 w-64 flex-col gap-1 rounded-md border bg-card p-2.5 text-sm shadow-xs',
        nodeToneClasses(node.tone),
        selected && 'ring-2 ring-ring'
      )}
    >
      {children}
    </div>
  )
}

function GraphNodeTitle({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div className="flex min-w-0 items-center justify-between gap-2">
      <span className="truncate font-medium">{title}</span>
      {children}
    </div>
  )
}

function GraphNodeLine({ icon, className, children }: { icon?: ReactNode; className?: string; children: ReactNode }) {
  return (
    <div className={cn('flex min-w-0 items-center gap-1.5 text-xs text-muted-foreground', className)}>
      {icon}
      <span className="truncate">{children}</span>
    </div>
  )
}

function dataSetCardProblem(signal?: ObservabilitySignal) {
  const [firstDetail, ...moreDetails] = observabilitySignalDetails(signal)
  if (firstDetail) return moreDetails.length > 0 ? `${firstDetail} (+${moreDetails.length})` : firstDetail
  return signal ? freshnessLabel(signal.freshness) : 'No state recorded'
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

function toneTextClass(tone: StatusTone) {
  switch (tone) {
    case 'warning':
      return 'text-[color:var(--status-warning)]'
    case 'danger':
      return 'text-[color:var(--status-danger)]'
    default:
      return undefined
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
