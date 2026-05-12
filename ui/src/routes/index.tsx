import { createFileRoute, Link } from '@tanstack/react-router'
import { CheckCircle2, ChevronRight, Database, FileBox, HardDrive, Loader2 } from 'lucide-react'
import { Bar, BarChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { PageHeader } from '@/components/app/PageHeader'
import { StatusBadge } from '@/components/app/StatusBadge'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { useOverview } from '@/hooks/queries'
import { attentionDisplayRows, overviewPipelineRows, type PipelineDisplayRow, workerHealthRows } from '@/lib/overview'
import { formatBytes, formatDuration, formatNumber } from '@/lib/utils'

export const Route = createFileRoute('/')({
  component: OverviewPage,
})

function OverviewPage() {
  const { data, isLoading, error } = useOverview()

  if (isLoading) {
    return (
      <div className="flex h-full items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (error || !data) {
    return <div className="flex h-full items-center justify-center text-destructive">Failed to load overview data</div>
  }

  const attentionRows = attentionDisplayRows({
    objects: data.objects.attention ?? { needs_attention: 0, unavailable: 0 },
    tasks: data.tasks.attention ?? { failed: 0, exhausted: 0 },
  })
  const pipelineRows = overviewPipelineRows(data.tasks.active_pipeline ?? [])
  const hasActiveTasks = pipelineRows.some((row) => row.total > 0)
  const workers = workerHealthRows(data.workers)
  const cachePercent = data.cache.max_bytes > 0 ? (data.cache.used_bytes / data.cache.max_bytes) * 100 : 0

  return (
    <div className="flex flex-col gap-6 p-6">
      <PageHeader title="Overview" />

      <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
        <StatCard icon={Database} label="Buckets" value={formatNumber(data.buckets.total)} />
        <StatCard icon={FileBox} label="Objects" value={formatNumber(data.objects.total)} />
        <StatCard icon={HardDrive} label="Storage" value={formatBytes(data.objects.total_size_bytes)} />
        <StatCard
          icon={HardDrive}
          label="Cache"
          value={`${cachePercent.toFixed(1)}%`}
          sub={`${formatBytes(data.cache.used_bytes)} / ${formatBytes(data.cache.max_bytes)}`}
        />
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Attention Needed</CardTitle>
          </CardHeader>
          <CardContent>
            {attentionRows.length > 0 ? (
              <div className="flex h-60 flex-col justify-center gap-3">
                {attentionRows.map((row) => (
                  <AttentionLinkRow key={row.key} row={row} />
                ))}
              </div>
            ) : (
              <div className="flex h-60 flex-col items-center justify-center gap-2 text-muted-foreground">
                <CheckCircle2 className="size-5 text-status-success" />
                <div className="text-sm text-foreground">No attention needed</div>
                <div className="max-w-full text-center text-xs">
                  Object failures 0 · Unavailable 0 · Failed tasks 0 · Retry limit reached 0
                </div>
              </div>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Active Task Pipeline</CardTitle>
          </CardHeader>
          <CardContent>
            {hasActiveTasks ? (
              <ResponsiveContainer width="100%" height={240}>
                <BarChart data={pipelineRows} layout="vertical" margin={{ top: 8, right: 24, bottom: 8, left: 8 }}>
                  <XAxis type="number" tick={{ fontSize: 12 }} allowDecimals={false} />
                  <YAxis type="category" dataKey="label" tick={{ fontSize: 12 }} width={72} />
                  <Tooltip content={<PipelineTooltip />} />
                  <Bar dataKey="total" fill="var(--chart-1)" radius={[0, 4, 4, 0]} />
                </BarChart>
              </ResponsiveContainer>
            ) : (
              <div className="flex h-60 items-center justify-center text-muted-foreground">No active tasks</div>
            )}
          </CardContent>
        </Card>
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Worker Health</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex flex-col gap-2">
              {workers.map((worker) => (
                <div key={worker.key} className="flex items-center justify-between">
                  <span className="text-sm">{worker.label}</span>
                  <StatusBadge tone={worker.healthy ? 'success' : 'danger'}>
                    {worker.healthy ? 'Healthy' : 'Unhealthy'}
                  </StatusBadge>
                </div>
              ))}
              {workers.length === 0 && <div className="text-sm text-muted-foreground">No workers registered</div>}
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>System Info</CardTitle>
          </CardHeader>
          <CardContent>
            <dl className="flex flex-col gap-2 text-sm">
              <div className="flex justify-between">
                <dt className="text-muted-foreground">Version</dt>
                <dd className="font-mono">{data.system.version}</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-muted-foreground">Commit</dt>
                <dd className="font-mono">{data.system.commit.substring(0, 8)}</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-muted-foreground">Uptime</dt>
                <dd>{formatDuration(data.system.uptime_seconds)}</dd>
              </div>
            </dl>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}

function AttentionLinkRow({ row }: { row: ReturnType<typeof attentionDisplayRows>[number] }) {
  const content = (
    <>
      <span className="min-w-0 truncate text-sm text-foreground">{row.label}</span>
      <span className="flex shrink-0 items-center gap-2">
        <StatusBadge tone={row.tone}>{formatNumber(row.value)}</StatusBadge>
        <ChevronRight className="size-4 text-muted-foreground transition-transform group-hover:translate-x-0.5" />
      </span>
    </>
  )
  const className =
    'group flex items-center justify-between gap-3 rounded-md border border-border px-3 py-2 transition-colors hover:bg-muted/50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring'

  if (row.target === 'tasks') {
    return (
      <Link
        to="/tasks"
        search={{ type: 'all', status: row.taskStatus }}
        className={className}
        aria-label={`${row.label}: ${formatNumber(row.value)}`}
      >
        {content}
      </Link>
    )
  }

  return (
    <Link to="/buckets" className={className} aria-label={`${row.label}: ${formatNumber(row.value)}`}>
      {content}
    </Link>
  )
}

interface PipelineTooltipProps {
  active?: boolean
  label?: string
  payload?: Array<{ payload?: PipelineDisplayRow }>
}

function PipelineTooltip({ active, payload, label }: PipelineTooltipProps) {
  if (!active) return null
  const row = payload?.[0]?.payload
  if (!row) return null
  return (
    <div className="rounded-md border border-border bg-popover px-3 py-2 text-xs text-popover-foreground shadow-md">
      <div className="mb-1 font-medium">{label}</div>
      <div className="grid grid-cols-[auto_auto] gap-x-3 gap-y-1">
        <span className="text-muted-foreground">Queued</span>
        <span className="text-right">{formatNumber(row.queued)}</span>
        <span className="text-muted-foreground">Scheduled</span>
        <span className="text-right">{formatNumber(row.scheduled)}</span>
        <span className="text-muted-foreground">Waiting</span>
        <span className="text-right">{formatNumber(row.waiting)}</span>
        <span className="text-muted-foreground">Running</span>
        <span className="text-right">{formatNumber(row.running)}</span>
      </div>
    </div>
  )
}

function StatCard({
  icon: Icon,
  label,
  value,
  sub,
}: {
  icon: React.ElementType
  label: string
  value: string
  sub?: string
}) {
  return (
    <Card>
      <CardContent>
        <div className="flex items-center gap-2 text-muted-foreground">
          <Icon className="size-4" />
          <span className="text-sm">{label}</span>
        </div>
        <div className="mt-2 text-2xl font-bold">{value}</div>
        {sub && <div className="mt-1 text-xs text-muted-foreground">{sub}</div>}
      </CardContent>
    </Card>
  )
}
