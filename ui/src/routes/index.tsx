import { useQueryClient } from '@tanstack/react-query'
import { createFileRoute, Link } from '@tanstack/react-router'
import { CheckCircle2, ChevronRight, Database, FileBox, HardDrive, MemoryStick } from 'lucide-react'
import type { ElementType } from 'react'
import { Bar, BarChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import type { OverviewData } from '@/api/client'
import { PageHeader, RefreshButton } from '@/components/app/PageHeader'
import { PageError, PageLoading } from '@/components/app/PageState'
import { StatusBadge } from '@/components/app/StatusBadge'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { useOverview } from '@/hooks/queries'
import {
  attentionDisplayRows,
  filecoinStorageHealthCheckedLabel,
  filecoinStorageHealthDisplayLevel,
  filecoinStorageHealthLevelStyle,
  filecoinStorageHealthPartialErrorRows,
  filecoinStorageHealthStatusLabel,
  filecoinStorageHealthSummaryRow,
  overviewPipelineRows,
  type PipelineDisplayRow,
  workerHealthRows,
} from '@/lib/overview'
import { formatBytes, formatDuration, formatNumber } from '@/lib/utils'

export const Route = createFileRoute('/')({
  component: OverviewPage,
})

function OverviewPage() {
  const { data, isLoading, error, refetch, isFetching } = useOverview()
  const qc = useQueryClient()
  const header = (
    <PageHeader
      title="Overview"
      actions={
        <RefreshButton onClick={() => qc.invalidateQueries({ queryKey: ['overview'] })} refreshing={isFetching} />
      }
    />
  )

  if (isLoading || error || !data) {
    return (
      <div className="flex flex-col gap-6 p-6">
        {header}
        {isLoading ? (
          <PageLoading />
        ) : (
          <PageError
            title="Failed to load overview"
            description={error?.message}
            onRetry={() => refetch()}
            retrying={isFetching}
          />
        )}
      </div>
    )
  }

  const attentionRows = attentionDisplayRows({
    objects: data.objects.attention ?? { needs_attention: 0, unavailable: 0 },
    tasks: data.tasks.attention ?? { failed: 0 },
  })
  const pipelineRows = overviewPipelineRows(data.tasks.active_pipeline ?? [])
  const hasActiveTasks = pipelineRows.some((row) => row.total > 0)
  const workers = workerHealthRows(data.workers)
  const cachePercent = data.cache.max_bytes > 0 ? (data.cache.used_bytes / data.cache.max_bytes) * 100 : 0

  return (
    <div className="flex flex-col gap-6 p-6">
      {header}

      <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
        <StatCard icon={Database} label="Buckets" value={formatNumber(data.buckets.total)} to="/buckets" />
        <StatCard icon={FileBox} label="Objects" value={formatNumber(data.objects.total)} />
        <StatCard icon={HardDrive} label="Storage" value={formatBytes(data.objects.total_size_bytes)} />
        <StatCard
          icon={MemoryStick}
          label="Cache"
          value={`${cachePercent.toFixed(1)}%`}
          sub={`${formatBytes(data.cache.used_bytes)} / ${formatBytes(data.cache.max_bytes)}`}
        />
      </div>

      <StorageHealthCard health={data.filecoin_storage_health} />

      <div className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Attention needed</CardTitle>
          </CardHeader>
          <CardContent>
            {attentionRows.length > 0 ? (
              <div className="flex flex-col gap-3">
                {attentionRows.map((row) => (
                  <AttentionLinkRow key={row.key} row={row} />
                ))}
              </div>
            ) : (
              <div className="flex min-h-24 items-center justify-center gap-2 text-sm text-muted-foreground">
                <CheckCircle2 className="size-4 text-status-success" />
                Nothing needs attention
              </div>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Active task pipeline</CardTitle>
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
              <div className="flex min-h-24 items-center justify-center text-sm text-muted-foreground">
                No active tasks
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Worker health</CardTitle>
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
            <CardTitle>System info</CardTitle>
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

function StorageHealthCard({ health }: { health: OverviewData['filecoin_storage_health'] }) {
  const partialErrors = filecoinStorageHealthPartialErrorRows(health.partial_errors ?? {})
  const providers = filecoinStorageHealthSummaryRow('providers', 'Providers', health.providers)
  const dataSets = filecoinStorageHealthSummaryRow('data_sets', 'Data sets', health.data_sets)
  const healthStyle = filecoinStorageHealthLevelStyle(filecoinStorageHealthDisplayLevel(health))
  return (
    <Card size="sm">
      <CardContent>
        <div className="grid min-w-0 gap-6 text-xs lg:grid-cols-2 lg:items-center">
          <div className="grid min-w-0 gap-2">
            <div className="flex min-w-0 items-baseline justify-between gap-4">
              <CardTitle className="truncate">Storage health</CardTitle>
              <span className={`shrink-0 font-medium ${healthStyle.textClassName}`}>
                {filecoinStorageHealthStatusLabel(health)}
              </span>
            </div>
            <div className="grid min-w-0 grid-cols-[auto_minmax(0,1fr)_auto] items-baseline gap-3">
              <span className="text-muted-foreground">Data sets</span>
              <span className="truncate font-semibold text-foreground">
                {formatOptionalNumber(dataSets.available)} / {formatOptionalNumber(dataSets.total)} ready
              </span>
              <span className="font-semibold text-foreground">{formatOptionalPercent(dataSets.readyPercent)}</span>
            </div>
            <div
              role="progressbar"
              aria-label="Data set readiness"
              aria-valuemin={dataSets.readyPercent != null ? 0 : undefined}
              aria-valuemax={dataSets.readyPercent != null ? 100 : undefined}
              aria-valuenow={dataSets.readyPercent ?? undefined}
              className="h-2 min-w-0 overflow-hidden rounded-full bg-muted"
            >
              <div
                className={`h-full rounded-full ${healthStyle.progressClassName}`}
                style={{ width: `${dataSets.readyPercent ?? 0}%` }}
              />
            </div>
          </div>

          <div className="grid min-w-0 gap-2">
            <CompactMetric
              label="Providers"
              value={`${formatOptionalNumber(providers.available)} / ${formatOptionalNumber(providers.total)} available`}
            />
            <div className="flex min-w-0 flex-wrap items-baseline gap-x-4 gap-y-1">
              <span className="shrink-0 text-muted-foreground">Data set states</span>
              <div className="flex min-w-0 flex-wrap gap-x-4 gap-y-1">
                <InlineMetric label="Unavailable" value={dataSets.unavailable} tone="danger" />
                <InlineMetric label="Degraded" value={dataSets.degraded} />
                <InlineMetric label="Unknown" value={dataSets.unknown} />
              </div>
              <div className="ml-auto">
                <CompactMetric label="Checked" value={filecoinStorageHealthCheckedLabel(health)} align="end" />
              </div>
            </div>
          </div>

          {partialErrors.length > 0 && (
            <div className="flex flex-wrap gap-1 lg:col-span-2">
              {partialErrors.map((error) => (
                <StatusBadge key={error.key} tone="warning">
                  {error.label}
                </StatusBadge>
              ))}
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  )
}

function CompactMetric({
  label,
  value,
  suffix,
  align = 'start',
}: {
  label: string
  value: string
  suffix?: string
  align?: 'start' | 'end'
}) {
  return (
    <div className={`flex min-w-0 items-baseline gap-2 ${align === 'end' ? 'justify-end' : ''}`}>
      <span className="shrink-0 text-muted-foreground">{label}</span>
      <span className="truncate font-semibold text-foreground">{value}</span>
      {suffix && <span className="shrink-0 font-semibold text-foreground">{suffix}</span>}
    </div>
  )
}

function InlineMetric({
  label,
  value,
  tone = 'neutral',
}: {
  label: string
  value: number | null
  tone?: 'neutral' | 'warning' | 'danger'
}) {
  const effectiveTone = value != null && value > 0 ? tone : 'neutral'
  const toneClassName =
    effectiveTone === 'danger'
      ? 'text-[color:var(--status-danger)]'
      : effectiveTone === 'warning'
        ? 'text-[color:var(--status-warning)]'
        : 'text-muted-foreground'
  return (
    <span className={toneClassName}>
      {label} <span className="font-medium text-foreground">{formatOptionalNumber(value)}</span>
    </span>
  )
}

function formatOptionalNumber(value: number | null) {
  return value == null ? '—' : formatNumber(value)
}

function formatOptionalPercent(value: number | null) {
  return value == null ? '—' : `${value}%`
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
        search={{ status: row.taskStatus, type: row.taskType }}
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
        <span className="text-muted-foreground">Pending</span>
        <span className="text-right">{formatNumber(row.pending)}</span>
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
  to,
}: {
  icon: ElementType
  label: string
  value: string
  sub?: string
  to?: '/buckets'
}) {
  const card = (
    <Card className={to ? 'h-full transition-colors hover:bg-muted/50' : 'h-full'}>
      <CardContent>
        <div className="flex items-center gap-2 text-muted-foreground">
          <Icon className="size-4" />
          <span className="text-sm">{label}</span>
          {to && <ChevronRight className="ml-auto size-4" />}
        </div>
        <div className="mt-2 text-2xl font-bold tabular-nums">{value}</div>
        {sub && <div className="mt-1 text-xs text-muted-foreground">{sub}</div>}
      </CardContent>
    </Card>
  )
  if (!to) return card
  return (
    <Link to={to} className="rounded-xl focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring">
      {card}
    </Link>
  )
}
