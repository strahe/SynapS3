import { AlertTriangle, Info, RefreshCw } from 'lucide-react'
import { useState } from 'react'
import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import type { MonitoringData, MonitoringStatus } from '@/api/client'
import { Banner } from '@/components/app/Banner'
import { PageError } from '@/components/app/PageState'
import { Button } from '@/components/ui/button'
import { Card, CardAction, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { Skeleton } from '@/components/ui/skeleton'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import {
  formatMonitoringBytes,
  formatMonitoringCPU,
  formatMonitoringRate,
  formatMonitoringTime,
  type MonitoringChartPoint,
  monitoringChartPoints,
  monitoringIsStale,
  monitoringStatusLabel,
} from '@/lib/monitoring'
import { formatBytes } from '@/lib/utils'

type SeriesKey = Exclude<keyof MonitoringChartPoint, 'timestamp'>
type TrafficScope = 'client' | 'storage'

interface ChartSeries {
  key: SeriesKey
  label: string
  color: string
  format: (value: number | null) => string
}

const cpuSeries: ChartSeries[] = [
  { key: 'cpu_cores', label: 'CPU', color: 'var(--chart-1)', format: formatMonitoringCPU },
]
const memorySeries: ChartSeries[] = [
  { key: 'rss_bytes', label: 'Memory', color: 'var(--chart-2)', format: formatMemory },
]
const trafficSeries: Record<TrafficScope, ChartSeries[]> = {
  client: [
    {
      key: 'client_upload_bytes_per_second',
      label: 'Upload',
      color: 'var(--chart-1)',
      format: formatMonitoringRate,
    },
    {
      key: 'client_download_bytes_per_second',
      label: 'Download',
      color: 'var(--chart-5)',
      format: formatMonitoringRate,
    },
  ],
  storage: [
    {
      key: 'storage_upload_bytes_per_second',
      label: 'Upload',
      color: 'var(--chart-1)',
      format: formatMonitoringRate,
    },
    {
      key: 'storage_download_bytes_per_second',
      label: 'Download',
      color: 'var(--chart-5)',
      format: formatMonitoringRate,
    },
  ],
}

export function MonitoringCards({
  data,
  loading,
  error,
  refreshing,
  onRetry,
  updatedAt,
}: {
  data: MonitoringData | undefined
  loading: boolean
  error: boolean
  refreshing: boolean
  onRetry: () => void
  updatedAt: number
}) {
  const [scope, setScope] = useState<TrafficScope>('client')
  if (!data && !loading) {
    return (
      <div className="grid gap-4 lg:grid-cols-2">
        {['Process resources', 'Application traffic'].map((title) => (
          <Card key={title} className="min-w-0">
            <CardHeader>
              <CardTitle>{title}</CardTitle>
            </CardHeader>
            <CardContent>
              <PageError title="Could not load monitoring" onRetry={onRetry} retrying={refreshing} />
            </CardContent>
          </Card>
        ))}
      </div>
    )
  }

  const rows = monitoringChartPoints(data?.points ?? [])
  const latest = data?.points[data.points.length - 1]
  const now = Date.now()
  const domain: [number, number] = data
    ? [Date.parse(data.window_start), Date.parse(data.window_end)]
    : [now - 3_600_000, now]
  const stale = data ? monitoringIsStale(data, updatedAt, now) : false
  const pending = !data

  return (
    <div className="flex flex-col gap-4">
      {(error || stale) && (
        <Banner tone="warning" icon={AlertTriangle}>
          <div className="flex flex-wrap items-center justify-between gap-2">
            <span>{error ? 'Could not refresh monitoring' : 'Monitoring is out of date'}</span>
            <Button variant="outline" size="sm" onClick={onRetry} disabled={refreshing}>
              <RefreshCw data-icon="inline-start" className={refreshing ? 'animate-spin' : undefined} />
              Retry
            </Button>
          </div>
        </Banner>
      )}
      <div className="grid gap-4 lg:grid-cols-2">
        <Card className="min-w-0">
          <CardHeader className="flex min-h-8 items-center justify-between">
            <CardTitle>Process resources</CardTitle>
            <CardAction className="self-center">
              <MonitoringDetails data={data} />
            </CardAction>
          </CardHeader>
          <CardContent className="flex flex-col gap-4">
            <ResourceChart
              label="CPU"
              value={latest?.cpu_cores ?? null}
              status={data?.status.cpu ?? 'collecting'}
              loading={pending}
              series={cpuSeries}
              rows={rows}
              domain={domain}
              tickFormat={(value) => String(Number(value.toFixed(2)))}
              showTimeAxis={false}
            />
            <ResourceChart
              label="Memory"
              value={latest?.rss_bytes ?? null}
              status={data?.status.rss ?? 'collecting'}
              loading={pending}
              series={memorySeries}
              rows={rows}
              domain={domain}
              tickFormat={formatMemory}
            />
          </CardContent>
        </Card>

        <Card className="min-w-0">
          <Tabs
            value={scope}
            onValueChange={(value) => setScope(value === 'storage' ? 'storage' : 'client')}
            className="gap-4"
          >
            <CardHeader className="flex flex-row flex-wrap items-center justify-between gap-3">
              <CardTitle>Application traffic</CardTitle>
              <CardAction className="flex items-center gap-1">
                <TabsList aria-label="Traffic scope">
                  <TabsTrigger value="client">Client</TabsTrigger>
                  <TabsTrigger value="storage">Storage</TabsTrigger>
                </TabsList>
                <MonitoringDetails data={data} scope={scope} />
              </CardAction>
            </CardHeader>
            <CardContent>
              {(['client', 'storage'] as const).map((scope) => (
                <TabsContent key={scope} value={scope}>
                  <TrafficChart data={data} scope={scope} rows={rows} domain={domain} />
                </TabsContent>
              ))}
            </CardContent>
          </Tabs>
        </Card>
      </div>
    </div>
  )
}

function ResourceChart({
  label,
  value,
  status,
  loading,
  series,
  rows,
  domain,
  tickFormat,
  showTimeAxis = true,
}: {
  label: string
  value: number | null
  status: MonitoringStatus
  loading: boolean
  series: ChartSeries[]
  rows: MonitoringChartPoint[]
  domain: [number, number]
  tickFormat: (value: number) => string
  showTimeAxis?: boolean
}) {
  const format = series[0]?.format ?? formatMemory
  return (
    <div className="flex flex-col gap-2">
      <div className="flex min-h-6 items-center justify-between gap-4">
        <span className="text-sm text-muted-foreground">{label}</span>
        {loading ? (
          <Skeleton className="h-6 w-16" aria-label={`Loading ${label}`} />
        ) : (
          <span
            className="font-semibold tabular-nums"
            title={status === 'ready' ? undefined : monitoringStatusLabel(status)}
          >
            {status === 'ready' ? format(value) : '—'}
          </span>
        )}
      </div>
      <MonitoringChart
        rows={rows}
        domain={domain}
        series={series}
        height={90}
        tickFormat={tickFormat}
        status={status}
        loading={loading}
        showTimeAxis={showTimeAxis}
      />
    </div>
  )
}

function TrafficChart({
  data,
  scope,
  rows,
  domain,
}: {
  data: MonitoringData | undefined
  scope: TrafficScope
  rows: MonitoringChartPoint[]
  domain: [number, number]
}) {
  const latest = rows[rows.length - 1]
  const series = trafficSeries[scope]
  const status = data?.status.traffic ?? 'collecting'
  return (
    <div className="flex flex-col gap-4">
      <div className="grid grid-cols-2 gap-4">
        {series.map((item) => (
          <div key={item.key} className="flex flex-col gap-1">
            <span className="flex items-center gap-2 text-sm text-muted-foreground">
              <span className="size-2 rounded-full" style={{ background: item.color }} />
              {item.label}
            </span>
            {data ? (
              <span
                className="font-semibold tabular-nums"
                title={status === 'ready' ? undefined : monitoringStatusLabel(status)}
              >
                {status === 'ready' ? formatMonitoringRate(latest?.[item.key] ?? null) : '—'}
              </span>
            ) : (
              <Skeleton className="h-6 w-16" aria-label={`Loading ${item.label.toLowerCase()} rate`} />
            )}
          </div>
        ))}
      </div>
      <MonitoringChart
        rows={rows}
        domain={domain}
        series={series}
        height={190}
        tickFormat={formatMonitoringRate}
        status={status}
        loading={!data}
      />
    </div>
  )
}

function MonitoringChart({
  rows,
  domain,
  series,
  height,
  tickFormat,
  status,
  loading,
  showTimeAxis = true,
}: {
  rows: MonitoringChartPoint[]
  domain: [number, number]
  series: ChartSeries[]
  height: number
  tickFormat: (value: number) => string
  status: MonitoringStatus
  loading: boolean
  showTimeAxis?: boolean
}) {
  const hasValues = rows.some((point) => series.some((item) => point[item.key] !== null))
  const chartRows = rows.length ? rows : domain.map((timestamp) => ({ timestamp }))
  return (
    <div className="relative" style={{ height }} aria-busy={loading}>
      <ResponsiveContainer width="100%" height="100%">
        <LineChart
          data={chartRows}
          margin={{ top: 4, right: 8, bottom: showTimeAxis ? 0 : 8, left: 0 }}
          accessibilityLayer
        >
          <CartesianGrid vertical={false} stroke="var(--border)" />
          <XAxis
            hide={!showTimeAxis}
            type="number"
            dataKey="timestamp"
            scale="time"
            domain={domain}
            allowDataOverflow
            ticks={Array.from({ length: 5 }, (_, index) => domain[0] + ((domain[1] - domain[0]) * index) / 4)}
            minTickGap={24}
            tickFormatter={(value) => formatMonitoringTime(value)}
            tick={{ fontSize: 11, fill: 'var(--muted-foreground)' }}
            axisLine={false}
            tickLine={false}
          />
          <YAxis
            domain={hasValues ? [0, (maximum: number) => Math.max(maximum, 1)] : [0, 1]}
            ticks={hasValues ? undefined : [0, 0.5, 1]}
            width={72}
            tickCount={3}
            interval={0}
            tickFormatter={(value) => (hasValues || value === 0 ? tickFormat(value) : '')}
            tick={{ fontSize: 11, fill: 'var(--muted-foreground)' }}
            axisLine={false}
            tickLine={false}
          />
          {hasValues && <Tooltip content={<MonitoringTooltip series={series} />} />}
          {series.map((item) => (
            <Line
              key={item.key}
              type="linear"
              dataKey={item.key}
              name={item.label}
              stroke={item.color}
              strokeWidth={1.75}
              dot={(point: { cx?: number; cy?: number; index?: number }) => {
                const index = point.index
                if (
                  index === undefined ||
                  rows[index]?.[item.key] == null ||
                  rows[index - 1]?.[item.key] != null ||
                  rows[index + 1]?.[item.key] != null
                ) {
                  return <g key={index} />
                }
                return <circle key={index} cx={point.cx} cy={point.cy} r={2} fill={item.color} />
              }}
              activeDot={{ r: 3 }}
              connectNulls={false}
              isAnimationActive={false}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
      {(status !== 'ready' || !hasValues) && (
        <span
          role="status"
          className="pointer-events-none absolute right-2 top-1 bg-card px-1 text-xs text-muted-foreground"
        >
          {loading ? 'Loading…' : monitoringStatusLabel(status)}
        </span>
      )}
    </div>
  )
}

function MonitoringTooltip({
  active,
  payload,
  series,
}: {
  active?: boolean
  payload?: Array<{ payload?: MonitoringChartPoint }>
  series: ChartSeries[]
}) {
  const point = payload?.[0]?.payload
  if (!active || !point) return null
  return (
    <div className="rounded-md border border-border bg-popover px-3 py-2 text-xs text-popover-foreground shadow-md">
      <div className="mb-1 font-medium">{formatMonitoringTime(point.timestamp, true)}</div>
      <dl className="grid grid-cols-[auto_auto] gap-x-4 gap-y-1">
        {series.map((item) => (
          <div key={item.key} className="contents">
            <dt className="text-muted-foreground">{item.label}</dt>
            <dd className="text-right tabular-nums">{item.format(point[item.key])}</dd>
          </div>
        ))}
      </dl>
    </div>
  )
}

function MonitoringDetails({ data, scope }: { data: MonitoringData | undefined; scope?: TrafficScope }) {
  const label = scope ? 'Traffic details' : 'Process details'
  if (!data) {
    return (
      <Button variant="ghost" size="icon-xs" aria-label={label} title={label} disabled>
        <Info />
      </Button>
    )
  }
  const totals = scope ? data.totals[scope] : undefined
  return (
    <Popover>
      <PopoverTrigger asChild>
        <Button variant="ghost" size="icon-xs" aria-label={label} title={label}>
          <Info />
        </Button>
      </PopoverTrigger>
      <PopoverContent align="end" className="w-64" aria-label={label}>
        <div className="flex flex-col gap-3 text-sm">
          <p>
            {scope === 'client'
              ? 'S3 and Admin object transfers.'
              : scope === 'storage'
                ? 'Transfers with storage providers.'
                : 'CPU and resident memory for the SynapS3 process.'}
          </p>
          <p className="text-muted-foreground">Last hour · 15s samples</p>
          <dl className="flex flex-col gap-2">
            <div className="flex justify-between gap-3">
              <dt className="text-muted-foreground">Collected since</dt>
              <dd className="tabular-nums">{formatMonitoringTime(data.started_at)}</dd>
            </div>
            <div className="flex justify-between gap-3">
              <dt className="text-muted-foreground">Last sample</dt>
              <dd className="tabular-nums">{data.sampled_at ? formatMonitoringTime(data.sampled_at, true) : '—'}</dd>
            </div>
          </dl>
          {totals && (
            <div className="flex flex-col gap-2">
              <p className="text-muted-foreground">Since startup</p>
              <dl className="flex flex-col gap-2">
                <div className="flex justify-between gap-3">
                  <dt>Upload</dt>
                  <dd className="tabular-nums">{formatMonitoringBytes(totals.upload_bytes)}</dd>
                </div>
                <div className="flex justify-between gap-3">
                  <dt>Download</dt>
                  <dd className="tabular-nums">{formatMonitoringBytes(totals.download_bytes)}</dd>
                </div>
              </dl>
            </div>
          )}
        </div>
      </PopoverContent>
    </Popover>
  )
}

function formatMemory(value: number | null): string {
  return value === null ? '—' : formatBytes(value)
}
