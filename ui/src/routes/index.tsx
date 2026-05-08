import { createFileRoute } from '@tanstack/react-router'
import { Database, FileBox, HardDrive, Loader2 } from 'lucide-react'
import { Bar, BarChart, Cell, Pie, PieChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { PageHeader } from '@/components/app/PageHeader'
import { StatusBadge } from '@/components/app/StatusBadge'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { useOverview } from '@/hooks/queries'
import { formatBytes, formatDuration, formatNumber } from '@/lib/utils'

export const Route = createFileRoute('/')({
  component: OverviewPage,
})

const STATE_COLORS: Record<string, string> = {
  cached: 'var(--chart-1)',
  uploading: 'var(--chart-2)',
  committing: 'var(--chart-4)',
  replicating: 'var(--chart-3)',
  stored: 'var(--chart-5)',
  failed: 'var(--destructive)',
  cache_evicted: 'var(--muted-foreground)',
  uploaded: 'var(--chart-3)',
  onchaining: 'var(--chart-4)',
  onchained: 'var(--chart-5)',
}

const STATUS_COLORS: Record<string, string> = {
  queued: 'var(--chart-4)',
  scheduled: 'var(--chart-3)',
  waiting: 'var(--chart-2)',
  running: 'var(--chart-2)',
  completed: 'var(--chart-5)',
  failed: 'var(--destructive)',
  exhausted: 'var(--destructive)',
  cancelled: 'var(--muted-foreground)',
}

const DEFAULT_CHART_COLOR = 'var(--muted-foreground)'

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

  const objStateData = Object.entries(data.objects.by_state).map(([name, value]) => ({ name, value }))
  const taskStatusData = Object.entries(data.tasks.by_status).map(([name, value]) => ({ name, value }))
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
            <CardTitle>Current Object Versions by State</CardTitle>
          </CardHeader>
          <CardContent>
            {objStateData.length > 0 ? (
              <ResponsiveContainer width="100%" height={240}>
                <PieChart>
                  <Pie
                    data={objStateData}
                    dataKey="value"
                    nameKey="name"
                    cx="50%"
                    cy="50%"
                    outerRadius={80}
                    label={({ name, value }) => `${name}: ${value}`}
                  >
                    {objStateData.map((entry) => (
                      <Cell key={entry.name} fill={STATE_COLORS[entry.name] ?? DEFAULT_CHART_COLOR} />
                    ))}
                  </Pie>
                  <Tooltip />
                </PieChart>
              </ResponsiveContainer>
            ) : (
              <div className="flex h-60 items-center justify-center text-muted-foreground">No objects yet</div>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Tasks by Status</CardTitle>
          </CardHeader>
          <CardContent>
            {taskStatusData.length > 0 ? (
              <ResponsiveContainer width="100%" height={240}>
                <BarChart data={taskStatusData}>
                  <XAxis dataKey="name" tick={{ fontSize: 12 }} />
                  <YAxis tick={{ fontSize: 12 }} />
                  <Tooltip />
                  <Bar dataKey="value">
                    {taskStatusData.map((entry) => (
                      <Cell key={entry.name} fill={STATUS_COLORS[entry.name] ?? DEFAULT_CHART_COLOR} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            ) : (
              <div className="flex h-60 items-center justify-center text-muted-foreground">No tasks yet</div>
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
              {Object.entries(data.workers).map(([name, healthy]) => (
                <div key={name} className="flex items-center justify-between">
                  <span className="text-sm capitalize">{name}</span>
                  <StatusBadge tone={healthy ? 'success' : 'danger'}>{healthy ? 'Healthy' : 'Unhealthy'}</StatusBadge>
                </div>
              ))}
              {Object.keys(data.workers).length === 0 && (
                <div className="text-sm text-muted-foreground">No workers registered</div>
              )}
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
