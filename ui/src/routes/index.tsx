import { createFileRoute } from '@tanstack/react-router'
import { useOverview } from '@/hooks/queries'
import { formatBytes, formatNumber, formatDuration } from '@/lib/utils'
import { Database, FileBox, HardDrive, CheckCircle, XCircle, Loader2 } from 'lucide-react'
import { PieChart, Pie, Cell, BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts'

export const Route = createFileRoute('/')({
  component: OverviewPage,
})

const STATE_COLORS: Record<string, string> = {
  cached: '#3b82f6',
  uploading: '#6366f1',
  uploaded: '#8b5cf6',
  onchaining: '#f59e0b',
  onchained: '#22c55e',
  failed: '#ef4444',
  cache_evicted: '#6b7280',
}

const STATUS_COLORS: Record<string, string> = {
  pending: '#f59e0b',
  running: '#3b82f6',
  completed: '#22c55e',
  failed: '#ef4444',
  dead_letter: '#dc2626',
  cancelled: '#6b7280',
}

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
    return (
      <div className="flex h-full items-center justify-center text-destructive">
        Failed to load overview data
      </div>
    )
  }

  const objStateData = Object.entries(data.objects.by_state).map(([name, value]) => ({ name, value }))
  const taskStatusData = Object.entries(data.tasks.by_status).map(([name, value]) => ({ name, value }))
  const cachePercent = data.cache.max_bytes > 0 ? (data.cache.used_bytes / data.cache.max_bytes) * 100 : 0

  return (
    <div className="space-y-6 p-6">
      <h1 className="text-2xl font-bold">Overview</h1>

      {/* Stat cards */}
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

      {/* Charts row */}
      <div className="grid gap-4 lg:grid-cols-2">
        <div className="rounded-lg border border-border bg-card p-4">
          <h3 className="mb-4 text-sm font-medium text-muted-foreground">Object State Distribution</h3>
          {objStateData.length > 0 ? (
            <ResponsiveContainer width="100%" height={240}>
              <PieChart>
                <Pie data={objStateData} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={80} label={({ name, value }) => `${name}: ${value}`}>
                  {objStateData.map((entry) => (
                    <Cell key={entry.name} fill={STATE_COLORS[entry.name] ?? '#6b7280'} />
                  ))}
                </Pie>
                <Tooltip />
              </PieChart>
            </ResponsiveContainer>
          ) : (
            <div className="flex h-60 items-center justify-center text-muted-foreground">No objects yet</div>
          )}
        </div>

        <div className="rounded-lg border border-border bg-card p-4">
          <h3 className="mb-4 text-sm font-medium text-muted-foreground">Task Pipeline Status</h3>
          {taskStatusData.length > 0 ? (
            <ResponsiveContainer width="100%" height={240}>
              <BarChart data={taskStatusData}>
                <XAxis dataKey="name" tick={{ fontSize: 12 }} />
                <YAxis tick={{ fontSize: 12 }} />
                <Tooltip />
                <Bar dataKey="value">
                  {taskStatusData.map((entry) => (
                    <Cell key={entry.name} fill={STATUS_COLORS[entry.name] ?? '#6b7280'} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          ) : (
            <div className="flex h-60 items-center justify-center text-muted-foreground">No tasks yet</div>
          )}
        </div>
      </div>

      {/* Bottom row: workers + system */}
      <div className="grid gap-4 lg:grid-cols-2">
        <div className="rounded-lg border border-border bg-card p-4">
          <h3 className="mb-4 text-sm font-medium text-muted-foreground">Worker Health</h3>
          <div className="space-y-2">
            {Object.entries(data.workers).map(([name, healthy]) => (
              <div key={name} className="flex items-center justify-between">
                <span className="text-sm capitalize">{name}</span>
                {healthy ? (
                  <span className="flex items-center gap-1 text-sm text-green-500">
                    <CheckCircle className="h-4 w-4" /> Healthy
                  </span>
                ) : (
                  <span className="flex items-center gap-1 text-sm text-red-500">
                    <XCircle className="h-4 w-4" /> Unhealthy
                  </span>
                )}
              </div>
            ))}
            {Object.keys(data.workers).length === 0 && (
              <div className="text-sm text-muted-foreground">No workers registered</div>
            )}
          </div>
        </div>

        <div className="rounded-lg border border-border bg-card p-4">
          <h3 className="mb-4 text-sm font-medium text-muted-foreground">System Info</h3>
          <dl className="space-y-2 text-sm">
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
        </div>
      </div>
    </div>
  )
}

function StatCard({ icon: Icon, label, value, sub }: { icon: React.ElementType; label: string; value: string; sub?: string }) {
  return (
    <div className="rounded-lg border border-border bg-card p-4">
      <div className="flex items-center gap-2 text-muted-foreground">
        <Icon className="h-4 w-4" />
        <span className="text-sm">{label}</span>
      </div>
      <div className="mt-2 text-2xl font-bold">{value}</div>
      {sub && <div className="mt-1 text-xs text-muted-foreground">{sub}</div>}
    </div>
  )
}
