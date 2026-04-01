import { createFileRoute, Link } from '@tanstack/react-router'
import { useBuckets } from '@/hooks/queries'
import { formatBytes, formatNumber, timeAgo } from '@/lib/utils'
import { cn } from '@/lib/utils'
import { Loader2, RefreshCw } from 'lucide-react'
import { useQueryClient } from '@tanstack/react-query'

export const Route = createFileRoute('/buckets')({
  component: BucketsPage,
})

const statusColor: Record<string, string> = {
  active: 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300',
  creating: 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-300',
  deleting: 'bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-300',
  deleted: 'bg-gray-100 text-gray-800 dark:bg-gray-900 dark:text-gray-300',
  create_failed: 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300',
  delete_failed: 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300',
}

function BucketsPage() {
  const { data, isLoading, error } = useBuckets()
  const qc = useQueryClient()

  return (
    <div className="space-y-4 p-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">Buckets</h1>
        <button
          onClick={() => qc.invalidateQueries({ queryKey: ['buckets'] })}
          className="inline-flex items-center gap-2 rounded-md border border-border px-3 py-1.5 text-sm hover:bg-accent"
        >
          <RefreshCw className="h-4 w-4" /> Refresh
        </button>
      </div>

      {isLoading ? (
        <div className="flex h-60 items-center justify-center">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      ) : error ? (
        <div className="text-destructive">Failed to load buckets</div>
      ) : (
        <div className="overflow-x-auto rounded-lg border border-border">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-border bg-muted/50">
                <th className="px-4 py-3 text-left font-medium">Name</th>
                <th className="px-4 py-3 text-left font-medium">Status</th>
                <th className="px-4 py-3 text-left font-medium">Proof Set</th>
                <th className="px-4 py-3 text-right font-medium">Objects</th>
                <th className="px-4 py-3 text-right font-medium">Size</th>
                <th className="px-4 py-3 text-left font-medium">Created</th>
              </tr>
            </thead>
            <tbody>
              {data && data.length > 0 ? data.map((b) => (
                <tr key={b.id} className="border-b border-border hover:bg-muted/30">
                  <td className="px-4 py-3">
                    <Link
                      to="/buckets/$name"
                      params={{ name: b.name }}
                      className="font-medium text-primary hover:underline"
                    >
                      {b.name}
                    </Link>
                  </td>
                  <td className="px-4 py-3">
                    <span className={cn('inline-block rounded-full px-2 py-0.5 text-xs font-medium', statusColor[b.status] ?? 'bg-gray-100 text-gray-800')}>
                      {b.status}
                    </span>
                  </td>
                  <td className="px-4 py-3 font-mono text-xs text-muted-foreground">
                    {b.proof_set_id ?? '—'}
                  </td>
                  <td className="px-4 py-3 text-right">{formatNumber(b.object_count)}</td>
                  <td className="px-4 py-3 text-right">{formatBytes(b.total_size_bytes)}</td>
                  <td className="px-4 py-3 text-muted-foreground">{timeAgo(b.created_at)}</td>
                </tr>
              )) : (
                <tr>
                  <td colSpan={6} className="px-4 py-8 text-center text-muted-foreground">
                    No buckets found
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
