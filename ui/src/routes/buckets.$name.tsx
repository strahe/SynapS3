import { createFileRoute, Link } from '@tanstack/react-router'
import { useBucketObjects } from '@/hooks/queries'
import { formatBytes, timeAgo } from '@/lib/utils'
import { cn } from '@/lib/utils'
import { Loader2, RefreshCw, ChevronRight, Folder } from 'lucide-react'
import { useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'

export const Route = createFileRoute('/buckets/$name')({
  component: ObjectBrowserPage,
})

const stateColor: Record<string, string> = {
  cached: 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300',
  uploading: 'bg-indigo-100 text-indigo-800 dark:bg-indigo-900 dark:text-indigo-300',
  uploaded: 'bg-violet-100 text-violet-800 dark:bg-violet-900 dark:text-violet-300',
  onchaining: 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-300',
  onchained: 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300',
  failed: 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300',
  cache_evicted: 'bg-gray-100 text-gray-800 dark:bg-gray-900 dark:text-gray-300',
}

function ObjectBrowserPage() {
  const { name } = Route.useParams()
  const [prefix, setPrefix] = useState('')
  const [marker, setMarker] = useState('')
  const { data, isLoading, error } = useBucketObjects(name, prefix, marker)
  const qc = useQueryClient()

  const prefixParts = prefix.split('/').filter(Boolean)

  const navigateToPrefix = (newPrefix: string) => {
    setPrefix(newPrefix)
    setMarker('')
  }

  // Deduplicate "folder" prefixes from objects
  const folders = new Set<string>()
  const files = data?.objects.filter((o) => {
    const rest = o.key.slice(prefix.length)
    const slashIdx = rest.indexOf('/')
    if (slashIdx >= 0) {
      folders.add(prefix + rest.substring(0, slashIdx + 1))
      return false
    }
    return true
  }) ?? []

  return (
    <div className="space-y-4 p-6">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-1 text-sm">
          <Link to="/buckets" className="text-primary hover:underline">Buckets</Link>
          <ChevronRight className="h-4 w-4 text-muted-foreground" />
          <button onClick={() => navigateToPrefix('')} className="text-primary hover:underline">{name}</button>
          {prefixParts.map((part, i) => (
            <span key={i} className="flex items-center gap-1">
              <ChevronRight className="h-4 w-4 text-muted-foreground" />
              <button
                onClick={() => navigateToPrefix(prefixParts.slice(0, i + 1).join('/') + '/')}
                className="text-primary hover:underline"
              >
                {part}
              </button>
            </span>
          ))}
        </div>
        <button
          onClick={() => qc.invalidateQueries({ queryKey: ['objects', name] })}
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
        <div className="text-destructive">Failed to load objects</div>
      ) : (
        <>
          <div className="overflow-x-auto rounded-lg border border-border">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border bg-muted/50">
                  <th className="px-4 py-3 text-left font-medium">Key</th>
                  <th className="px-4 py-3 text-right font-medium">Size</th>
                  <th className="px-4 py-3 text-left font-medium">State</th>
                  <th className="px-4 py-3 text-left font-medium">Type</th>
                  <th className="px-4 py-3 text-left font-medium">Updated</th>
                </tr>
              </thead>
              <tbody>
                {[...folders].sort().map((f) => (
                  <tr key={f} className="border-b border-border hover:bg-muted/30 cursor-pointer" onClick={() => navigateToPrefix(f)}>
                    <td className="px-4 py-3">
                      <span className="flex items-center gap-2 text-primary">
                        <Folder className="h-4 w-4" />
                        {f.slice(prefix.length)}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-right text-muted-foreground">—</td>
                    <td className="px-4 py-3">—</td>
                    <td className="px-4 py-3">—</td>
                    <td className="px-4 py-3">—</td>
                  </tr>
                ))}
                {files.map((o) => (
                  <tr key={o.id} className="border-b border-border hover:bg-muted/30">
                    <td className="px-4 py-3 font-mono text-xs">{o.key.slice(prefix.length)}</td>
                    <td className="px-4 py-3 text-right">{formatBytes(o.size)}</td>
                    <td className="px-4 py-3">
                      <span className={cn('inline-block rounded-full px-2 py-0.5 text-xs font-medium', stateColor[o.state] ?? 'bg-gray-100 text-gray-800')}>
                        {o.state}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-muted-foreground">{o.content_type}</td>
                    <td className="px-4 py-3 text-muted-foreground">{timeAgo(o.updated_at)}</td>
                  </tr>
                ))}
                {folders.size === 0 && files.length === 0 && (
                  <tr>
                    <td colSpan={5} className="px-4 py-8 text-center text-muted-foreground">
                      No objects found
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>

          {/* Pagination */}
          <div className="flex justify-between">
            {marker && (
              <button
                onClick={() => setMarker('')}
                className="rounded-md border border-border px-3 py-1.5 text-sm hover:bg-accent"
              >
                ← First page
              </button>
            )}
            {data?.has_more && data.next_marker && (
              <button
                onClick={() => setMarker(data.next_marker!)}
                className="ml-auto rounded-md border border-border px-3 py-1.5 text-sm hover:bg-accent"
              >
                Next page →
              </button>
            )}
          </div>
        </>
      )}
    </div>
  )
}
