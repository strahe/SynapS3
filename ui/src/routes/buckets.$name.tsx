import { useQueryClient } from '@tanstack/react-query'
import { createFileRoute, Link, useNavigate } from '@tanstack/react-router'
import { ChevronRight, Folder, Loader2, RefreshCw, Trash2 } from 'lucide-react'
import { useState } from 'react'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { useBucket, useBucketObjects, useDeleteBucket } from '@/hooks/queries'
import { cn, formatBytes, formatNumber, timeAgo } from '@/lib/utils'

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

const bucketStatusColor: Record<string, string> = {
  active: 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300',
  creating: 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-300',
  deleting: 'bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-300',
  deleted: 'bg-gray-100 text-gray-800 dark:bg-gray-900 dark:text-gray-300',
  create_failed: 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300',
  delete_failed: 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300',
}

function DeleteBucketDetailDialog({ bucketName, objectCount }: { bucketName: string; objectCount: number }) {
  const [open, setOpen] = useState(false)
  const [confirmName, setConfirmName] = useState('')
  const [error, setError] = useState<string | null>(null)
  const deleteBucket = useDeleteBucket()
  const navigate = useNavigate()

  const recursive = objectCount > 0
  const nameMatches = confirmName === bucketName

  const reset = () => {
    setConfirmName('')
    setError(null)
    deleteBucket.reset()
  }

  const handleOpenChange = (next: boolean) => {
    if (!next) reset()
    setOpen(next)
  }

  const handleDelete = () => {
    if (!nameMatches) return
    setError(null)
    deleteBucket.mutate(
      { name: bucketName, recursive },
      {
        onSuccess: () => {
          setOpen(false)
          reset()
          navigate({ to: '/buckets' })
        },
        onError: (mutationError) => {
          setError(mutationError instanceof Error ? mutationError.message : 'Failed to delete bucket')
        },
      }
    )
  }

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogTrigger asChild>
        <Button variant="destructive" size="sm">
          <Trash2 className="h-4 w-4" />
          Delete
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Delete bucket "{bucketName}"</DialogTitle>
          <DialogDescription>
            {recursive
              ? `This will recursively purge ${formatNumber(objectCount)} object(s) and their cached data. Deletion is blocked while lifecycle tasks, object processing, or multipart uploads are in flight.`
              : 'This empty bucket will be marked for deletion and its proof set removed on-chain. Deletion is blocked while lifecycle tasks or multipart uploads are in flight.'}
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-2">
          <Label htmlFor="confirm-delete-detail">
            Type <span className="font-mono font-semibold">{bucketName}</span> to confirm
          </Label>
          <Input
            id="confirm-delete-detail"
            value={confirmName}
            onChange={(e) => setConfirmName(e.target.value)}
            placeholder={bucketName}
            autoFocus
            disabled={deleteBucket.isPending}
          />
        </div>
        {error && <p className="text-sm text-destructive">{error}</p>}
        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            onClick={() => handleOpenChange(false)}
            disabled={deleteBucket.isPending}
          >
            Cancel
          </Button>
          <Button variant="destructive" onClick={handleDelete} disabled={!nameMatches || deleteBucket.isPending}>
            {deleteBucket.isPending && <Loader2 className="h-4 w-4 animate-spin" />}
            Delete bucket
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

function ObjectBrowserPage() {
  const { name } = Route.useParams()
  const [prefix, setPrefix] = useState('')
  const [marker, setMarker] = useState('')

  const bucket = useBucket(name)
  const objects = useBucketObjects(name, prefix, marker)
  const qc = useQueryClient()

  const prefixParts = prefix.split('/').filter(Boolean)

  const navigateToPrefix = (newPrefix: string) => {
    setPrefix(newPrefix)
    setMarker('')
  }

  const handleRefresh = () => {
    qc.invalidateQueries({ queryKey: ['bucket', name] })
    qc.invalidateQueries({ queryKey: ['objects', name] })
  }

  const canDelete = bucket.data?.status === 'active'

  const folders = new Set<string>()
  const files =
    objects.data?.objects.filter((object) => {
      const rest = object.key.slice(prefix.length)
      const slashIdx = rest.indexOf('/')
      if (slashIdx >= 0) {
        folders.add(prefix + rest.substring(0, slashIdx + 1))
        return false
      }
      return true
    }) ?? []

  return (
    <div className="space-y-4 p-6">
      <div className="flex items-center justify-between gap-3">
        <div className="flex items-center gap-1 text-sm">
          <Link to="/buckets" className="text-primary hover:underline">
            Buckets
          </Link>
          <ChevronRight className="h-4 w-4 text-muted-foreground" />
          <button type="button" onClick={() => navigateToPrefix('')} className="text-primary hover:underline">
            {name}
          </button>
          {prefixParts.map((part, index) => {
            const targetPrefix = `${prefixParts.slice(0, index + 1).join('/')}/`

            return (
              <span key={targetPrefix} className="flex items-center gap-1">
                <ChevronRight className="h-4 w-4 text-muted-foreground" />
                <button
                  type="button"
                  onClick={() => navigateToPrefix(targetPrefix)}
                  className="text-primary hover:underline"
                >
                  {part}
                </button>
              </span>
            )
          })}
        </div>

        <div className="flex items-center gap-2">
          {canDelete ? (
            <DeleteBucketDetailDialog bucketName={name} objectCount={bucket.data?.object_count ?? 0} />
          ) : (
            <Button variant="destructive" size="sm" disabled>
              <Trash2 className="h-4 w-4" />
              Delete
            </Button>
          )}
          <Button variant="outline" size="sm" onClick={handleRefresh}>
            <RefreshCw className="h-4 w-4" /> Refresh
          </Button>
        </div>
      </div>

      {bucket.isLoading ? (
        <div className="flex h-32 items-center justify-center">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      ) : bucket.error ? (
        <div className="text-destructive">Failed to load bucket details</div>
      ) : bucket.data ? (
        <div className="rounded-lg border border-border p-4">
          <div className="mb-4 flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
            <div>
              <h1 className="text-2xl font-bold">{bucket.data.name}</h1>
              <p className="text-sm text-muted-foreground">Inspect metadata and browse files in this bucket.</p>
            </div>
            <span
              className={cn(
                'inline-block rounded-full px-2 py-0.5 text-xs font-medium',
                bucketStatusColor[bucket.data.status] ?? 'bg-gray-100 text-gray-800'
              )}
            >
              {bucket.data.status}
            </span>
          </div>

          <dl className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
            <div>
              <dt className="text-sm text-muted-foreground">Proof Set</dt>
              <dd className="font-mono text-xs text-muted-foreground">{bucket.data.proof_set_id ?? '—'}</dd>
            </div>
            <div>
              <dt className="text-sm text-muted-foreground">Objects</dt>
              <dd className="text-sm font-medium">{formatNumber(bucket.data.object_count)}</dd>
            </div>
            <div>
              <dt className="text-sm text-muted-foreground">Total size</dt>
              <dd className="text-sm font-medium">{formatBytes(bucket.data.total_size_bytes)}</dd>
            </div>
            <div>
              <dt className="text-sm text-muted-foreground">Created</dt>
              <dd className="text-sm text-muted-foreground" title={bucket.data.created_at}>
                {timeAgo(bucket.data.created_at)}
              </dd>
            </div>
            <div>
              <dt className="text-sm text-muted-foreground">Updated</dt>
              <dd className="text-sm text-muted-foreground" title={bucket.data.updated_at}>
                {timeAgo(bucket.data.updated_at)}
              </dd>
            </div>
            <div>
              <dt className="text-sm text-muted-foreground">Current path</dt>
              <dd className="font-mono text-xs text-muted-foreground">{prefix || '/'}</dd>
            </div>
          </dl>
        </div>
      ) : null}

      {objects.isLoading ? (
        <div className="flex h-60 items-center justify-center">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      ) : objects.error ? (
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
                  <th className="px-4 py-3 text-left font-medium">ETag</th>
                  <th className="px-4 py-3 text-left font-medium">Piece CID</th>
                  <th className="px-4 py-3 text-left font-medium">Created</th>
                  <th className="px-4 py-3 text-left font-medium">Updated</th>
                </tr>
              </thead>
              <tbody>
                {[...folders].sort().map((folder) => (
                  <tr
                    key={folder}
                    className="cursor-pointer border-b border-border hover:bg-muted/30"
                    onClick={() => navigateToPrefix(folder)}
                  >
                    <td className="px-4 py-3">
                      <span className="flex items-center gap-2 text-primary">
                        <Folder className="h-4 w-4" />
                        {folder.slice(prefix.length)}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-right text-muted-foreground">—</td>
                    <td className="px-4 py-3">—</td>
                    <td className="px-4 py-3">—</td>
                    <td className="px-4 py-3">—</td>
                    <td className="px-4 py-3">—</td>
                    <td className="px-4 py-3">—</td>
                    <td className="px-4 py-3">—</td>
                  </tr>
                ))}
                {files.map((object) => (
                  <tr key={object.id} className="border-b border-border hover:bg-muted/30">
                    <td className="px-4 py-3 font-mono text-xs">{object.key.slice(prefix.length)}</td>
                    <td className="px-4 py-3 text-right">{formatBytes(object.size)}</td>
                    <td className="px-4 py-3">
                      <span
                        className={cn(
                          'inline-block rounded-full px-2 py-0.5 text-xs font-medium',
                          stateColor[object.state] ?? 'bg-gray-100 text-gray-800'
                        )}
                      >
                        {object.state}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-muted-foreground">{object.content_type}</td>
                    <td
                      className="max-w-40 truncate px-4 py-3 font-mono text-xs text-muted-foreground"
                      title={object.etag}
                    >
                      {object.etag}
                    </td>
                    <td
                      className="max-w-52 truncate px-4 py-3 font-mono text-xs text-muted-foreground"
                      title={object.piece_cid ?? undefined}
                    >
                      {object.piece_cid ?? '—'}
                    </td>
                    <td className="px-4 py-3 text-muted-foreground" title={object.created_at}>
                      {timeAgo(object.created_at)}
                    </td>
                    <td className="px-4 py-3 text-muted-foreground" title={object.updated_at}>
                      {timeAgo(object.updated_at)}
                    </td>
                  </tr>
                ))}
                {folders.size === 0 && files.length === 0 && (
                  <tr>
                    <td colSpan={8} className="px-4 py-8 text-center text-muted-foreground">
                      No objects found
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>

          <div className="flex justify-between">
            {marker && (
              <Button variant="outline" size="sm" onClick={() => setMarker('')}>
                ← First page
              </Button>
            )}
            {objects.data?.has_more && objects.data.next_marker && (
              <Button
                variant="outline"
                size="sm"
                className="ml-auto"
                onClick={() => {
                  if (objects.data?.next_marker) {
                    setMarker(objects.data.next_marker)
                  }
                }}
              >
                Next page →
              </Button>
            )}
          </div>
        </>
      )}
    </div>
  )
}
