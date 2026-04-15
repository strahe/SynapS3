import { type FormEvent, useState } from 'react'
import { createFileRoute, Link, useNavigate } from '@tanstack/react-router'
import { Loader2, Plus, RefreshCw, Trash2 } from 'lucide-react'
import type { BucketItem } from '@/api/client'
import { useBuckets, useCreateBucket, useDeleteBucket } from '@/hooks/queries'
import { cn, formatBytes, formatNumber, timeAgo } from '@/lib/utils'
import { useQueryClient } from '@tanstack/react-query'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog'

export const Route = createFileRoute('/buckets/')({
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

const deletableBucketStatuses = new Set(['active'])

function CreateBucketDialog() {
  const [open, setOpen] = useState(false)
  const [bucketName, setBucketName] = useState('')
  const [error, setError] = useState<string | null>(null)
  const createBucket = useCreateBucket()
  const navigate = useNavigate()

  const reset = () => {
    setBucketName('')
    setError(null)
    createBucket.reset()
  }

  const handleOpenChange = (next: boolean) => {
    if (!next) reset()
    setOpen(next)
  }

  const handleSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    const name = bucketName.trim()
    if (!name) {
      setError('Bucket name is required')
      return
    }

    setError(null)
    createBucket.mutate(name, {
      onSuccess: (bucket) => {
        setOpen(false)
        reset()
        navigate({ to: '/buckets/$name', params: { name: bucket.name } })
      },
      onError: (mutationError) => {
        setError(mutationError instanceof Error ? mutationError.message : 'Failed to create bucket')
      },
    })
  }

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogTrigger asChild>
        <Button size="sm">
          <Plus className="h-4 w-4" />
          Create Bucket
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Create Bucket</DialogTitle>
          <DialogDescription>
            A new proof set will be created on-chain for this bucket.
          </DialogDescription>
        </DialogHeader>
        <form onSubmit={handleSubmit} className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="bucket-name">Bucket name</Label>
            <Input
              id="bucket-name"
              value={bucketName}
              onChange={(e) => setBucketName(e.target.value)}
              placeholder="my-bucket"
              autoFocus
              disabled={createBucket.isPending}
            />
          </div>
          {error && (
            <p className="text-sm text-destructive">{error}</p>
          )}
          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => handleOpenChange(false)} disabled={createBucket.isPending}>
              Cancel
            </Button>
            <Button type="submit" disabled={createBucket.isPending}>
              {createBucket.isPending && <Loader2 className="h-4 w-4 animate-spin" />}
              Create
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}

function DeleteBucketDialog({ bucket }: { bucket: BucketItem }) {
  const [open, setOpen] = useState(false)
  const [confirmName, setConfirmName] = useState('')
  const [error, setError] = useState<string | null>(null)
  const deleteBucket = useDeleteBucket()

  const recursive = bucket.object_count > 0
  const nameMatches = confirmName === bucket.name

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
      { name: bucket.name, recursive },
      {
        onSuccess: () => {
          setOpen(false)
          reset()
        },
        onError: (mutationError) => {
          setError(mutationError instanceof Error ? mutationError.message : 'Failed to delete bucket')
        },
      },
    )
  }

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogTrigger asChild>
        <Button variant="destructive" size="xs">
          <Trash2 className="h-3 w-3" />
          Delete
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Delete bucket "{bucket.name}"</DialogTitle>
          <DialogDescription>
            {recursive
              ? `This will recursively purge ${formatNumber(bucket.object_count)} object(s) and their cached data. Deletion is blocked while lifecycle tasks, object processing, or multipart uploads are in flight.`
              : 'This empty bucket will be marked for deletion and its proof set removed on-chain. Deletion is blocked while lifecycle tasks or multipart uploads are in flight.'}
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-2">
          <Label htmlFor={`confirm-delete-${bucket.id}`}>
            Type <span className="font-mono font-semibold">{bucket.name}</span> to confirm
          </Label>
          <Input
            id={`confirm-delete-${bucket.id}`}
            value={confirmName}
            onChange={(e) => setConfirmName(e.target.value)}
            placeholder={bucket.name}
            autoFocus
            disabled={deleteBucket.isPending}
          />
        </div>
        {error && (
          <p className="text-sm text-destructive">{error}</p>
        )}
        <DialogFooter>
          <Button type="button" variant="outline" onClick={() => handleOpenChange(false)} disabled={deleteBucket.isPending}>
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

function BucketsPage() {
  const { data, isLoading, error } = useBuckets()
  const qc = useQueryClient()

  return (
    <div className="space-y-4 p-6">
      <div className="flex flex-col gap-3 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <h1 className="text-2xl font-bold">Buckets</h1>
          <p className="text-sm text-muted-foreground">Create buckets, delete them safely, and drill into their files.</p>
        </div>

        <div className="flex items-center gap-2">
          <CreateBucketDialog />
          <Button
            variant="outline"
            size="sm"
            onClick={() => qc.invalidateQueries({ queryKey: ['buckets'] })}
          >
            <RefreshCw className="h-4 w-4" /> Refresh
          </Button>
        </div>
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
                <th className="px-4 py-3 text-left font-medium">Actions</th>
              </tr>
            </thead>
            <tbody>
              {data && data.length > 0 ? data.map((bucket) => {
                const canDelete = deletableBucketStatuses.has(bucket.status)

                return (
                  <tr key={bucket.id} className="border-b border-border hover:bg-muted/30">
                    <td className="px-4 py-3">
                      <Link
                        to="/buckets/$name"
                        params={{ name: bucket.name }}
                        className="font-medium text-primary hover:underline"
                      >
                        {bucket.name}
                      </Link>
                    </td>
                    <td className="px-4 py-3">
                      <span className={cn('inline-block rounded-full px-2 py-0.5 text-xs font-medium', statusColor[bucket.status] ?? 'bg-gray-100 text-gray-800')}>
                        {bucket.status}
                      </span>
                    </td>
                    <td className="px-4 py-3 font-mono text-xs text-muted-foreground">
                      {bucket.proof_set_id ?? '—'}
                    </td>
                    <td className="px-4 py-3 text-right">{formatNumber(bucket.object_count)}</td>
                    <td className="px-4 py-3 text-right">{formatBytes(bucket.total_size_bytes)}</td>
                    <td className="px-4 py-3 text-muted-foreground" title={bucket.created_at}>{timeAgo(bucket.created_at)}</td>
                    <td className="px-4 py-3">
                      {canDelete ? (
                        <DeleteBucketDialog bucket={bucket} />
                      ) : (
                        <Button variant="destructive" size="xs" disabled title="Only active or creating buckets can be deleted">
                          <Trash2 className="h-3 w-3" />
                          Delete
                        </Button>
                      )}
                    </td>
                  </tr>
                )
              }) : (
                <tr>
                  <td colSpan={7} className="px-4 py-8 text-center text-muted-foreground">
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
