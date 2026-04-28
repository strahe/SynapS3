import { useQueryClient } from '@tanstack/react-query'
import { createFileRoute, Link, useNavigate } from '@tanstack/react-router'
import { Loader2, Plus, RefreshCw, Trash2, UserRound } from 'lucide-react'
import { type FormEvent, useEffect, useState } from 'react'
import { type BucketItem, internalRootOwnerAccessKey } from '@/api/client'
import { BucketOwnerSelect } from '@/components/app/BucketOwnerSelect'
import { PageHeader } from '@/components/app/PageHeader'
import { bucketStatusTone, StatusBadge } from '@/components/app/StatusBadge'
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
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { useBuckets, useCreateBucket, useDeleteBucket, useS3Users, useUpdateBucketOwner } from '@/hooks/queries'
import { formatBytes, formatNumber, timeAgo } from '@/lib/utils'

export const Route = createFileRoute('/buckets/')({
  component: BucketsPage,
})

const deletableBucketStatuses = new Set(['active'])

function CreateBucketDialog() {
  const [open, setOpen] = useState(false)
  const [bucketName, setBucketName] = useState('')
  const [ownerAccessKey, setOwnerAccessKey] = useState('')
  const [error, setError] = useState<string | null>(null)
  const { data: users = [], isLoading: usersLoading, error: usersError } = useS3Users()
  const createBucket = useCreateBucket()
  const navigate = useNavigate()

  const reset = () => {
    setBucketName('')
    setOwnerAccessKey('')
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
    if (!ownerAccessKey) {
      setError('Bucket owner is required')
      return
    }

    setError(null)
    createBucket.mutate(
      { name, ownerAccessKey },
      {
        onSuccess: (bucket) => {
          setOpen(false)
          reset()
          navigate({ to: '/buckets/$name', params: { name: bucket.name } })
        },
        onError: (mutationError) => {
          setError(mutationError instanceof Error ? mutationError.message : 'Failed to create bucket')
        },
      }
    )
  }

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogTrigger asChild>
        <Button size="sm">
          <Plus data-icon="inline-start" />
          Create Bucket
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Create Bucket</DialogTitle>
          <DialogDescription>Choose the S3 user that will own and manage this bucket.</DialogDescription>
        </DialogHeader>
        <form onSubmit={handleSubmit} className="flex flex-col gap-4">
          <div className="flex flex-col gap-2">
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
          <div className="flex flex-col gap-2">
            <Label htmlFor="bucket-owner">Owner</Label>
            <BucketOwnerSelect
              id="bucket-owner"
              value={ownerAccessKey}
              onChange={setOwnerAccessKey}
              disabled={createBucket.isPending || usersLoading}
              users={users}
            />
            {users.length === 0 && !usersLoading && (
              <p className="text-xs text-muted-foreground">
                No S3 users yet. Internal root can be used as fallback owner.
              </p>
            )}
            {usersError && <p className="text-xs text-destructive">Failed to load S3 users.</p>}
          </div>
          {error && <p className="text-sm text-destructive">{error}</p>}
          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              onClick={() => handleOpenChange(false)}
              disabled={createBucket.isPending}
            >
              Cancel
            </Button>
            <Button type="submit" disabled={createBucket.isPending || usersLoading}>
              {createBucket.isPending && <Loader2 data-icon="inline-start" className="animate-spin" />}
              Create
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}

function ChangeBucketOwnerDialog({ bucket }: { bucket: BucketItem }) {
  const [open, setOpen] = useState(false)
  const [ownerAccessKey, setOwnerAccessKey] = useState(bucket.owner_access_key ?? '')
  const [error, setError] = useState<string | null>(null)
  const { data: users = [], isLoading: usersLoading, error: usersError } = useS3Users()
  const updateOwner = useUpdateBucketOwner()

  useEffect(() => {
    if (!open) {
      setOwnerAccessKey(bucket.owner_access_key ?? '')
      setError(null)
    }
  }, [bucket.owner_access_key, open])

  const reset = () => {
    setOwnerAccessKey(bucket.owner_access_key ?? '')
    setError(null)
    updateOwner.reset()
  }

  const handleOpenChange = (next: boolean) => {
    reset()
    setOpen(next)
  }

  const handleUpdate = () => {
    if (!ownerAccessKey || ownerAccessKey === bucket.owner_access_key) return
    setError(null)
    updateOwner.mutate(
      { name: bucket.name, ownerAccessKey },
      {
        onSuccess: () => {
          setOpen(false)
          reset()
        },
        onError: (mutationError) => {
          setError(mutationError instanceof Error ? mutationError.message : 'Failed to update owner')
        },
      }
    )
  }

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogTrigger asChild>
        <Button variant="outline" size="xs">
          <UserRound data-icon="inline-start" />
          {bucket.owner_access_key ? 'Change owner' : 'Assign owner'}
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{bucket.owner_access_key ? 'Change bucket owner' : 'Assign bucket owner'}</DialogTitle>
          <DialogDescription>Transfer full control of "{bucket.name}" to an existing S3 user.</DialogDescription>
        </DialogHeader>
        <div className="flex flex-col gap-2">
          <Label htmlFor={`owner-${bucket.id}`}>Owner</Label>
          <BucketOwnerSelect
            id={`owner-${bucket.id}`}
            value={ownerAccessKey}
            onChange={setOwnerAccessKey}
            disabled={updateOwner.isPending || usersLoading}
            users={users}
          />
          {users.length === 0 && !usersLoading && (
            <p className="text-xs text-muted-foreground">
              No S3 users yet. Internal root can be used as fallback owner.
            </p>
          )}
          {usersError && <p className="text-xs text-destructive">Failed to load S3 users.</p>}
        </div>
        {error && <p className="text-sm text-destructive">{error}</p>}
        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            onClick={() => handleOpenChange(false)}
            disabled={updateOwner.isPending}
          >
            Cancel
          </Button>
          <Button
            type="button"
            onClick={handleUpdate}
            disabled={!ownerAccessKey || ownerAccessKey === bucket.owner_access_key || updateOwner.isPending}
          >
            {updateOwner.isPending && <Loader2 data-icon="inline-start" className="animate-spin" />}
            Save owner
          </Button>
        </DialogFooter>
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
      }
    )
  }

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogTrigger asChild>
        <Button variant="destructive" size="xs">
          <Trash2 data-icon="inline-start" />
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
        <div className="flex flex-col gap-2">
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
            {deleteBucket.isPending && <Loader2 data-icon="inline-start" className="animate-spin" />}
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
    <div className="flex flex-col gap-4 p-6">
      <PageHeader
        title="Buckets"
        actions={
          <>
            <CreateBucketDialog />
            <Button variant="outline" size="sm" onClick={() => qc.invalidateQueries({ queryKey: ['buckets'] })}>
              <RefreshCw data-icon="inline-start" /> Refresh
            </Button>
          </>
        }
      />

      {isLoading ? (
        <div className="flex h-60 items-center justify-center">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      ) : error ? (
        <div className="text-destructive">Failed to load buckets</div>
      ) : (
        <div className="overflow-hidden rounded-lg border border-border">
          <Table>
            <TableHeader>
              <TableRow className="bg-muted/50">
                <TableHead className="px-4">Name</TableHead>
                <TableHead className="px-4">Owner</TableHead>
                <TableHead className="px-4">Status</TableHead>
                <TableHead className="px-4">Proof Set</TableHead>
                <TableHead className="px-4 text-right">Objects</TableHead>
                <TableHead className="px-4 text-right">Size</TableHead>
                <TableHead className="px-4">Created</TableHead>
                <TableHead className="px-4">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {data && data.length > 0 ? (
                data.map((bucket) => {
                  const canDelete = deletableBucketStatuses.has(bucket.status)

                  return (
                    <TableRow key={bucket.id}>
                      <TableCell className="px-4">
                        <Link
                          to="/buckets/$name"
                          params={{ name: bucket.name }}
                          className="font-medium text-primary hover:underline"
                        >
                          {bucket.name}
                        </Link>
                      </TableCell>
                      <TableCell className="px-4">
                        <OwnerCell ownerAccessKey={bucket.owner_access_key} />
                      </TableCell>
                      <TableCell className="px-4">
                        <StatusBadge tone={bucketStatusTone(bucket.status)}>{bucket.status}</StatusBadge>
                      </TableCell>
                      <TableCell className="px-4 font-mono text-xs text-muted-foreground">
                        {bucket.proof_set_id ?? '—'}
                      </TableCell>
                      <TableCell className="px-4 text-right">{formatNumber(bucket.object_count)}</TableCell>
                      <TableCell className="px-4 text-right">{formatBytes(bucket.total_size_bytes)}</TableCell>
                      <TableCell className="px-4 text-muted-foreground" title={bucket.created_at}>
                        {timeAgo(bucket.created_at)}
                      </TableCell>
                      <TableCell className="px-4">
                        <div className="flex flex-wrap items-center gap-2">
                          <ChangeBucketOwnerDialog bucket={bucket} />
                          {canDelete ? (
                            <DeleteBucketDialog bucket={bucket} />
                          ) : (
                            <Button
                              variant="destructive"
                              size="xs"
                              disabled
                              title="Only active or creating buckets can be deleted"
                            >
                              <Trash2 data-icon="inline-start" />
                              Delete
                            </Button>
                          )}
                        </div>
                      </TableCell>
                    </TableRow>
                  )
                })
              ) : (
                <TableRow>
                  <TableCell colSpan={8} className="h-24 text-center text-muted-foreground">
                    No buckets found
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </div>
      )}
    </div>
  )
}

function OwnerCell({ ownerAccessKey }: { ownerAccessKey: string | null }) {
  if (!ownerAccessKey) {
    return <StatusBadge tone="warning">Unassigned</StatusBadge>
  }
  if (ownerAccessKey === internalRootOwnerAccessKey) {
    return <StatusBadge tone="neutral">Internal root</StatusBadge>
  }
  return <code className="block max-w-56 truncate text-xs text-muted-foreground">{ownerAccessKey}</code>
}
