import { useQueryClient } from '@tanstack/react-query'
import { createFileRoute, Link, useNavigate } from '@tanstack/react-router'
import { Loader2, Plus, RefreshCw, UserRound } from 'lucide-react'
import { type FormEvent, useEffect, useState } from 'react'
import { type BucketItem, internalRootOwnerAccessKey } from '@/api/client'
import { BucketOwnerSelect } from '@/components/app/BucketOwnerSelect'
import { PageErrorState } from '@/components/app/PageErrorState'
import { PageHeader } from '@/components/app/PageHeader'
import { ReviewDetails } from '@/components/app/ReviewDetails'
import { bucketStatusTone, StatusBadge } from '@/components/app/StatusBadge'
import { Alert, AlertDescription } from '@/components/ui/alert'
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
import { Field, FieldDescription, FieldError, FieldGroup, FieldLabel } from '@/components/ui/field'
import { Input } from '@/components/ui/input'
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { useBuckets, useCreateBucket, useS3Users, useUpdateBucketOwner } from '@/hooks/queries'
import { bucketCopyPolicyLabel, copyPolicyOptions, inheritedCopyPolicyValue } from '@/lib/bucket-copy-policy'
import {
  bucketStorageHealthLabel,
  bucketStorageHealthStatusTone,
  bucketStorageHealthTitle,
} from '@/lib/bucket-storage-health'
import { ownerLabel } from '@/lib/s3-owner'
import { formatBytes, formatNumber, timeAgo } from '@/lib/utils'

export const Route = createFileRoute('/buckets/')({
  component: BucketsPage,
})

function CreateBucketDialog() {
  const [open, setOpen] = useState(false)
  const [bucketName, setBucketName] = useState('')
  const [ownerAccessKey, setOwnerAccessKey] = useState('')
  const [copyPolicy, setCopyPolicy] = useState(inheritedCopyPolicyValue)
  const [error, setError] = useState<string | null>(null)
  const { data: users = [], isLoading: usersLoading, error: usersError } = useS3Users()
  const createBucket = useCreateBucket()
  const navigate = useNavigate()

  const reset = () => {
    setBucketName('')
    setOwnerAccessKey('')
    setCopyPolicy(inheritedCopyPolicyValue)
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
    const defaultCopies = copyPolicy === inheritedCopyPolicyValue ? null : Number(copyPolicy)
    createBucket.mutate(
      { name, ownerAccessKey, defaultCopies },
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

  const bucketNameError = error === 'Bucket name is required' ? error : null
  const ownerError = error === 'Bucket owner is required' ? error : null
  const formError = error && !bucketNameError && !ownerError ? error : null

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
          <FieldGroup>
            <Field data-invalid={Boolean(bucketNameError)}>
              <FieldLabel htmlFor="bucket-name">Bucket name</FieldLabel>
              <Input
                id="bucket-name"
                value={bucketName}
                onChange={(e) => setBucketName(e.target.value)}
                placeholder="my-bucket"
                autoFocus
                disabled={createBucket.isPending}
                aria-invalid={Boolean(bucketNameError)}
              />
              {bucketNameError && <FieldError>{bucketNameError}</FieldError>}
            </Field>
            <Field data-invalid={Boolean(ownerError || usersError)}>
              <FieldLabel htmlFor="bucket-owner">Owner</FieldLabel>
              <BucketOwnerSelect
                id="bucket-owner"
                value={ownerAccessKey}
                onChange={setOwnerAccessKey}
                disabled={createBucket.isPending || usersLoading}
                invalid={Boolean(ownerError || usersError)}
                users={users}
              />
              {users.length === 0 && !usersLoading && (
                <FieldDescription>No S3 users yet. Internal root can be used as fallback owner.</FieldDescription>
              )}
              {ownerError && <FieldError>{ownerError}</FieldError>}
              {usersError && <FieldError>Failed to load S3 users.</FieldError>}
            </Field>
            <Field>
              <FieldLabel htmlFor="bucket-copies">Replicas</FieldLabel>
              <Select value={copyPolicy} onValueChange={setCopyPolicy} disabled={createBucket.isPending}>
                <SelectTrigger id="bucket-copies" className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectGroup>
                    <SelectItem value={inheritedCopyPolicyValue}>Inherit global default</SelectItem>
                    {copyPolicyOptions.map((copies) => (
                      <SelectItem key={copies} value={copies.toString()}>
                        {copies} {copies === 1 ? 'copy' : 'copies'}
                      </SelectItem>
                    ))}
                  </SelectGroup>
                </SelectContent>
              </Select>
            </Field>
          </FieldGroup>
          {formError && (
            <Alert variant="destructive">
              <AlertDescription>{formError}</AlertDescription>
            </Alert>
          )}
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
  const [reviewing, setReviewing] = useState(false)
  const { data: users = [], isLoading: usersLoading, error: usersError } = useS3Users()
  const updateOwner = useUpdateBucketOwner()

  useEffect(() => {
    if (!open) {
      setOwnerAccessKey(bucket.owner_access_key ?? '')
      setReviewing(false)
    }
  }, [bucket.owner_access_key, open])

  const reset = () => {
    setOwnerAccessKey(bucket.owner_access_key ?? '')
    setReviewing(false)
    updateOwner.reset()
  }

  const handleOpenChange = (next: boolean) => {
    reset()
    setOpen(next)
  }

  const handleUpdate = () => {
    if (!ownerAccessKey || ownerAccessKey === bucket.owner_access_key) return
    if (!reviewing) {
      setReviewing(true)
      return
    }
    updateOwner.mutate(
      { name: bucket.name, ownerAccessKey },
      {
        onSuccess: () => {
          setReviewing(false)
          setOpen(false)
          reset()
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
          <DialogTitle>
            {reviewing
              ? 'Review bucket owner'
              : bucket.owner_access_key
                ? 'Change bucket owner'
                : 'Assign bucket owner'}
          </DialogTitle>
          <DialogDescription>
            {reviewing
              ? 'Confirm the owner that will receive full control of this bucket.'
              : `Transfer full control of "${bucket.name}" to an existing S3 user.`}
          </DialogDescription>
        </DialogHeader>
        {reviewing ? (
          <ReviewDetails
            rows={[
              { id: 'bucket', label: 'Bucket', value: bucket.name, copyable: true },
              {
                id: 'current-owner',
                label: 'Current owner',
                value: bucket.owner_access_key ?? ownerLabel(bucket.owner_access_key),
                displayValue: ownerLabel(bucket.owner_access_key),
                copyable: Boolean(bucket.owner_access_key),
              },
              {
                id: 'new-owner',
                label: 'New owner',
                value: ownerAccessKey || ownerLabel(null),
                displayValue: ownerLabel(ownerAccessKey),
                copyable: Boolean(ownerAccessKey),
              },
            ]}
          />
        ) : (
          <FieldGroup>
            <Field data-invalid={Boolean(usersError)}>
              <FieldLabel htmlFor={`owner-${bucket.id}`}>Owner</FieldLabel>
              <BucketOwnerSelect
                id={`owner-${bucket.id}`}
                value={ownerAccessKey}
                onChange={setOwnerAccessKey}
                disabled={updateOwner.isPending || usersLoading}
                invalid={Boolean(usersError)}
                users={users}
              />
              {users.length === 0 && !usersLoading && (
                <FieldDescription>No S3 users yet. Internal root can be used as fallback owner.</FieldDescription>
              )}
              {usersError && <FieldError>Failed to load S3 users.</FieldError>}
            </Field>
          </FieldGroup>
        )}
        {updateOwner.error && (
          <Alert variant="destructive">
            <AlertDescription>{updateOwner.error.message}</AlertDescription>
          </Alert>
        )}
        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            onClick={() => (reviewing ? setReviewing(false) : handleOpenChange(false))}
            disabled={updateOwner.isPending}
          >
            {reviewing ? 'Back' : 'Cancel'}
          </Button>
          <Button
            type="button"
            onClick={handleUpdate}
            disabled={!ownerAccessKey || ownerAccessKey === bucket.owner_access_key || updateOwner.isPending}
          >
            {updateOwner.isPending && <Loader2 data-icon="inline-start" className="animate-spin" />}
            {reviewing ? 'Confirm owner' : 'Review'}
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
        <PageErrorState title="Failed to load buckets" className="h-60" />
      ) : (
        <div className="overflow-hidden rounded-lg border border-border">
          <Table>
            <TableHeader>
              <TableRow className="bg-muted/50">
                <TableHead className="px-4">Name</TableHead>
                <TableHead className="px-4">Owner</TableHead>
                <TableHead className="px-4">Replicas</TableHead>
                <TableHead className="px-4">Storage Health</TableHead>
                <TableHead className="px-4">Status</TableHead>
                <TableHead className="px-4 text-right">Objects</TableHead>
                <TableHead className="px-4 text-right">Size</TableHead>
                <TableHead className="px-4">Created</TableHead>
                <TableHead className="px-4">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {data && data.length > 0 ? (
                data.map((bucket) => (
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
                    <TableCell className="px-4">{bucketCopyPolicyLabel(bucket)}</TableCell>
                    <TableCell className="px-4">
                      <BucketStorageHealthCell bucket={bucket} />
                    </TableCell>
                    <TableCell className="px-4">
                      <StatusBadge tone={bucketStatusTone(bucket.status)}>{bucket.status}</StatusBadge>
                    </TableCell>
                    <TableCell className="px-4 text-right">{formatNumber(bucket.object_count)}</TableCell>
                    <TableCell className="px-4 text-right">{formatBytes(bucket.total_size_bytes)}</TableCell>
                    <TableCell className="px-4 text-muted-foreground" title={bucket.created_at}>
                      {timeAgo(bucket.created_at)}
                    </TableCell>
                    <TableCell className="px-4">
                      <ChangeBucketOwnerDialog bucket={bucket} />
                    </TableCell>
                  </TableRow>
                ))
              ) : (
                <TableRow>
                  <TableCell colSpan={9} className="h-24 text-center text-muted-foreground">
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

function BucketStorageHealthCell({ bucket }: { bucket: BucketItem }) {
  const health = bucket.storage_health
  return (
    <span className="inline-flex" title={bucketStorageHealthTitle(health)}>
      <StatusBadge tone={bucketStorageHealthStatusTone(health)} className="whitespace-nowrap">
        {bucketStorageHealthLabel(health)}
      </StatusBadge>
    </span>
  )
}
