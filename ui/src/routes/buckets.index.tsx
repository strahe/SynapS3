import { useQueryClient } from '@tanstack/react-query'
import { createFileRoute, Link, useNavigate } from '@tanstack/react-router'
import { Database, FolderOpen, HardDrive, Loader2, Plus, Settings } from 'lucide-react'
import { type FormEvent, useState } from 'react'
import { toast } from 'sonner'
import type { ProviderSelectionStrategy } from '@/api/client'
import { type BucketItem, internalRootOwnerAccessKey, type S3User } from '@/api/client'
import { BucketOwnerSelect } from '@/components/app/BucketOwnerSelect'
import { CopyableValue } from '@/components/app/CopyableValue'
import { clickableRowProps, DataTableFrame, TableSkeleton, tableHeaderRowClassName } from '@/components/app/DataTable'
import { PageHeader, RefreshButton } from '@/components/app/PageHeader'
import { EmptyState, PageError } from '@/components/app/PageState'
import { RelativeTime } from '@/components/app/RelativeTime'
import { RowActionItem, RowActionsMenu } from '@/components/app/RowActionsMenu'
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
} from '@/components/ui/dialog'
import { Field, FieldDescription, FieldError, FieldGroup, FieldLabel } from '@/components/ui/field'
import { Input } from '@/components/ui/input'
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { useBuckets, useCreateBucket, useS3Users, useSettings } from '@/hooks/queries'
import {
  bucketCopyPolicyLabel,
  clampMinimumDurableCopiesValue,
  copyPolicyOptions,
  minimumDurableCopiesChoiceNote,
  minimumDurableCopiesFixedCountNote,
  minimumDurableCopiesOptionLabel,
  minimumDurableCopiesOptions,
  minimumDurableCopiesValue,
  minimumDurableCopiesWarning,
  replicaCountLabel,
  replicaTargetChoiceNote,
  selectedTargetCopies,
  showsMinimumDurableCopiesWarning,
} from '@/lib/bucket-copy-policy'
import type { BucketTab } from '@/lib/bucket-route-search'
import {
  bucketStorageHealthLabel,
  bucketStorageHealthStatusTone,
  bucketStorageHealthTitle,
} from '@/lib/bucket-storage-health'
import { bucketStatusLabel, providerSelectionStrategyLabel } from '@/lib/display-labels'
import { ownerLabel } from '@/lib/s3-owner'
import { formatBytes, formatNumber } from '@/lib/utils'

export const Route = createFileRoute('/buckets/')({
  component: BucketsPage,
})

function CreateBucketDialog({ open, onOpenChange }: { open: boolean; onOpenChange: (open: boolean) => void }) {
  const [providerSelectionStrategy, setProviderSelectionStrategy] = useState<ProviderSelectionStrategy>('distribution')
  const [bucketName, setBucketName] = useState('')
  const [ownerAccessKey, setOwnerAccessKey] = useState('')
  const [copyPolicyOverride, setCopyPolicyOverride] = useState<string | null>(null)
  const [minimumDurableCopiesOverride, setMinimumDurableCopiesOverride] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const { data: users = [], isLoading: usersLoading, error: usersError } = useS3Users()
  const { data: settings } = useSettings()
  const createBucket = useCreateBucket()
  const navigate = useNavigate()
  const runtimeDefaultCopies = settings?.runtime_filecoin_default_copies
  // A bucket stores its own policy, so the dialog only pre-selects the
  // configured target instead of offering an "inherit" value to store.
  const copyPolicy = copyPolicyOverride ?? runtimeDefaultCopies?.toString() ?? ''
  const targetCopies = selectedTargetCopies(copyPolicy)
  const minimumDurableCopies = minimumDurableCopiesOverride ?? targetCopies?.toString() ?? ''
  const minimumOptions = minimumDurableCopiesOptions(targetCopies)

  const reset = () => {
    setBucketName('')
    setProviderSelectionStrategy('distribution')
    setOwnerAccessKey('')
    setCopyPolicyOverride(null)
    setMinimumDurableCopiesOverride(null)
    setError(null)
    createBucket.reset()
  }

  const handleOpenChange = (next: boolean) => {
    if (!next) reset()
    onOpenChange(next)
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
    const defaultCopies = targetCopies
    const minimumCopies = selectedTargetCopies(minimumDurableCopies)
    createBucket.mutate(
      { name, ownerAccessKey, defaultCopies, minimumDurableCopies: minimumCopies, providerSelectionStrategy },
      {
        onSuccess: (bucket) => {
          handleOpenChange(false)
          toast.success(`Created bucket ${bucket.name}`)
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
  const handleCopyPolicyChange = (next: string) => {
    const nextTarget = selectedTargetCopies(next)
    setCopyPolicyOverride(next)
    setMinimumDurableCopiesOverride((current) =>
      current == null ? null : clampMinimumDurableCopiesValue(current, nextTarget)
    )
  }

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Create bucket</DialogTitle>
          <DialogDescription>
            Choose the bucket's owner and how many Filecoin replicas new uploads keep.
          </DialogDescription>
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
              <Select value={copyPolicy} onValueChange={handleCopyPolicyChange} disabled={createBucket.isPending}>
                <SelectTrigger id="bucket-copies" className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectGroup>
                    {copyPolicyOptions.map((copies) => (
                      <SelectItem key={copies} value={copies.toString()}>
                        {replicaCountLabel(copies)}
                      </SelectItem>
                    ))}
                  </SelectGroup>
                </SelectContent>
              </Select>
              {targetCopies == null ? (
                <FieldDescription>
                  The current runtime default is unavailable. Choose a replica count for this bucket.
                </FieldDescription>
              ) : (
                <FieldDescription>{replicaTargetChoiceNote()}</FieldDescription>
              )}
            </Field>
            <Field>
              <FieldLabel htmlFor="bucket-minimum-durable-copies">Release cache after</FieldLabel>
              <Select
                value={minimumDurableCopies}
                onValueChange={setMinimumDurableCopiesOverride}
                disabled={createBucket.isPending}
              >
                <SelectTrigger id="bucket-minimum-durable-copies" className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectGroup>
                    {minimumOptions.map((copies) => (
                      <SelectItem key={copies} value={copies.toString()}>
                        {minimumDurableCopiesOptionLabel(copies, targetCopies)}
                      </SelectItem>
                    ))}
                  </SelectGroup>
                </SelectContent>
              </Select>
              {targetCopies != null && Number(minimumDurableCopies) === targetCopies ? (
                <FieldDescription>{minimumDurableCopiesChoiceNote()}</FieldDescription>
              ) : (
                <FieldDescription>{minimumDurableCopiesFixedCountNote()}</FieldDescription>
              )}
            </Field>
            <Field>
              <FieldLabel htmlFor="bucket-provider-preference">Provider preference</FieldLabel>
              <Select
                value={providerSelectionStrategy}
                onValueChange={(value) => {
                  if (value === 'distribution' || value === 'speed') setProviderSelectionStrategy(value)
                }}
                disabled={createBucket.isPending}
              >
                <SelectTrigger id="bucket-provider-preference" className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectGroup>
                    <SelectItem value="distribution">{providerSelectionStrategyLabel('distribution')}</SelectItem>
                    <SelectItem value="speed">{providerSelectionStrategyLabel('speed')}</SelectItem>
                  </SelectGroup>
                </SelectContent>
              </Select>
              <FieldDescription>
                {providerSelectionStrategy === 'speed'
                  ? 'Prefers the fastest measured providers.'
                  : 'Spreads new data sets across the least-used providers.'}{' '}
                This can't be changed later.
              </FieldDescription>
            </Field>
          </FieldGroup>
          {showsMinimumDurableCopiesWarning(minimumDurableCopies, targetCopies) && (
            <Alert>
              <AlertDescription>{minimumDurableCopiesWarning()}</AlertDescription>
            </Alert>
          )}
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

function BucketsPage() {
  const { data, isLoading, error, refetch, isFetching } = useBuckets()
  const { data: users = [] } = useS3Users()
  const navigate = useNavigate()
  const qc = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const buckets = data ?? []
  const openBucket = (name: string, tab?: BucketTab) =>
    navigate({ to: '/buckets/$name', params: { name }, search: { tab: tab === 'objects' ? undefined : tab } })

  return (
    <div className="flex flex-col gap-6 p-6">
      <PageHeader
        title="Buckets"
        actions={
          <>
            <RefreshButton onClick={() => qc.invalidateQueries({ queryKey: ['buckets'] })} refreshing={isFetching} />
            <Button size="sm" onClick={() => setCreateOpen(true)}>
              <Plus data-icon="inline-start" />
              Create bucket
            </Button>
          </>
        }
      />

      {isLoading ? (
        <TableSkeleton />
      ) : error ? (
        <PageError
          title="Failed to load buckets"
          description={error.message}
          onRetry={() => refetch()}
          retrying={isFetching}
        />
      ) : buckets.length === 0 ? (
        <EmptyState
          icon={<Database />}
          title="No buckets yet"
          description="Create a bucket here or with any S3 client."
          action={
            <Button size="sm" variant="outline" onClick={() => setCreateOpen(true)}>
              <Plus data-icon="inline-start" />
              Create bucket
            </Button>
          }
        />
      ) : (
        <DataTableFrame>
          <Table className="min-w-[960px]">
            <TableHeader>
              <TableRow className={tableHeaderRowClassName}>
                <TableHead className="px-4">Name</TableHead>
                <TableHead className="px-4">Owner</TableHead>
                <TableHead className="px-4">Replicas</TableHead>
                <TableHead className="px-4">Storage health</TableHead>
                <TableHead className="px-4">Status</TableHead>
                <TableHead className="px-4 text-right">Objects</TableHead>
                <TableHead className="px-4 text-right">Size</TableHead>
                <TableHead className="px-4">Created</TableHead>
                <TableHead className="w-12 px-4">
                  <span className="sr-only">Actions</span>
                </TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {buckets.map((bucket) => (
                <TableRow key={bucket.id} {...clickableRowProps(() => openBucket(bucket.name))}>
                  <TableCell className="px-4">
                    <Link to="/buckets/$name" params={{ name: bucket.name }} className="font-medium hover:underline">
                      {bucket.name}
                    </Link>
                  </TableCell>
                  <TableCell className="px-4">
                    <OwnerCell ownerAccessKey={bucket.owner_access_key} users={users} />
                  </TableCell>
                  <TableCell className="px-4">
                    <div>{bucketCopyPolicyLabel(bucket)}</div>
                    <div className="text-xs text-muted-foreground">
                      Release cache after {minimumDurableCopiesValue(bucket)}
                    </div>
                  </TableCell>
                  <TableCell className="px-4">
                    <BucketStorageHealthCell bucket={bucket} />
                  </TableCell>
                  <TableCell className="px-4">
                    <StatusBadge tone={bucketStatusTone(bucket.status)}>{bucketStatusLabel(bucket.status)}</StatusBadge>
                  </TableCell>
                  <TableCell className="px-4 text-right tabular-nums">{formatNumber(bucket.object_count)}</TableCell>
                  <TableCell className="px-4 text-right tabular-nums">{formatBytes(bucket.total_size_bytes)}</TableCell>
                  <TableCell className="px-4 text-muted-foreground">
                    <RelativeTime value={bucket.created_at} />
                  </TableCell>
                  <TableCell className="px-4 text-right">
                    <RowActionsMenu label={bucket.name}>
                      <RowActionItem onSelect={() => openBucket(bucket.name)}>
                        <FolderOpen data-icon="inline-start" />
                        Open
                      </RowActionItem>
                      <RowActionItem onSelect={() => openBucket(bucket.name, 'storage')}>
                        <HardDrive data-icon="inline-start" />
                        Storage
                      </RowActionItem>
                      <RowActionItem onSelect={() => openBucket(bucket.name, 'settings')}>
                        <Settings data-icon="inline-start" />
                        Settings
                      </RowActionItem>
                    </RowActionsMenu>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </DataTableFrame>
      )}
      <CreateBucketDialog open={createOpen} onOpenChange={setCreateOpen} />
    </div>
  )
}

function OwnerCell({ ownerAccessKey, users }: { ownerAccessKey: string | null; users: S3User[] }) {
  if (!ownerAccessKey) {
    return <StatusBadge tone="warning">Unassigned</StatusBadge>
  }
  if (ownerAccessKey === internalRootOwnerAccessKey) {
    return <StatusBadge tone="neutral">Internal root</StatusBadge>
  }
  return (
    <div className="max-w-56">
      <CopyableValue
        label="Owner access key"
        value={ownerAccessKey}
        displayValue={ownerLabel(ownerAccessKey, users)}
        maxLength={28}
      />
    </div>
  )
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
