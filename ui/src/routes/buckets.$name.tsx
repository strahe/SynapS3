import { useQueryClient } from '@tanstack/react-query'
import { createFileRoute, Link, useNavigate } from '@tanstack/react-router'
import {
  CheckCircle2,
  CircleSlash,
  Clock3,
  Download,
  FileIcon,
  Fingerprint,
  Folder,
  History,
  Info,
  Loader2,
  MoreHorizontal,
  RefreshCw,
  RotateCcw,
  Trash2,
  TriangleAlert,
  Upload,
  UserRound,
} from 'lucide-react'
import { type ChangeEvent, Fragment, type ReactNode, useEffect, useRef, useState } from 'react'
import {
  api,
  type DeletedObjectItem,
  maxFOCUploadSize,
  minFOCUploadSize,
  type ObjectFolderItem,
  type ObjectItem,
  type ObjectProvenance,
  type ObjectProvenanceCopy,
  type ObjectProvenanceFailure,
  type ObjectState,
  type ObjectStatus,
  type ObjectUploadClientProgress,
  type ObjectUploadStatus,
  type ObjectVersionItem,
  type ProviderIdentity,
  type StorageDataSetSummary,
  type StorageHealthStatus,
  type UploadTransferProgress,
  validateFOCUploadSize,
} from '@/api/client'
import { BreadcrumbCurrentPage } from '@/components/app/BreadcrumbCurrentPage'
import { BucketOwnerSelect } from '@/components/app/BucketOwnerSelect'
import { DangerActionAlertDialog } from '@/components/app/DangerActionAlertDialog'
import { DetailTextDialog } from '@/components/app/DetailTextDialog'
import { PageHeader } from '@/components/app/PageHeader'
import { ReviewDetails } from '@/components/app/ReviewDetails'
import { bucketStatusTone, StatusBadge, type StatusTone } from '@/components/app/StatusBadge'
import { UploadProgressRing, uploadProgressPercent } from '@/components/app/UploadProgress'
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbSeparator,
} from '@/components/ui/breadcrumb'
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
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuGroup,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from '@/components/ui/empty'
import { Field, FieldDescription, FieldGroup, FieldLabel } from '@/components/ui/field'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { Progress } from '@/components/ui/progress'
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area'
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from '@/components/ui/sheet'
import { Skeleton } from '@/components/ui/skeleton'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import {
  useBucket,
  useBucketObjects,
  useBucketObjectVersions,
  useDeleteBucket,
  useDeleteBucketObject,
  useDeletedBucketObjects,
  useObjectProvenance,
  useObjectStatusDetail,
  usePermanentDeleteBucketObjectVersion,
  usePermanentDeleteDeletedBucketObject,
  useRefreshDataSetStorageHealth,
  useRestoreBucketObject,
  useS3Users,
  useUpdateBucketCopyPolicy,
  useUpdateBucketOwner,
} from '@/hooks/queries'
import {
  bucketCopyPolicyEffectNote,
  bucketCopyPolicyInheritOptionLabel,
  bucketCopyPolicyLabel,
  bucketCopyPolicySavedMessage,
  bucketCopyPolicyValue,
  copyPolicyOptions,
  inheritedCopyPolicyValue,
} from '@/lib/bucket-copy-policy'
import { dataSetStorageHealthDetailParts, dataSetStorageHealthRefreshErrorMessage } from '@/lib/data-set-storage-health'
import { ownerLabel } from '@/lib/s3-owner'
import { type BucketPrefixCrumb, bucketPrefixCrumbs, duplicateObjectUploadKeys, objectUploadKey } from '@/lib/s3-prefix'
import { objectStateLabel, replicaLabel, transferMethodLabel, uploadStatusLabel } from '@/lib/storage-status-labels'
import { cn, formatBytes, formatNumber, timeAgo } from '@/lib/utils'

type ObjectBrowserSearch = {
  prefix?: string
  marker?: string
  view?: 'objects' | 'deleted'
}
type ProvenanceFailureDialogState = { title: string; text: string }

const objectBrowserSkeletonRows = ['row-1', 'row-2', 'row-3', 'row-4', 'row-5', 'row-6', 'row-7', 'row-8']

export const Route = createFileRoute('/buckets/$name')({
  validateSearch: (search: Record<string, unknown>): ObjectBrowserSearch => ({
    prefix: normalizePrefixSearch(search.prefix),
    marker: normalizeSearchString(search.marker),
    view: search.view === 'deleted' ? search.view : undefined,
  }),
  component: ObjectBrowserPage,
})

function normalizeSearchString(value: unknown) {
  return typeof value === 'string' && value.length > 0 ? value : undefined
}

function normalizePrefixSearch(value: unknown) {
  const prefix = normalizeSearchString(value)
  if (!prefix) return undefined
  return prefix.endsWith('/') ? prefix : `${prefix}/`
}

function DeleteBucketDetailDialog({
  bucketName,
  objectCount,
  open: controlledOpen,
  onOpenChange,
  showTrigger = true,
}: {
  bucketName: string
  objectCount: number
  open?: boolean
  onOpenChange?: (open: boolean) => void
  showTrigger?: boolean
}) {
  const [internalOpen, setInternalOpen] = useState(false)
  const [confirmName, setConfirmName] = useState('')
  const [error, setError] = useState<string | null>(null)
  const deleteBucket = useDeleteBucket()
  const navigate = useNavigate()

  const dialogOpen = controlledOpen ?? internalOpen
  const setDialogOpen = onOpenChange ?? setInternalOpen
  const recursive = objectCount > 0
  const nameMatches = confirmName === bucketName

  const reset = () => {
    setConfirmName('')
    setError(null)
    deleteBucket.reset()
  }

  const handleOpenChange = (next: boolean) => {
    if (!next) reset()
    setDialogOpen(next)
  }

  const handleDelete = () => {
    if (!nameMatches) return
    setError(null)
    deleteBucket.mutate(
      { name: bucketName, recursive },
      {
        onSuccess: () => {
          setDialogOpen(false)
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
    <Dialog open={dialogOpen} onOpenChange={handleOpenChange}>
      {showTrigger && (
        <DialogTrigger asChild>
          <Button variant="destructive" size="sm">
            <Trash2 data-icon="inline-start" />
            Delete
          </Button>
        </DialogTrigger>
      )}
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Delete bucket "{bucketName}"</DialogTitle>
          <DialogDescription>
            {recursive
              ? `This will recursively purge ${formatNumber(objectCount)} object(s) and their cached data. Deletion is blocked while lifecycle tasks, object processing, or multipart uploads are in flight.`
              : 'This empty bucket will be marked for deletion. Deletion is blocked while lifecycle tasks or multipart uploads are in flight.'}
          </DialogDescription>
        </DialogHeader>
        <div className="flex flex-col gap-2">
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
            {deleteBucket.isPending && <Loader2 data-icon="inline-start" className="animate-spin" />}
            Delete bucket
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

function ChangeBucketOwnerDetailDialog({
  bucketName,
  ownerAccessKey,
  open: controlledOpen,
  onOpenChange,
  showTrigger = true,
}: {
  bucketName: string
  ownerAccessKey: string | null
  open?: boolean
  onOpenChange?: (open: boolean) => void
  showTrigger?: boolean
}) {
  const [internalOpen, setInternalOpen] = useState(false)
  const [selectedOwner, setSelectedOwner] = useState(ownerAccessKey ?? '')
  const [reviewing, setReviewing] = useState(false)
  const { data: users = [], isLoading: usersLoading, error: usersError } = useS3Users()
  const updateOwner = useUpdateBucketOwner()
  const dialogOpen = controlledOpen ?? internalOpen
  const setDialogOpen = onOpenChange ?? setInternalOpen

  useEffect(() => {
    if (!dialogOpen) {
      setSelectedOwner(ownerAccessKey ?? '')
      setReviewing(false)
    }
  }, [ownerAccessKey, dialogOpen])

  const reset = () => {
    setSelectedOwner(ownerAccessKey ?? '')
    setReviewing(false)
    updateOwner.reset()
  }

  const handleOpenChange = (next: boolean) => {
    if (!next) reset()
    setDialogOpen(next)
  }

  const handleUpdate = () => {
    if (!selectedOwner || selectedOwner === ownerAccessKey) return
    if (!reviewing) {
      setReviewing(true)
      return
    }
    updateOwner.mutate(
      { name: bucketName, ownerAccessKey: selectedOwner },
      {
        onSuccess: () => {
          setReviewing(false)
          setDialogOpen(false)
          reset()
        },
      }
    )
  }

  return (
    <Dialog open={dialogOpen} onOpenChange={handleOpenChange}>
      {showTrigger && (
        <DialogTrigger asChild>
          <Button variant="outline" size="sm">
            <UserRound data-icon="inline-start" />
            {ownerAccessKey ? 'Change owner' : 'Assign owner'}
          </Button>
        </DialogTrigger>
      )}
      <DialogContent>
        <DialogHeader>
          <DialogTitle>
            {reviewing ? 'Review bucket owner' : ownerAccessKey ? 'Change bucket owner' : 'Assign bucket owner'}
          </DialogTitle>
          <DialogDescription>
            {reviewing
              ? 'Confirm the owner that will receive full control of this bucket.'
              : `Transfer full control of "${bucketName}" to an existing S3 user.`}
          </DialogDescription>
        </DialogHeader>
        {reviewing ? (
          <ReviewDetails
            rows={[
              { id: 'bucket', label: 'Bucket', value: bucketName },
              { id: 'current-owner', label: 'Current owner', value: ownerLabel(ownerAccessKey) },
              { id: 'new-owner', label: 'New owner', value: ownerLabel(selectedOwner) },
            ]}
          />
        ) : (
          <div className="flex flex-col gap-2">
            <Label htmlFor="bucket-detail-owner">Owner</Label>
            <BucketOwnerSelect
              id="bucket-detail-owner"
              value={selectedOwner}
              onChange={setSelectedOwner}
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
        )}
        {updateOwner.error && <p className="text-sm text-destructive">{updateOwner.error.message}</p>}
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
            disabled={!selectedOwner || selectedOwner === ownerAccessKey || updateOwner.isPending}
          >
            {updateOwner.isPending && <Loader2 data-icon="inline-start" className="animate-spin" />}
            {reviewing ? 'Confirm owner' : 'Review'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

function ObjectVersionsDialog({
  bucketName,
  objectKey,
  open,
  onOpenChange,
}: {
  bucketName: string
  objectKey: string
  open: boolean
  onOpenChange: (open: boolean) => void
}) {
  const [versionMarker, setVersionMarker] = useState('')
  const titleRef = useRef<HTMLHeadingElement>(null)
  const versions = useBucketObjectVersions(bucketName, objectKey, versionMarker, 50, open)

  useEffect(() => {
    if (open) setVersionMarker('')
  }, [open])

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent
        className="w-[calc(100vw-2rem)] max-w-[calc(100vw-2rem)] sm:max-w-6xl lg:p-6"
        onOpenAutoFocus={(event) => {
          event.preventDefault()
          titleRef.current?.focus({ preventScroll: true })
        }}
      >
        <DialogHeader>
          <DialogTitle ref={titleRef} tabIndex={-1} className="outline-none">
            Object versions
          </DialogTitle>
          <DialogDescription className="pr-8">
            <span className="block max-w-full truncate font-mono text-xs" title={objectKey}>
              {objectKey}
            </span>
          </DialogDescription>
        </DialogHeader>
        {versions.isLoading ? (
          <div className="flex h-40 items-center justify-center">
            <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
          </div>
        ) : versions.error ? (
          <div className="text-sm text-destructive">Failed to load object versions</div>
        ) : (
          <div className="overflow-hidden rounded-md border border-border">
            <Table className="table-fixed">
              <colgroup>
                <col className="w-[22%]" />
                <col className="w-[8%]" />
                <col className="w-[15%]" />
                <col className="w-[18%]" />
                <col className="w-[20%]" />
                <col className="w-[10%]" />
                <col className="w-[7%]" />
              </colgroup>
              <TableHeader>
                <TableRow className="bg-muted/50">
                  <TableHead className="px-2">Version</TableHead>
                  <TableHead className="px-2 text-right">Size</TableHead>
                  <TableHead className="px-2">Location</TableHead>
                  <TableHead className="px-2">ETag</TableHead>
                  <TableHead className="px-2">Piece CID</TableHead>
                  <TableHead className="px-2">Created</TableHead>
                  <TableHead className="px-2 text-right">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {versions.data?.versions.map((version) => (
                  <TableRow key={version.version_id}>
                    <TableCell className="overflow-hidden px-2" title={version.version_id}>
                      <div className="flex min-w-0 items-center gap-2">
                        <span className="min-w-0 truncate font-mono text-xs">{version.version_id}</span>
                        {version.is_delete_marker ? (
                          <StatusBadge tone="neutral" className="shrink-0">
                            Deleted
                          </StatusBadge>
                        ) : (
                          <ObjectStatusIcon
                            bucketName={bucketName}
                            versionID={version.version_id}
                            state={version.state}
                            status={version.status}
                            uploadStatus={version.upload_status}
                            progress={version.progress}
                            compact
                          />
                        )}
                        {version.is_current && (
                          <StatusBadge tone="success" className="shrink-0">
                            Current
                          </StatusBadge>
                        )}
                      </div>
                    </TableCell>
                    <TableCell className="overflow-hidden px-2 text-right">{formatBytes(version.size)}</TableCell>
                    <TableCell className="overflow-hidden px-2">
                      <LocationBadges location={version.location} />
                    </TableCell>
                    <TableCell
                      className="overflow-hidden truncate px-2 font-mono text-xs text-muted-foreground"
                      title={version.etag}
                    >
                      {version.etag}
                    </TableCell>
                    <TableCell
                      className="overflow-hidden truncate px-2 font-mono text-xs text-muted-foreground"
                      title={version.piece_cid ?? undefined}
                    >
                      {version.piece_cid ?? '—'}
                    </TableCell>
                    <TableCell
                      className="overflow-hidden truncate px-2 text-muted-foreground"
                      title={version.created_at}
                    >
                      {timeAgo(version.created_at)}
                    </TableCell>
                    <TableCell className="px-2 text-right">
                      <VersionActions bucketName={bucketName} objectKey={objectKey} version={version} />
                    </TableCell>
                  </TableRow>
                ))}
                {versions.data?.versions.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={7} className="h-20 text-center text-muted-foreground">
                      No versions found
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </div>
        )}
        {versions.data?.has_more && versions.data.next_version_marker && (
          <DialogFooter>
            <Button
              variant="outline"
              size="sm"
              onClick={() => setVersionMarker(versions.data.next_version_marker ?? '')}
            >
              Next page
            </Button>
          </DialogFooter>
        )}
      </DialogContent>
    </Dialog>
  )
}

function VersionActions({
  bucketName,
  objectKey,
  version,
}: {
  bucketName: string
  objectKey: string
  version: ObjectVersionItem
}) {
  const [provenanceOpen, setProvenanceOpen] = useState(false)
  const [permanentDeleteOpen, setPermanentDeleteOpen] = useState(false)
  const permanentDelete = usePermanentDeleteBucketObjectVersion()

  if (version.is_delete_marker) {
    return <span className="text-xs text-muted-foreground">-</span>
  }

  const handlePermanentDelete = () => {
    permanentDelete.mutate(
      { name: bucketName, key: objectKey, versionID: version.version_id },
      {
        onSuccess: () => {
          setPermanentDeleteOpen(false)
          permanentDelete.reset()
        },
      }
    )
  }

  return (
    <>
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button variant="ghost" size="icon-sm" aria-label={`Actions for ${version.version_id}`} title="Actions">
            <MoreHorizontal />
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end" className="w-40">
          <DropdownMenuGroup>
            <DropdownMenuItem asChild>
              <a
                href={api.getObjectDownloadUrl(bucketName, objectKey, version.version_id)}
                aria-label={`Download ${objectKey} version ${version.version_id}`}
              >
                <Download data-icon="inline-start" />
                Download
              </a>
            </DropdownMenuItem>
            <DropdownMenuItem onSelect={() => setProvenanceOpen(true)}>
              <Fingerprint data-icon="inline-start" />
              Provenance
            </DropdownMenuItem>
            {!version.is_delete_marker && (
              <DropdownMenuItem variant="destructive" onSelect={() => setPermanentDeleteOpen(true)}>
                <Trash2 data-icon="inline-start" />
                Permanently delete
              </DropdownMenuItem>
            )}
          </DropdownMenuGroup>
        </DropdownMenuContent>
      </DropdownMenu>
      <ObjectProvenanceDialog
        bucketName={bucketName}
        objectKey={objectKey}
        versionID={version.version_id}
        open={provenanceOpen}
        onOpenChange={setProvenanceOpen}
      />
      <DangerActionAlertDialog
        open={permanentDeleteOpen}
        onOpenChange={(next) => {
          setPermanentDeleteOpen(next)
          if (!next) permanentDelete.reset()
        }}
        title="Permanently delete version"
        description="This permanently deletes this version. Storage used only by this version will be released in the background."
        confirmLabel="Permanently delete"
        pending={permanentDelete.isPending}
        error={permanentDelete.error?.message}
        onConfirm={handlePermanentDelete}
      >
        <ReviewDetails
          rows={[
            { id: 'key', label: 'Object', value: objectKey },
            { id: 'version', label: 'Version', value: version.version_id },
            { id: 'size', label: 'Size', value: formatBytes(version.size) },
          ]}
        />
      </DangerActionAlertDialog>
    </>
  )
}

function ObjectProvenanceDialog({
  bucketName,
  objectKey,
  versionID,
  open,
  onOpenChange,
}: {
  bucketName: string
  objectKey: string
  versionID: string
  open: boolean
  onOpenChange: (open: boolean) => void
}) {
  const provenance = useObjectProvenance(bucketName, versionID, open)
  const data = provenance.data
  const [failureDialog, setFailureDialog] = useState<ProvenanceFailureDialogState | null>(null)

  const handleOpenChange = (next: boolean) => {
    if (!next) setFailureDialog(null)
    onOpenChange(next)
  }

  return (
    <>
      <Dialog open={open} onOpenChange={handleOpenChange}>
        <DialogContent className="w-[calc(100vw-2rem)] max-w-[calc(100vw-2rem)] sm:max-w-5xl lg:p-6">
          <DialogHeader>
            <DialogTitle>Storage provenance</DialogTitle>
            <DialogDescription className="pr-8">
              <span className="block max-w-full truncate font-mono text-xs" title={objectKey}>
                {objectKey}
              </span>
              <span className="block max-w-full truncate font-mono text-xs" title={versionID}>
                {versionID}
              </span>
            </DialogDescription>
          </DialogHeader>

          {provenance.isLoading ? (
            <div className="flex h-40 items-center justify-center">
              <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
            </div>
          ) : provenance.error ? (
            <div className="text-sm text-destructive">Failed to load provenance</div>
          ) : data ? (
            <div className="flex max-h-[70vh] flex-col gap-4 overflow-y-auto pr-1">
              <ProvenanceSummary data={data} />
              <ProvenanceCopies copies={data.copies} />
              <ProvenanceFailures
                failures={data.failures}
                onOpenError={(failure) => {
                  if (!failure.error) return
                  setFailureDialog({ title: 'Failed Attempt Error', text: failure.error })
                }}
              />
            </div>
          ) : null}
        </DialogContent>
      </Dialog>
      <DetailTextDialog
        title={failureDialog?.title ?? 'Failed Attempt Error'}
        text={failureDialog?.text ?? null}
        onClose={() => setFailureDialog(null)}
      />
    </>
  )
}

function ProvenanceSummary({ data }: { data: ObjectProvenance }) {
  const progressPercent = data.upload_status === 'running' ? uploadProgressPercent(data.progress) : null
  const uploadLabel = data.upload_status ? uploadStatusLabel(data.upload_status, progressPercent) : 'No upload recorded'

  return (
    <dl className="grid gap-3 rounded-md border border-border p-3 text-sm sm:grid-cols-2 lg:grid-cols-4">
      <ProvenanceSummaryItem
        label="Object status"
        value={objectStateLabel(data.state, data.status, data.upload_status, progressPercent)}
      />
      <ProvenanceSummaryItem label="Upload" value={uploadLabel} />
      <ProvenanceSummaryItem label="Replicas" value={`${data.success_copies} / ${data.requested_copies}`} />
      <ProvenanceSummaryItem label="Updated" value={timeAgo(data.updated_at)} title={data.updated_at} />
      <ProvenanceSummaryItem
        label="Piece CID"
        value={data.piece_cid ?? '—'}
        title={data.piece_cid}
        className="sm:col-span-2 lg:col-span-4"
        mono
      />
    </dl>
  )
}

function ProvenanceSummaryItem({
  label,
  value,
  title,
  className,
  mono = false,
}: {
  label: string
  value: string
  title?: string
  className?: string
  mono?: boolean
}) {
  return (
    <div className={className}>
      <dt className="text-xs text-muted-foreground">{label}</dt>
      <dd className={`mt-1 truncate ${mono ? 'font-mono text-xs' : 'font-medium'}`} title={title ?? value}>
        {value}
      </dd>
    </div>
  )
}

function ProviderIdentityCell({ providerID, identity }: { providerID?: string; identity?: ProviderIdentity }) {
  const [detailsOpen, setDetailsOpen] = useState(false)
  const registryID = identity?.registry_provider_id ?? providerID
  const label = identity?.name?.trim() || (registryID ? `Registry #${registryID}` : '—')

  if (!identity) {
    if (!registryID) {
      return (
        <span className="block max-w-44 truncate font-mono text-xs text-muted-foreground" title={registryID}>
          {label}
        </span>
      )
    }

    return <ProviderTopologyLink providerID={registryID} label={label} className="font-mono text-xs" />
  }

  return (
    <div className="flex min-w-0 items-center gap-1.5">
      {registryID ? <ProviderTopologyLink providerID={registryID} label={label} /> : <span>{label}</span>}
      <Popover open={detailsOpen} onOpenChange={setDetailsOpen}>
        <PopoverTrigger asChild>
          <Button
            type="button"
            variant="ghost"
            size="icon-xs"
            aria-label={`Provider details for ${label}`}
            aria-expanded={detailsOpen}
          >
            <Info />
          </Button>
        </PopoverTrigger>
        <PopoverContent
          side="top"
          className="max-h-[min(calc(100vh-2rem),32rem)] w-max max-w-[min(calc(100vw-2rem),36rem)] overflow-y-auto whitespace-normal p-3.5 text-left"
        >
          <ProviderIdentityDetails providerID={registryID} identity={identity} />
        </PopoverContent>
      </Popover>
    </div>
  )
}

function ProviderTopologyLink({
  providerID,
  label,
  className,
}: {
  providerID: string
  label: string
  className?: string
}) {
  return (
    <Link
      to="/storage-topology"
      search={{ provider: providerID }}
      className={cn('block min-w-0 max-w-44 truncate font-medium hover:underline', className)}
      title={label}
    >
      {label}
    </Link>
  )
}

function ProviderIdentityDetails({ providerID, identity }: { providerID?: string; identity: ProviderIdentity }) {
  const allFields: Array<[string, string | undefined]> = [
    ['Registry Provider ID', identity.registry_provider_id || providerID],
    ['Actor ID', identity.filecoin_actor_id],
    ['Filecoin address', identity.filecoin_address],
    ['EVM service provider', identity.service_provider_address],
    ['Payee address', identity.payee_address],
    ['Service URL', identity.service_url],
    ['Location', identity.location],
    ['Description', identity.description],
  ]
  const fields = allFields.filter((field): field is [string, string] => Boolean(field[1]))
  const extras = Object.entries(identity.extra_capabilities ?? {}).sort(([a], [b]) => a.localeCompare(b))

  return (
    <div className="flex w-full select-text flex-col gap-3">
      <div className="font-medium">
        {identity.name?.trim() || `Registry #${identity.registry_provider_id || providerID}`}
      </div>
      <div className="grid grid-cols-1 gap-x-3 gap-y-2 text-xs sm:grid-cols-[9rem_minmax(0,1fr)]">
        {fields.map(([label, value]) => (
          <Fragment key={label}>
            <span className="text-muted-foreground">{label}</span>
            <span className="min-w-0 break-words font-mono leading-relaxed">{value}</span>
          </Fragment>
        ))}
        {extras.map(([label, value]) => (
          <Fragment key={label}>
            <span className="text-muted-foreground">{label}</span>
            <span className="min-w-0 break-words font-mono leading-relaxed">{value}</span>
          </Fragment>
        ))}
      </div>
    </div>
  )
}

function ProvenanceCopies({ copies }: { copies: ObjectProvenanceCopy[] }) {
  return (
    <div className="overflow-hidden rounded-md border border-border">
      <div className="border-b border-border bg-muted/50 px-3 py-2 text-sm font-medium">Replicas</div>
      <ScrollArea className="w-full">
        <Table className="min-w-[960px]">
          <TableHeader>
            <TableRow>
              <TableHead className="px-3">Replica</TableHead>
              <TableHead className="px-3">Transfer</TableHead>
              <TableHead className="px-3">Status</TableHead>
              <TableHead className="px-3">Provider</TableHead>
              <TableHead className="px-3">Data Set ID</TableHead>
              <TableHead className="px-3">Piece ID</TableHead>
              <TableHead className="px-3">New</TableHead>
              <TableHead className="px-3">Retrieval URL</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {copies.map((copy) => (
              <TableRow key={`${copy.copy_index}-${copy.transfer_method}`}>
                <TableCell className="px-3 font-mono text-xs">{replicaLabel(copy.copy_index)}</TableCell>
                <TableCell className="px-3">{transferMethodLabel(copy.transfer_method)}</TableCell>
                <TableCell className="px-3">
                  <StatusBadge tone={copyStatusTone(copy.status)}>{copy.status}</StatusBadge>
                </TableCell>
                <TableCell className="px-3">
                  <ProviderIdentityCell providerID={copy.provider_id} identity={copy.provider_identity} />
                </TableCell>
                <TableCell className="px-3 font-mono text-xs text-muted-foreground">
                  {copy.data_set_id ?? '—'}
                </TableCell>
                <TableCell className="px-3 font-mono text-xs text-muted-foreground">{copy.piece_id ?? '—'}</TableCell>
                <TableCell className="px-3">
                  <StatusBadge tone={copy.is_new_data_set ? 'info' : 'neutral'}>
                    {copy.is_new_data_set ? 'Yes' : 'No'}
                  </StatusBadge>
                </TableCell>
                <TableCell className="max-w-72 overflow-hidden truncate px-3 font-mono text-xs text-muted-foreground">
                  {copy.retrieval_url ? (
                    <a
                      href={copy.retrieval_url}
                      target="_blank"
                      rel="noreferrer"
                      className="hover:text-foreground hover:underline"
                      title={copy.retrieval_url}
                    >
                      {copy.retrieval_url}
                    </a>
                  ) : (
                    '—'
                  )}
                </TableCell>
              </TableRow>
            ))}
            {copies.length === 0 && (
              <TableRow>
                <TableCell colSpan={8} className="h-20 text-center text-muted-foreground">
                  No replicas recorded
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
        <ScrollBar orientation="horizontal" />
      </ScrollArea>
    </div>
  )
}

function ProvenanceFailures({
  failures,
  onOpenError,
}: {
  failures: ObjectProvenanceFailure[]
  onOpenError: (failure: ObjectProvenanceFailure) => void
}) {
  return (
    <div className="overflow-hidden rounded-md border border-border">
      <div className="border-b border-border bg-muted/50 px-3 py-2 text-sm font-medium">Failed attempts</div>
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead className="px-3">Provider</TableHead>
            <TableHead className="px-3">Transfer</TableHead>
            <TableHead className="px-3">Stage</TableHead>
            <TableHead className="px-3">Error</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {failures.map((failure) => (
            <TableRow key={`${failure.attempt_index}-${failure.provider_id ?? 'unknown'}`}>
              <TableCell className="px-3">
                <ProviderIdentityCell providerID={failure.provider_id} identity={failure.provider_identity} />
              </TableCell>
              <TableCell className="px-3">{transferMethodLabel(failure.transfer_method)}</TableCell>
              <TableCell className="px-3">{failure.stage ?? '—'}</TableCell>
              <TableCell className="max-w-md px-3">
                <ProvenanceFailureErrorCell failure={failure} onOpenError={onOpenError} />
              </TableCell>
            </TableRow>
          ))}
          {failures.length === 0 && (
            <TableRow>
              <TableCell colSpan={4} className="h-20 text-center text-muted-foreground">
                No failed attempts recorded
              </TableCell>
            </TableRow>
          )}
        </TableBody>
      </Table>
    </div>
  )
}

function ProvenanceFailureErrorCell({
  failure,
  onOpenError,
}: {
  failure: ObjectProvenanceFailure
  onOpenError: (failure: ObjectProvenanceFailure) => void
}) {
  if (!failure.error) {
    return <span className="text-muted-foreground">—</span>
  }
  return (
    <Button
      type="button"
      variant="link"
      onClick={() => onOpenError(failure)}
      className="h-auto max-w-full justify-start p-0 text-left text-xs font-normal text-muted-foreground hover:text-foreground"
    >
      <span className="truncate">{failure.error}</span>
    </Button>
  )
}

function LocationBadges({ location }: { location: { cache: boolean; filecoin: boolean } }) {
  if (!location.cache && !location.filecoin) {
    return <StatusBadge tone="neutral">None</StatusBadge>
  }

  return (
    <div className="flex min-w-0 flex-wrap gap-1">
      {location.cache && <StatusBadge tone="info">Cache</StatusBadge>}
      {location.filecoin && <StatusBadge tone="success">Filecoin</StatusBadge>}
    </div>
  )
}

function ObjectStatusIcon({
  bucketName,
  versionID,
  state,
  status,
  uploadStatus,
  progress,
  compact = false,
}: {
  bucketName: string
  versionID: string
  state?: ObjectState
  status: ObjectStatus
  uploadStatus?: ObjectUploadStatus
  progress?: UploadTransferProgress
  compact?: boolean
}) {
  const [detailEnabled, setDetailEnabled] = useState(false)
  const visualStatus = objectVisualStatus(status, uploadStatus)
  const detail = useObjectStatusDetail(bucketName, versionID, visualStatus === 'warning' && detailEnabled)
  const progressPercent = uploadStatus === 'running' ? uploadProgressPercent(progress) : null
  const displayLabel = objectStateLabel(state, status, uploadStatus, progressPercent)
  const progressDetail =
    progressPercent === null || !progress
      ? null
      : `${formatBytes(progress.uploaded_bytes)} of ${formatBytes(progress.total_bytes)} uploaded`

  const loadDetail = () => {
    if (visualStatus === 'warning') setDetailEnabled(true)
  }

  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <button
          type="button"
          className={
            compact
              ? 'inline-flex size-5 shrink-0 items-center justify-center rounded-sm text-muted-foreground leading-none hover:bg-muted focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring'
              : 'inline-flex size-8 items-center justify-center rounded-md text-muted-foreground leading-none hover:bg-muted focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring'
          }
          aria-label={`${displayLabel} status`}
          onMouseEnter={loadDetail}
          onFocus={loadDetail}
          onClick={loadDetail}
        >
          {objectStatusIcon(visualStatus, compact, progressPercent)}
        </button>
      </TooltipTrigger>
      <TooltipContent side="top" className="max-w-sm items-start whitespace-normal text-left">
        <div className="flex max-w-xs flex-col gap-1">
          <span className="font-medium">{displayLabel}</span>
          {progressDetail && <span className="break-words opacity-90">{progressDetail}</span>}
          {visualStatus === 'warning' && (
            <span className="break-words opacity-90">
              {detail.isLoading
                ? 'Loading issue details'
                : detail.error
                  ? 'Failed to load issue details'
                  : detail.data?.message
                    ? `${failureStageLabel(detail.data.failed_at_state)}: ${detail.data.message}`
                    : 'No issue details recorded'}
            </span>
          )}
        </div>
      </TooltipContent>
    </Tooltip>
  )
}

function objectStatusIcon(status: ObjectStatus, compact = false, progressPercent: number | null = null) {
  const sizeClass = compact ? 'size-3.5' : 'size-4'
  if (progressPercent !== null) {
    const progressIconSizeClass = compact ? 'size-3' : 'size-3.5'
    return (
      <UploadProgressRing percent={progressPercent} compact={compact}>
        <Clock3 className={`${progressIconSizeClass} animate-spin text-status-info`} />
      </UploadProgressRing>
    )
  }
  switch (status) {
    case 'success':
      return <CheckCircle2 className={`${sizeClass} text-status-success`} />
    case 'warning':
      return <TriangleAlert className={`${sizeClass} text-status-warning`} />
    case 'unavailable':
      return <CircleSlash className={`${sizeClass} text-status-danger`} />
    default:
      return <Clock3 className={`${sizeClass} text-status-info`} />
  }
}

function objectVisualStatus(status: ObjectStatus, uploadStatus?: ObjectUploadStatus): ObjectStatus {
  switch (uploadStatus) {
    case 'failed':
    case 'rejected':
      return 'warning'
    default:
      return status
  }
}

function copyStatusTone(status: ObjectProvenanceCopy['status']): StatusTone {
  switch (status) {
    case 'committed':
      return 'success'
    case 'failed':
      return 'danger'
    case 'committing':
    case 'piece_ready':
      return 'info'
    case 'pending':
      return 'neutral'
  }
}

function failureStageLabel(state?: string) {
  switch (state) {
    case 'uploading':
      return 'Failed while uploading'
    case 'committing':
      return 'Failed while registering storage record'
    case 'replicating':
      return 'Failed while syncing replicas'
    case 'stored':
      return 'Failed after storage'
    case 'cached':
      return 'Failed while cached'
    case 'cache_evicted':
      return 'Failed after cache removal'
    default:
      return 'Failure'
  }
}

function ObjectBrowserPage() {
  const { name } = Route.useParams()
  const search = Route.useSearch()
  const navigate = useNavigate()
  const prefix = search.prefix ?? ''
  const marker = search.marker ?? ''
  const view = search.view ?? 'objects'
  const [detailsOpen, setDetailsOpen] = useState(false)
  const [changeOwnerOpen, setChangeOwnerOpen] = useState(false)
  const [deleteBucketOpen, setDeleteBucketOpen] = useState(false)
  const [uploadOpen, setUploadOpen] = useState(false)

  const bucket = useBucket(name)
  const objects = useBucketObjects(name, prefix, marker, 50, '/', view === 'objects')
  const deletedObjects = useDeletedBucketObjects(name, prefix, marker, 50, view === 'deleted')
  const qc = useQueryClient()

  const pathCrumbs = bucketPrefixCrumbs(prefix)

  const navigateToPrefix = (newPrefix: string) => {
    navigate({
      to: '/buckets/$name',
      params: { name },
      search: {
        prefix: newPrefix || undefined,
        marker: undefined,
        view: view === 'deleted' ? view : undefined,
      },
    })
  }

  const navigateToMarker = (newMarker: string) => {
    navigate({
      to: '/buckets/$name',
      params: { name },
      search: {
        prefix: prefix || undefined,
        marker: newMarker || undefined,
        view: view === 'deleted' ? view : undefined,
      },
    })
  }

  const navigateToView = (nextView: 'objects' | 'deleted') => {
    navigate({
      to: '/buckets/$name',
      params: { name },
      search: {
        prefix: prefix || undefined,
        marker: undefined,
        view: nextView === 'objects' ? undefined : nextView,
      },
    })
  }

  const handleRefresh = () => {
    qc.invalidateQueries({ queryKey: ['bucket', name] })
    qc.invalidateQueries({ queryKey: ['objects', name] })
    qc.invalidateQueries({ queryKey: ['deletedObjects', name] })
  }

  const handleUploadCompleted = () => {
    qc.invalidateQueries({ queryKey: ['bucket', name] })
    qc.invalidateQueries({ queryKey: ['objects', name] })
    qc.invalidateQueries({ queryKey: ['tasks'] })
    qc.invalidateQueries({ queryKey: ['taskStats'] })
    if (marker) {
      navigate({
        to: '/buckets/$name',
        params: { name },
        search: {
          prefix: prefix || undefined,
          marker: undefined,
          view: undefined,
        },
      })
    }
  }

  const canDelete = bucket.data?.status === 'active'
  const openChangeOwner = () => {
    setChangeOwnerOpen(true)
  }
  const openDeleteBucket = () => {
    setDeleteBucketOpen(true)
  }

  return (
    <div className="flex flex-col gap-4 p-6">
      <BucketBreadcrumb name={name} pathCrumbs={pathCrumbs} navigateToPrefix={navigateToPrefix} />

      <PageHeader
        title={name}
        meta={
          bucket.data && <StatusBadge tone={bucketStatusTone(bucket.data.status)}>{bucket.data.status}</StatusBadge>
        }
        actions={
          <>
            {view === 'objects' && (
              <Button
                variant="outline"
                size="sm"
                onClick={() => setUploadOpen(true)}
                disabled={bucket.data?.status !== 'active'}
              >
                <Upload data-icon="inline-start" /> Upload
              </Button>
            )}
            <Button variant="outline" size="sm" onClick={handleRefresh}>
              <RefreshCw data-icon="inline-start" /> Refresh
            </Button>
            {bucket.data && (
              <Button variant="outline" size="sm" onClick={() => setDetailsOpen(true)}>
                <Info data-icon="inline-start" /> Details
              </Button>
            )}
          </>
        }
      />

      {bucket.error && <div className="text-sm text-destructive">Failed to load bucket details</div>}

      <Tabs value={view} onValueChange={(value) => navigateToView(value === 'deleted' ? 'deleted' : 'objects')}>
        <TabsList className="max-w-full justify-start overflow-x-auto">
          <TabsTrigger value="objects">Objects</TabsTrigger>
          <TabsTrigger value="deleted">Trash</TabsTrigger>
        </TabsList>
      </Tabs>

      {view === 'objects' && objects.isLoading ? (
        <ObjectBrowserSkeleton />
      ) : view === 'objects' && objects.error ? (
        <div className="text-destructive">Failed to load objects</div>
      ) : view === 'objects' ? (
        <ObjectBrowserTable
          bucketName={name}
          prefix={prefix}
          folders={objects.data?.folders ?? []}
          files={objects.data?.objects ?? []}
          hasMore={objects.data?.has_more ?? false}
          nextMarker={objects.data?.next_marker}
          marker={marker}
          navigateToPrefix={navigateToPrefix}
          navigateToMarker={navigateToMarker}
        />
      ) : view === 'deleted' && deletedObjects.isLoading ? (
        <ObjectBrowserSkeleton />
      ) : view === 'deleted' && deletedObjects.error ? (
        <div className="text-destructive">Failed to load trash</div>
      ) : view === 'deleted' ? (
        <DeletedObjectsTable
          bucketName={name}
          prefix={prefix}
          objects={deletedObjects.data?.objects ?? []}
          hasMore={deletedObjects.data?.has_more ?? false}
          nextMarker={deletedObjects.data?.next_marker}
          marker={marker}
          navigateToPrefix={navigateToPrefix}
          navigateToMarker={navigateToMarker}
        />
      ) : (
        <ObjectBrowserSkeleton />
      )}
      <UploadObjectsDialog
        bucketName={name}
        prefix={prefix}
        open={uploadOpen}
        onOpenChange={setUploadOpen}
        onUploaded={handleUploadCompleted}
      />
      {bucket.data && (
        <>
          <BucketDetailsSheet
            bucket={bucket.data}
            open={detailsOpen}
            onOpenChange={setDetailsOpen}
            canDelete={canDelete}
            onChangeOwner={openChangeOwner}
            onDeleteBucket={openDeleteBucket}
          />
          <ChangeBucketOwnerDetailDialog
            bucketName={name}
            ownerAccessKey={bucket.data.owner_access_key}
            open={changeOwnerOpen}
            onOpenChange={setChangeOwnerOpen}
            showTrigger={false}
          />
          <DeleteBucketDetailDialog
            bucketName={name}
            objectCount={bucket.data.object_count}
            open={deleteBucketOpen}
            onOpenChange={setDeleteBucketOpen}
            showTrigger={false}
          />
        </>
      )}
    </div>
  )
}

type UploadDialogStatus = 'queued' | 'uploading' | 'success' | 'failed'

type UploadDialogItem = {
  id: string
  file: File
  key: string
  status: UploadDialogStatus
  loaded: number
  total: number
  percent: number
  retryable: boolean
  error?: string
}

function UploadObjectsDialog({
  bucketName,
  prefix,
  open,
  onOpenChange,
  onUploaded,
}: {
  bucketName: string
  prefix: string
  open: boolean
  onOpenChange: (open: boolean) => void
  onUploaded: () => void
}) {
  const [items, setItems] = useState<UploadDialogItem[]>([])
  const [uploading, setUploading] = useState(false)
  const fileInputRef = useRef<HTMLInputElement>(null)
  const queuedCount = items.filter((item) => item.status === 'queued').length
  const retryableFailedCount = items.filter((item) => item.status === 'failed' && item.retryable).length

  useEffect(() => {
    if (!open && !uploading) {
      setItems([])
    }
  }, [open, uploading])

  const handleOpenChange = (nextOpen: boolean) => {
    if (uploading) return
    onOpenChange(nextOpen)
  }

  const handleFileChange = (event: ChangeEvent<HTMLInputElement>) => {
    const files = Array.from(event.currentTarget.files ?? [])
    const keys = files.map((file) => objectUploadKey(prefix, file.name))
    const duplicateKeys = new Set(duplicateObjectUploadKeys(keys))
    setItems(files.map((file, index) => createUploadDialogItem(file, keys[index] ?? file.name, index, duplicateKeys)))
    event.currentTarget.value = ''
  }

  const uploadItems = async (status: UploadDialogStatus) => {
    const targets = items.filter(
      (item) =>
        item.status === status &&
        (status === 'queued' || item.retryable) &&
        validateFOCUploadSize(item.file.size) === null
    )
    if (targets.length === 0) return
    setUploading(true)
    let uploaded = false
    for (const item of targets) {
      setItems(itemsSetUploading(item.id, item.file.size))
      try {
        await api.uploadBucketObject(bucketName, {
          key: item.key,
          file: item.file,
          onProgress: (progress) => setItems(itemsSetProgress(item.id, progress)),
        })
        uploaded = true
        setItems(itemsSetSuccess(item.id, item.file.size))
      } catch (error) {
        setItems(itemsSetFailed(item.id, errorMessage(error)))
      }
    }
    setUploading(false)
    if (uploaded) {
      onUploaded()
    }
  }

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent className="w-[calc(100vw-2rem)] max-w-[calc(100vw-2rem)] sm:max-w-3xl">
        <DialogHeader>
          <DialogTitle>Upload objects</DialogTitle>
          <DialogDescription>Target: {prefix || '/'}</DialogDescription>
        </DialogHeader>

        <FieldGroup>
          <Field>
            <FieldLabel htmlFor="object-upload-files">Files</FieldLabel>
            <Input
              ref={fileInputRef}
              id="object-upload-files"
              type="file"
              multiple
              disabled={uploading}
              onChange={handleFileChange}
              className="sr-only"
            />
            <div className="flex flex-col gap-2 sm:flex-row sm:items-center">
              <Button
                type="button"
                variant="outline"
                onClick={() => fileInputRef.current?.click()}
                disabled={uploading}
              >
                <Upload data-icon="inline-start" /> Select files
              </Button>
              <span className="text-sm text-muted-foreground">{formatCountLabel(items.length, 'selected file')}</span>
            </div>
            <FieldDescription>
              Allowed size: {formatBytes(minFOCUploadSize)} to {formatBytes(maxFOCUploadSize)}
            </FieldDescription>
          </Field>
        </FieldGroup>

        {items.length === 0 ? (
          <div className="rounded-md border border-border">
            <Empty className="h-44 border-0">
              <EmptyHeader>
                <EmptyMedia variant="icon">
                  <Upload />
                </EmptyMedia>
                <EmptyTitle>No files selected</EmptyTitle>
              </EmptyHeader>
            </Empty>
          </div>
        ) : (
          <div className="rounded-md border border-border">
            <ScrollArea className="max-h-80 w-full">
              <Table className="min-w-[680px]">
                <TableHeader>
                  <TableRow className="bg-muted/50">
                    <TableHead className="w-[36%] px-4">Name</TableHead>
                    <TableHead className="w-[16%] px-4 text-right">Size</TableHead>
                    <TableHead className="w-[14%] px-4">Status</TableHead>
                    <TableHead className="w-[20%] px-4">Progress</TableHead>
                    <TableHead className="w-[14%] px-4">Message</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {items.map((item) => (
                    <TableRow key={item.id}>
                      <TableCell className="px-4">
                        <span className="block max-w-72 truncate" title={item.key}>
                          {item.file.name}
                        </span>
                      </TableCell>
                      <TableCell className="px-4 text-right">{formatBytes(item.file.size)}</TableCell>
                      <TableCell className="px-4">
                        <StatusBadge tone={uploadDialogStatusTone(item.status)}>
                          {uploadDialogStatusLabel(item.status)}
                        </StatusBadge>
                      </TableCell>
                      <TableCell className="px-4">
                        <UploadDialogProgress item={item} />
                      </TableCell>
                      <TableCell className="px-4">
                        <span className="block max-w-44 truncate text-muted-foreground" title={item.error}>
                          {item.error ?? '-'}
                        </span>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
              <ScrollBar orientation="horizontal" />
            </ScrollArea>
          </div>
        )}

        <DialogFooter>
          <Button variant="outline" onClick={() => handleOpenChange(false)} disabled={uploading}>
            Close
          </Button>
          {retryableFailedCount > 0 && (
            <Button variant="outline" onClick={() => uploadItems('failed')} disabled={uploading}>
              <RotateCcw data-icon="inline-start" /> Retry failed
            </Button>
          )}
          <Button onClick={() => uploadItems('queued')} disabled={uploading || queuedCount === 0}>
            {uploading ? (
              <Loader2 data-icon="inline-start" className="animate-spin" />
            ) : (
              <Upload data-icon="inline-start" />
            )}
            Upload
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

function createUploadDialogItem(
  file: File,
  key: string,
  index: number,
  duplicateKeys: ReadonlySet<string>
): UploadDialogItem {
  const sizeError = validateFOCUploadSize(file.size)
  const duplicateError = duplicateKeys.has(key) ? 'Duplicate object key in selected files' : null
  const error = sizeError ?? duplicateError ?? undefined
  return {
    id: `${file.name}-${file.size}-${file.lastModified}-${index}`,
    file,
    key,
    status: error ? 'failed' : 'queued',
    loaded: 0,
    total: file.size,
    percent: 0,
    retryable: false,
    error,
  }
}

function updateUploadDialogItems(updater: (item: UploadDialogItem) => UploadDialogItem) {
  return (items: UploadDialogItem[]) => items.map((item) => updater(item))
}

function itemsSetUploading(id: string, total: number) {
  return updateUploadDialogItems((item) =>
    item.id === id
      ? { ...item, status: 'uploading', loaded: 0, total, percent: 0, retryable: false, error: undefined }
      : item
  )
}

function itemsSetProgress(id: string, progress: ObjectUploadClientProgress) {
  return updateUploadDialogItems((item) =>
    item.id === id
      ? {
          ...item,
          loaded: progress.loaded,
          total: progress.total,
          percent: Math.max(0, Math.min(100, progress.percent)),
        }
      : item
  )
}

function itemsSetSuccess(id: string, total: number) {
  return updateUploadDialogItems((item) =>
    item.id === id
      ? { ...item, status: 'success', loaded: total, total, percent: 100, retryable: false, error: undefined }
      : item
  )
}

function itemsSetFailed(id: string, message: string, retryable = true) {
  return updateUploadDialogItems((item) =>
    item.id === id ? { ...item, status: 'failed', retryable, error: message } : item
  )
}

function UploadDialogProgress({ item }: { item: UploadDialogItem }) {
  const percent = item.status === 'success' ? 100 : item.percent
  return (
    <div className="inline-flex w-36 shrink-0 items-center gap-2" title={`${percent}% uploaded`}>
      <Progress value={percent} className="min-w-0 flex-1" />
      <span className="w-8 shrink-0 text-right font-mono text-[10px] text-muted-foreground">{percent}%</span>
    </div>
  )
}

function uploadDialogStatusLabel(status: UploadDialogStatus) {
  switch (status) {
    case 'queued':
      return 'Queued'
    case 'uploading':
      return 'Uploading'
    case 'success':
      return 'Uploaded'
    case 'failed':
      return 'Failed'
  }
}

function uploadDialogStatusTone(status: UploadDialogStatus): StatusTone {
  switch (status) {
    case 'success':
      return 'success'
    case 'uploading':
      return 'info'
    case 'failed':
      return 'danger'
    case 'queued':
      return 'neutral'
  }
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : 'Upload failed'
}

function BucketDetailsSheet({
  bucket,
  open,
  onOpenChange,
  canDelete,
  onChangeOwner,
  onDeleteBucket,
}: {
  bucket: NonNullable<ReturnType<typeof useBucket>['data']>
  open: boolean
  onOpenChange: (open: boolean) => void
  canDelete: boolean
  onChangeOwner: () => void
  onDeleteBucket: () => void
}) {
  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="min-w-0 !w-[min(64rem,calc(100vw-2rem))] !max-w-[calc(100vw-2rem)]">
        <SheetHeader>
          <SheetTitle>Bucket details</SheetTitle>
          <SheetDescription>
            <span className="block max-w-full truncate font-mono text-xs" title={bucket.name}>
              {bucket.name}
            </span>
          </SheetDescription>
        </SheetHeader>
        <ScrollArea className="min-h-0 min-w-0 flex-1">
          <div className="flex min-w-0 flex-col gap-6 px-4 pb-4">
            <BucketDetailsSection title="Overview">
              <BucketDetailsOverview bucket={bucket} />
            </BucketDetailsSection>
            <BucketDetailsSection title="Storage">
              <BucketStorageDataSets bucketName={bucket.name} dataSets={bucket.data_sets ?? []} />
            </BucketDetailsSection>
            <BucketDetailsSection title="Settings">
              <BucketDetailsSettings
                bucket={bucket}
                canDelete={canDelete}
                onChangeOwner={onChangeOwner}
                onDeleteBucket={onDeleteBucket}
              />
            </BucketDetailsSection>
          </div>
        </ScrollArea>
      </SheetContent>
    </Sheet>
  )
}

function BucketDetailsSection({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className="flex min-w-0 flex-col gap-3">
      <h3 className="text-sm font-medium">{title}</h3>
      {children}
    </section>
  )
}

function BucketDetailsOverview({ bucket }: { bucket: NonNullable<ReturnType<typeof useBucket>['data']> }) {
  return (
    <dl className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
      <BucketDetailField label="Objects" value={formatObjectCount(bucket.object_count)} />
      <BucketDetailField label="Total size" value={formatBytes(bucket.total_size_bytes)} />
      <BucketDetailField label="Replicas" value={bucketCopyPolicyLabel(bucket)} />
      <BucketDetailField label="Versioning" value={bucket.versioning_status} />
      <BucketDetailField label="Owner" value={ownerLabel(bucket.owner_access_key)} />
      <BucketDetailField label="Created" value={timeAgo(bucket.created_at)} title={bucket.created_at} />
      <BucketDetailField label="Updated" value={timeAgo(bucket.updated_at)} title={bucket.updated_at} />
      <div>
        <dt className="text-xs text-muted-foreground">Status</dt>
        <dd className="mt-1">
          <StatusBadge tone={bucketStatusTone(bucket.status)}>{bucket.status}</StatusBadge>
        </dd>
      </div>
    </dl>
  )
}

function BucketDetailField({ label, value, title }: { label: string; value: string; title?: string }) {
  return (
    <div>
      <dt className="text-xs text-muted-foreground">{label}</dt>
      <dd className="mt-1 truncate font-medium" title={title ?? value}>
        {value}
      </dd>
    </div>
  )
}

function BucketStorageDataSets({ bucketName, dataSets }: { bucketName: string; dataSets: StorageDataSetSummary[] }) {
  const refreshStorageHealth = useRefreshDataSetStorageHealth()
  const [refreshError, setRefreshError] = useState<string | null>(null)

  if (dataSets.length === 0) {
    return (
      <div className="rounded-md border border-border p-4">
        <p className="text-sm font-medium">No data sets</p>
        <p className="mt-1 text-sm text-muted-foreground">This bucket has no provider data sets yet.</p>
      </div>
    )
  }

  return (
    <div className="min-w-0 overflow-hidden rounded-md border border-border">
      <div className="flex items-center justify-between gap-2 border-b border-border bg-muted/50 px-4 py-2">
        <div className="text-sm font-medium">Data Sets</div>
        <Button
          type="button"
          variant="outline"
          size="sm"
          disabled={refreshStorageHealth.isPending}
          onClick={() => {
            setRefreshError(null)
            refreshStorageHealth.mutate(
              { bucket: bucketName },
              {
                onSuccess: () => setRefreshError(null),
                onError: (error) => setRefreshError(dataSetStorageHealthRefreshErrorMessage(error)),
              }
            )
          }}
        >
          <RefreshCw data-icon="inline-start" className={refreshStorageHealth.isPending ? 'animate-spin' : undefined} />
          Refresh
        </Button>
      </div>
      {refreshError && (
        <div className="flex items-start gap-2 border-b border-border px-4 py-2 text-sm text-destructive">
          <TriangleAlert className="mt-0.5 size-4 shrink-0" />
          <span>{refreshError}</span>
        </div>
      )}
      <ScrollArea className="w-full">
        <Table className="min-w-[760px]">
          <TableHeader>
            <TableRow>
              <TableHead className="px-4">Replica Slot</TableHead>
              <TableHead className="px-4">Provider</TableHead>
              <TableHead className="px-4">Data Set ID</TableHead>
              <TableHead className="px-4">Storage Health</TableHead>
              <TableHead className="px-4">Last Used</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {dataSets.map((dataSet) => (
              <TableRow key={dataSet.id}>
                <TableCell className="px-4 font-mono text-xs">{replicaLabel(dataSet.copy_index)}</TableCell>
                <TableCell className="px-4">
                  <ProviderIdentityCell providerID={dataSet.provider_id} identity={dataSet.provider_identity} />
                </TableCell>
                <TableCell className="px-4 font-mono text-xs text-muted-foreground">
                  <Link
                    to="/storage-topology"
                    search={storageDataSetTopologySearch(bucketName, dataSet)}
                    className="hover:text-foreground hover:underline"
                  >
                    {storageDataSetTopologyLabel(dataSet)}
                  </Link>
                </TableCell>
                <TableCell className="px-4">
                  <DataSetStorageHealthCell dataSet={dataSet} />
                </TableCell>
                <TableCell className="px-4 text-muted-foreground" title={dataSet.updated_at}>
                  {timeAgo(dataSet.updated_at)}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
        <ScrollBar orientation="horizontal" />
      </ScrollArea>
    </div>
  )
}

function storageDataSetTopologySearch(bucketName: string, dataSet: StorageDataSetSummary) {
  const scope = {
    bucket: bucketName,
    provider: dataSet.provider_id,
    local_data_set_id: dataSet.id,
    selection_provider: dataSet.provider_id,
    selection_bucket: bucketName,
  }
  return dataSet.data_set_id ? { ...scope, chain_data_set_id: dataSet.data_set_id } : scope
}

function storageDataSetTopologyLabel(dataSet: StorageDataSetSummary) {
  return dataSet.data_set_id ? `#${dataSet.data_set_id}` : 'No chain data set'
}

function DataSetStorageHealthCell({ dataSet }: { dataSet: StorageDataSetSummary }) {
  const storageHealth = dataSet.storage_health
  const detailParts = dataSetStorageHealthDetailParts(dataSet)
  const details = detailParts.join(' · ')
  if (!storageHealth) {
    return (
      <div className="flex min-w-0 flex-col gap-1">
        <StatusBadge tone="neutral">unknown</StatusBadge>
        <span className="text-xs text-muted-foreground" title={details}>
          {details}
        </span>
      </div>
    )
  }

  return (
    <div className="flex min-w-0 flex-col gap-1">
      <div className="flex min-w-0 flex-wrap items-center gap-2">
        <StatusBadge tone={storageHealthStatusTone(storageHealth.status)}>{storageHealth.status}</StatusBadge>
        {storageHealth.stale && (
          <StatusBadge tone="warning" className="shrink-0">
            stale
          </StatusBadge>
        )}
      </div>
      <div className="min-w-0 truncate text-xs text-muted-foreground" title={details}>
        {details}
      </div>
    </div>
  )
}

function storageHealthStatusTone(status: StorageHealthStatus): StatusTone {
  switch (status) {
    case 'available':
      return 'success'
    case 'degraded':
      return 'warning'
    case 'unavailable':
      return 'danger'
    case 'unknown':
      return 'neutral'
  }
}

function BucketDetailsSettings({
  bucket,
  canDelete,
  onChangeOwner,
  onDeleteBucket,
}: {
  bucket: NonNullable<ReturnType<typeof useBucket>['data']>
  canDelete: boolean
  onChangeOwner: () => void
  onDeleteBucket: () => void
}) {
  const updateCopyPolicy = useUpdateBucketCopyPolicy()
  const currentCopyPolicy = bucketCopyPolicyValue(bucket)
  const [copyPolicy, setCopyPolicy] = useState(currentCopyPolicy)
  const [copyPolicyError, setCopyPolicyError] = useState<string | null>(null)
  const [copyPolicyNotice, setCopyPolicyNotice] = useState<string | null>(null)

  useEffect(() => {
    setCopyPolicy(currentCopyPolicy)
    setCopyPolicyError(null)
  }, [currentCopyPolicy])

  useEffect(() => {
    if (bucket.name) setCopyPolicyNotice(null)
  }, [bucket.name])

  const copyPolicyChanged = copyPolicy !== currentCopyPolicy && copyPolicyNotice == null
  const handleCopyPolicyChange = (next: string) => {
    setCopyPolicy(next)
    setCopyPolicyError(null)
    setCopyPolicyNotice(null)
  }
  const saveCopyPolicy = () => {
    setCopyPolicyError(null)
    setCopyPolicyNotice(null)
    updateCopyPolicy.mutate(
      {
        name: bucket.name,
        defaultCopies: copyPolicy === inheritedCopyPolicyValue ? null : Number(copyPolicy),
      },
      {
        onSuccess: (savedBucket) => {
          setCopyPolicy(bucketCopyPolicyValue(savedBucket))
          setCopyPolicyNotice(bucketCopyPolicySavedMessage())
        },
        onError: (mutationError) => {
          setCopyPolicyError(mutationError instanceof Error ? mutationError.message : 'Failed to update copy policy')
        },
      }
    )
  }

  return (
    <div className="flex min-w-0 flex-col gap-4">
      <section className="rounded-md border border-border p-4">
        <h3 className="text-sm font-medium">Owner</h3>
        <p className="mt-1 truncate text-sm text-muted-foreground" title={ownerLabel(bucket.owner_access_key)}>
          {ownerLabel(bucket.owner_access_key)}
        </p>
        <Button variant="outline" size="sm" className="mt-3" onClick={onChangeOwner}>
          <UserRound data-icon="inline-start" />
          {bucket.owner_access_key ? 'Change owner' : 'Assign owner'}
        </Button>
      </section>
      <section className="rounded-md border border-border p-4">
        <h3 className="text-sm font-medium">Replicas</h3>
        <p className="mt-1 text-sm text-muted-foreground">{bucketCopyPolicyLabel(bucket)}</p>
        <p className="mt-1 text-xs text-muted-foreground">{bucketCopyPolicyEffectNote()}</p>
        <div className="mt-3 flex flex-col gap-2 sm:flex-row sm:items-center">
          <Select value={copyPolicy} onValueChange={handleCopyPolicyChange} disabled={updateCopyPolicy.isPending}>
            <SelectTrigger className="w-full sm:w-56">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectGroup>
                <SelectItem value={inheritedCopyPolicyValue}>{bucketCopyPolicyInheritOptionLabel(bucket)}</SelectItem>
                {copyPolicyOptions.map((copies) => (
                  <SelectItem key={copies} value={copies.toString()}>
                    {copies} {copies === 1 ? 'copy' : 'copies'}
                  </SelectItem>
                ))}
              </SelectGroup>
            </SelectContent>
          </Select>
          <Button
            variant="outline"
            size="sm"
            onClick={saveCopyPolicy}
            disabled={!copyPolicyChanged || updateCopyPolicy.isPending}
          >
            {updateCopyPolicy.isPending ? (
              <Loader2 data-icon="inline-start" className="animate-spin" />
            ) : (
              <CheckCircle2 data-icon="inline-start" />
            )}
            Save
          </Button>
        </div>
        {copyPolicyNotice && (
          <p className="mt-2 inline-flex items-center gap-1.5 text-sm text-emerald-500" role="status">
            <CheckCircle2 className="size-4" />
            {copyPolicyNotice}
          </p>
        )}
        {copyPolicyError && <p className="mt-2 text-sm text-destructive">{copyPolicyError}</p>}
      </section>
      <section className="rounded-md border border-destructive/30 p-4">
        <h3 className="text-sm font-medium text-destructive">Delete bucket</h3>
        <p className="mt-1 text-sm text-muted-foreground">
          {canDelete
            ? 'Delete this bucket and its cached data after confirmation.'
            : 'Deletion is unavailable while the bucket is not active.'}
        </p>
        <Button variant="destructive" size="sm" className="mt-3" onClick={onDeleteBucket} disabled={!canDelete}>
          <Trash2 data-icon="inline-start" />
          Delete bucket
        </Button>
      </section>
    </div>
  )
}

function formatObjectCount(count: number) {
  return `${formatNumber(count)} ${count === 1 ? 'object' : 'objects'}`
}

function ObjectBrowserTable({
  bucketName,
  prefix,
  folders,
  files,
  hasMore,
  nextMarker,
  marker,
  navigateToPrefix,
  navigateToMarker,
}: {
  bucketName: string
  prefix: string
  folders: ObjectFolderItem[]
  files: ObjectItem[]
  hasMore: boolean
  nextMarker?: string
  marker: string
  navigateToPrefix: (prefix: string) => void
  navigateToMarker: (marker: string) => void
}) {
  const empty = folders.length === 0 && files.length === 0

  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
        <ObjectPathBreadcrumb prefix={prefix} navigateToPrefix={navigateToPrefix} />
        <p className="text-sm text-muted-foreground">{formatBrowserCount(folders.length, files.length)}</p>
      </div>

      {empty ? (
        <div className="rounded-md border border-border">
          <Empty className="h-64 border-0">
            <EmptyHeader>
              <EmptyMedia variant="icon">
                <Folder />
              </EmptyMedia>
              <EmptyTitle>No objects found</EmptyTitle>
              <EmptyDescription>This path has no visible objects.</EmptyDescription>
            </EmptyHeader>
          </Empty>
        </div>
      ) : (
        <div className="rounded-md border border-border">
          <ScrollArea className="w-full">
            <Table className="min-w-[780px]">
              <TableHeader>
                <TableRow className="bg-muted/50">
                  <TableHead className="w-[35%] px-4">Name</TableHead>
                  <TableHead className="w-[10%] px-4 text-right">Size</TableHead>
                  <TableHead className="w-[18%] px-4">Location</TableHead>
                  <TableHead className="w-[18%] px-4">Type</TableHead>
                  <TableHead className="w-[12%] px-4">Updated</TableHead>
                  <TableHead className="w-[7%] px-4 text-right">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {folders.map((folder) => (
                  <TableRow key={folder.prefix}>
                    <TableCell className="px-4">
                      <Button
                        type="button"
                        variant="ghost"
                        size="sm"
                        className="h-auto max-w-full justify-start gap-1.5 p-0 font-normal text-foreground hover:bg-transparent hover:text-foreground has-data-[icon=inline-start]:pl-0"
                        onClick={() => navigateToPrefix(folder.prefix)}
                      >
                        <Folder data-icon="inline-start" className="text-status-info" />
                        <span className="truncate font-medium">{folder.name}</span>
                      </Button>
                    </TableCell>
                    <TableCell className="px-4 text-right text-muted-foreground">-</TableCell>
                    <TableCell className="px-4 text-muted-foreground">-</TableCell>
                    <TableCell className="px-4">
                      <StatusBadge tone="info">Folder</StatusBadge>
                    </TableCell>
                    <TableCell className="px-4 text-muted-foreground">-</TableCell>
                    <TableCell className="px-4 text-right">
                      <FolderActions folder={folder} onOpen={() => navigateToPrefix(folder.prefix)} />
                    </TableCell>
                  </TableRow>
                ))}
                {files.map((object) => (
                  <TableRow key={object.id}>
                    <TableCell className="px-4">
                      <div className="flex min-w-0 items-center gap-1.5">
                        <FileIcon className="size-4 shrink-0 text-muted-foreground" />
                        <span className="min-w-0 truncate" title={object.key}>
                          {objectDisplayName(object.key, prefix)}
                        </span>
                        <ObjectStatusIcon
                          bucketName={bucketName}
                          versionID={object.current_version_id}
                          state={object.state}
                          status={object.status}
                          uploadStatus={object.upload_status}
                          progress={object.progress}
                          compact
                        />
                      </div>
                    </TableCell>
                    <TableCell className="px-4 text-right">{formatBytes(object.size)}</TableCell>
                    <TableCell className="px-4">
                      <LocationBadges location={object.location} />
                    </TableCell>
                    <TableCell className="px-4 text-muted-foreground">
                      <span className="block max-w-48 truncate" title={object.content_type}>
                        {object.content_type}
                      </span>
                    </TableCell>
                    <TableCell className="px-4 text-muted-foreground" title={object.updated_at}>
                      {timeAgo(object.updated_at)}
                    </TableCell>
                    <TableCell className="px-4 text-right">
                      <ObjectActions bucketName={bucketName} object={object} />
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
            <ScrollBar orientation="horizontal" />
          </ScrollArea>
        </div>
      )}

      <div className="flex justify-between">
        {marker ? (
          <Button variant="outline" size="sm" onClick={() => navigateToMarker('')}>
            First page
          </Button>
        ) : (
          <span />
        )}
        {hasMore && nextMarker && (
          <Button variant="outline" size="sm" onClick={() => navigateToMarker(nextMarker)}>
            Next page
          </Button>
        )}
      </div>
    </div>
  )
}

function DeletedObjectsTable({
  bucketName,
  prefix,
  objects,
  hasMore,
  nextMarker,
  marker,
  navigateToPrefix,
  navigateToMarker,
}: {
  bucketName: string
  prefix: string
  objects: DeletedObjectItem[]
  hasMore: boolean
  nextMarker?: string
  marker: string
  navigateToPrefix: (prefix: string) => void
  navigateToMarker: (marker: string) => void
}) {
  const empty = objects.length === 0

  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
        <ObjectPathBreadcrumb prefix={prefix} navigateToPrefix={navigateToPrefix} />
        <p className="text-sm text-muted-foreground">{formatCountLabel(objects.length, 'trash item')}</p>
      </div>

      {empty ? (
        <div className="rounded-md border border-border">
          <Empty className="h-64 border-0">
            <EmptyHeader>
              <EmptyMedia variant="icon">
                <Trash2 />
              </EmptyMedia>
              <EmptyTitle>Trash is empty</EmptyTitle>
              <EmptyDescription>This path has no objects that can be restored.</EmptyDescription>
            </EmptyHeader>
          </Empty>
        </div>
      ) : (
        <div className="rounded-md border border-border">
          <ScrollArea className="w-full">
            <Table className="min-w-[860px]">
              <TableHeader>
                <TableRow className="bg-muted/50">
                  <TableHead className="w-[35%] px-4">Name</TableHead>
                  <TableHead className="w-[22%] px-4">Restore target</TableHead>
                  <TableHead className="w-[10%] px-4 text-right">Size</TableHead>
                  <TableHead className="w-[18%] px-4">Type</TableHead>
                  <TableHead className="w-[10%] px-4">Moved to Trash</TableHead>
                  <TableHead className="w-[5%] px-4 text-right">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {objects.map((object) => (
                  <TableRow key={object.delete_marker_version_id}>
                    <TableCell className="px-4">
                      <div className="flex min-w-0 items-center gap-1.5">
                        <FileIcon className="size-4 shrink-0 text-muted-foreground" />
                        <span className="min-w-0 truncate" title={object.key}>
                          {objectDisplayName(object.key, prefix)}
                        </span>
                      </div>
                    </TableCell>
                    <TableCell className="overflow-hidden px-4">
                      <span
                        className="block truncate font-mono text-xs text-muted-foreground"
                        title={object.restore_version_id}
                      >
                        {object.restore_version_id}
                      </span>
                    </TableCell>
                    <TableCell className="px-4 text-right">{formatBytes(object.restore_size)}</TableCell>
                    <TableCell className="px-4 text-muted-foreground">
                      <span className="block max-w-48 truncate" title={object.restore_content_type}>
                        {object.restore_content_type}
                      </span>
                    </TableCell>
                    <TableCell className="px-4 text-muted-foreground" title={object.deleted_at}>
                      {timeAgo(object.deleted_at)}
                    </TableCell>
                    <TableCell className="px-4 text-right">
                      <DeletedObjectActions bucketName={bucketName} object={object} />
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
            <ScrollBar orientation="horizontal" />
          </ScrollArea>
        </div>
      )}

      <div className="flex justify-between">
        {marker ? (
          <Button variant="outline" size="sm" onClick={() => navigateToMarker('')}>
            First page
          </Button>
        ) : (
          <span />
        )}
        {hasMore && nextMarker && (
          <Button variant="outline" size="sm" onClick={() => navigateToMarker(nextMarker)}>
            Next page
          </Button>
        )}
      </div>
    </div>
  )
}

function DeletedObjectActions({ bucketName, object }: { bucketName: string; object: DeletedObjectItem }) {
  const [restoreOpen, setRestoreOpen] = useState(false)
  const [versionsOpen, setVersionsOpen] = useState(false)
  const [permanentDeleteOpen, setPermanentDeleteOpen] = useState(false)

  return (
    <>
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button variant="ghost" size="icon-sm" aria-label={`Actions for ${object.key}`} title="Actions">
            <MoreHorizontal />
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end" className="w-52">
          <DropdownMenuGroup>
            <DropdownMenuItem onSelect={() => setRestoreOpen(true)}>
              <RotateCcw data-icon="inline-start" />
              Restore
            </DropdownMenuItem>
            <DropdownMenuItem onSelect={() => setVersionsOpen(true)}>
              <History data-icon="inline-start" />
              Versions
            </DropdownMenuItem>
            <DropdownMenuItem variant="destructive" onSelect={() => setPermanentDeleteOpen(true)}>
              <Trash2 data-icon="inline-start" />
              Permanently delete object
            </DropdownMenuItem>
          </DropdownMenuGroup>
        </DropdownMenuContent>
      </DropdownMenu>
      <RestoreDeletedObjectDialog
        bucketName={bucketName}
        object={object}
        open={restoreOpen}
        onOpenChange={setRestoreOpen}
      />
      <ObjectVersionsDialog
        bucketName={bucketName}
        objectKey={object.key}
        open={versionsOpen}
        onOpenChange={setVersionsOpen}
      />
      <PermanentDeleteDeletedObjectDialog
        bucketName={bucketName}
        object={object}
        open={permanentDeleteOpen}
        onOpenChange={setPermanentDeleteOpen}
      />
    </>
  )
}

function PermanentDeleteDeletedObjectDialog({
  bucketName,
  object,
  open,
  onOpenChange,
}: {
  bucketName: string
  object: DeletedObjectItem
  open: boolean
  onOpenChange: (open: boolean) => void
}) {
  const permanentDelete = usePermanentDeleteDeletedBucketObject()

  const handleOpenChange = (next: boolean) => {
    onOpenChange(next)
    if (!next) permanentDelete.reset()
  }

  const handlePermanentDelete = () => {
    permanentDelete.mutate(
      { name: bucketName, key: object.key, deleteMarkerVersionID: object.delete_marker_version_id },
      {
        onSuccess: () => {
          handleOpenChange(false)
        },
      }
    )
  }

  return (
    <DangerActionAlertDialog
      open={open}
      onOpenChange={handleOpenChange}
      title="Permanently delete object"
      description="This permanently deletes this object from Trash and every version kept for restore. You cannot restore it afterward."
      confirmLabel="Permanently delete"
      pending={permanentDelete.isPending}
      error={permanentDelete.error?.message}
      onConfirm={handlePermanentDelete}
    >
      <ReviewDetails
        rows={[
          { id: 'key', label: 'Object', value: object.key },
          { id: 'size', label: 'Latest size', value: formatBytes(object.restore_size) },
        ]}
      />
    </DangerActionAlertDialog>
  )
}

function RestoreDeletedObjectDialog({
  bucketName,
  object,
  open,
  onOpenChange,
}: {
  bucketName: string
  object: DeletedObjectItem
  open: boolean
  onOpenChange: (open: boolean) => void
}) {
  const restoreObject = useRestoreBucketObject()

  const handleOpenChange = (next: boolean) => {
    onOpenChange(next)
    if (!next) restoreObject.reset()
  }

  const handleRestore = () => {
    restoreObject.mutate(
      { name: bucketName, key: object.key, deleteMarkerVersionID: object.delete_marker_version_id },
      {
        onSuccess: () => {
          handleOpenChange(false)
        },
      }
    )
  }

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Restore object</DialogTitle>
          <DialogDescription>Restore the latest data version and show the object again.</DialogDescription>
        </DialogHeader>
        <ReviewDetails
          rows={[
            { id: 'key', label: 'Object', value: object.key },
            { id: 'marker', label: 'Deletion record', value: object.delete_marker_version_id },
            { id: 'target', label: 'Restore version', value: object.restore_version_id },
            { id: 'size', label: 'Size', value: formatBytes(object.restore_size) },
          ]}
        />
        {restoreObject.error && <p className="text-sm text-destructive">{restoreObject.error.message}</p>}
        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            onClick={() => handleOpenChange(false)}
            disabled={restoreObject.isPending}
          >
            Cancel
          </Button>
          <Button type="button" onClick={handleRestore} disabled={restoreObject.isPending}>
            {restoreObject.isPending && <Loader2 data-icon="inline-start" className="animate-spin" />}
            Restore
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

function FolderActions({ folder, onOpen }: { folder: ObjectFolderItem; onOpen: () => void }) {
  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="ghost" size="icon-sm" aria-label={`Actions for ${folder.name}`} title="Actions">
          <MoreHorizontal />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="w-36">
        <DropdownMenuGroup>
          <DropdownMenuItem onSelect={onOpen}>
            <Folder data-icon="inline-start" />
            Open
          </DropdownMenuItem>
        </DropdownMenuGroup>
      </DropdownMenuContent>
    </DropdownMenu>
  )
}

function ObjectActions({ bucketName, object }: { bucketName: string; object: ObjectItem }) {
  const [versionsOpen, setVersionsOpen] = useState(false)
  const [provenanceOpen, setProvenanceOpen] = useState(false)
  const [deleteOpen, setDeleteOpen] = useState(false)
  const deleteObject = useDeleteBucketObject()

  const handleDelete = () => {
    deleteObject.mutate(
      { name: bucketName, key: object.key },
      {
        onSuccess: () => {
          setDeleteOpen(false)
          deleteObject.reset()
        },
      }
    )
  }

  return (
    <>
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button variant="ghost" size="icon-sm" aria-label={`Actions for ${object.key}`} title="Actions">
            <MoreHorizontal />
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end" className="w-40">
          <DropdownMenuGroup>
            <DropdownMenuItem asChild>
              <a href={api.getObjectDownloadUrl(bucketName, object.key)} aria-label={`Download ${object.key}`}>
                <Download data-icon="inline-start" />
                Download
              </a>
            </DropdownMenuItem>
            <DropdownMenuItem onSelect={() => setVersionsOpen(true)}>
              <History data-icon="inline-start" />
              Versions
            </DropdownMenuItem>
            <DropdownMenuItem onSelect={() => setProvenanceOpen(true)}>
              <Fingerprint data-icon="inline-start" />
              Provenance
            </DropdownMenuItem>
            <DropdownMenuItem variant="destructive" onSelect={() => setDeleteOpen(true)}>
              <Trash2 data-icon="inline-start" />
              Delete
            </DropdownMenuItem>
          </DropdownMenuGroup>
        </DropdownMenuContent>
      </DropdownMenu>
      <ObjectVersionsDialog
        bucketName={bucketName}
        objectKey={object.key}
        open={versionsOpen}
        onOpenChange={setVersionsOpen}
      />
      <ObjectProvenanceDialog
        bucketName={bucketName}
        objectKey={object.key}
        versionID={object.current_version_id}
        open={provenanceOpen}
        onOpenChange={setProvenanceOpen}
      />
      <DangerActionAlertDialog
        open={deleteOpen}
        onOpenChange={(next) => {
          setDeleteOpen(next)
          if (!next) deleteObject.reset()
        }}
        title="Delete object"
        description="This moves the object to Trash. Its data is kept so you can restore it later."
        confirmLabel="Delete object"
        pending={deleteObject.isPending}
        error={deleteObject.error?.message}
        onConfirm={handleDelete}
      />
    </>
  )
}

function ObjectBrowserSkeleton() {
  return (
    <div className="rounded-md border border-border p-4">
      <div className="flex flex-col gap-3">
        {objectBrowserSkeletonRows.map((row) => (
          <div key={row} className="grid grid-cols-[1fr_6rem_9rem_10rem_8rem_3rem] items-center gap-4">
            <Skeleton className="h-6 w-full" />
            <Skeleton className="h-6 w-full" />
            <Skeleton className="h-6 w-full" />
            <Skeleton className="h-6 w-full" />
            <Skeleton className="h-6 w-full" />
            <Skeleton className="h-6 w-full" />
          </div>
        ))}
      </div>
    </div>
  )
}

function ObjectPathBreadcrumb({
  prefix,
  navigateToPrefix,
}: {
  prefix: string
  navigateToPrefix: (prefix: string) => void
}) {
  const pathCrumbs = bucketPrefixCrumbs(prefix)

  return (
    <Breadcrumb className="min-w-0">
      <BreadcrumbList className="text-xs">
        <BreadcrumbItem>
          {pathCrumbs.length > 0 ? (
            <BreadcrumbLink asChild>
              <Button
                type="button"
                variant="link"
                className="h-auto p-0 text-xs font-normal"
                onClick={() => navigateToPrefix('')}
              >
                /
              </Button>
            </BreadcrumbLink>
          ) : (
            <BreadcrumbCurrentPage className="text-muted-foreground">/</BreadcrumbCurrentPage>
          )}
        </BreadcrumbItem>
        {pathCrumbs.map((crumb, index) => {
          const isLast = index === pathCrumbs.length - 1

          return (
            <Fragment key={crumb.prefix}>
              <BreadcrumbSeparator />
              <BreadcrumbItem>
                {isLast ? (
                  <BreadcrumbCurrentPage>{crumb.label}</BreadcrumbCurrentPage>
                ) : (
                  <BreadcrumbLink asChild>
                    <Button
                      type="button"
                      variant="link"
                      className="h-auto p-0 text-xs font-normal"
                      onClick={() => navigateToPrefix(crumb.prefix)}
                    >
                      {crumb.label}
                    </Button>
                  </BreadcrumbLink>
                )}
              </BreadcrumbItem>
            </Fragment>
          )
        })}
      </BreadcrumbList>
    </Breadcrumb>
  )
}

function formatBrowserCount(folderCount: number, fileCount: number) {
  return `${formatCountLabel(folderCount, 'folder')}, ${formatCountLabel(fileCount, 'file')}`
}

function formatCountLabel(count: number, noun: string) {
  return `${formatNumber(count)} ${noun}${count === 1 ? '' : 's'}`
}

function objectDisplayName(key: string, prefix: string) {
  const name = key.startsWith(prefix) ? key.slice(prefix.length) : key
  return name || key
}

function BucketBreadcrumb({
  name,
  pathCrumbs,
  navigateToPrefix,
}: {
  name: string
  pathCrumbs: BucketPrefixCrumb[]
  navigateToPrefix: (prefix: string) => void
}) {
  return (
    <Breadcrumb>
      <BreadcrumbList>
        <BreadcrumbItem>
          <BreadcrumbLink asChild>
            <Link to="/buckets">Buckets</Link>
          </BreadcrumbLink>
        </BreadcrumbItem>
        <BreadcrumbSeparator />
        <BreadcrumbItem>
          {pathCrumbs.length > 0 ? (
            <BreadcrumbLink asChild>
              <Button
                type="button"
                variant="link"
                className="h-auto p-0 text-sm font-normal"
                onClick={() => navigateToPrefix('')}
              >
                {name}
              </Button>
            </BreadcrumbLink>
          ) : (
            <BreadcrumbCurrentPage>{name}</BreadcrumbCurrentPage>
          )}
        </BreadcrumbItem>
        {pathCrumbs.map((crumb, index) => {
          const isLast = index === pathCrumbs.length - 1

          return (
            <Fragment key={crumb.prefix}>
              <BreadcrumbSeparator />
              <BreadcrumbItem>
                {isLast ? (
                  <BreadcrumbCurrentPage>{crumb.label}</BreadcrumbCurrentPage>
                ) : (
                  <BreadcrumbLink asChild>
                    <Button
                      type="button"
                      variant="link"
                      className="h-auto p-0 text-sm font-normal"
                      onClick={() => navigateToPrefix(crumb.prefix)}
                    >
                      {crumb.label}
                    </Button>
                  </BreadcrumbLink>
                )}
              </BreadcrumbItem>
            </Fragment>
          )
        })}
      </BreadcrumbList>
    </Breadcrumb>
  )
}
