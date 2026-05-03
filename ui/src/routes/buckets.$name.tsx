import { useQueryClient } from '@tanstack/react-query'
import { createFileRoute, Link, useNavigate } from '@tanstack/react-router'
import {
  CheckCircle2,
  CircleSlash,
  Clock3,
  Download,
  FileIcon,
  Folder,
  History,
  Loader2,
  MoreHorizontal,
  RefreshCw,
  Trash2,
  TriangleAlert,
  UserRound,
} from 'lucide-react'
import { Fragment, useEffect, useState } from 'react'
import {
  api,
  type ObjectFolderItem,
  type ObjectItem,
  type ObjectState,
  type ObjectStatus,
  type ObjectUploadStatus,
} from '@/api/client'
import { BreadcrumbCurrentPage } from '@/components/app/BreadcrumbCurrentPage'
import { BucketOwnerSelect } from '@/components/app/BucketOwnerSelect'
import { PageHeader } from '@/components/app/PageHeader'
import { ReviewDetails } from '@/components/app/ReviewDetails'
import { bucketStatusTone, StatusBadge } from '@/components/app/StatusBadge'
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
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area'
import { Skeleton } from '@/components/ui/skeleton'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import {
  useBucket,
  useBucketObjects,
  useBucketObjectVersions,
  useDeleteBucket,
  useObjectStatusDetail,
  useS3Users,
  useUpdateBucketOwner,
} from '@/hooks/queries'
import { ownerLabel } from '@/lib/s3-owner'
import { type BucketPrefixCrumb, bucketPrefixCrumbs } from '@/lib/s3-prefix'
import { formatBytes, formatNumber, timeAgo } from '@/lib/utils'

type ObjectBrowserSearch = {
  prefix?: string
  marker?: string
}

const objectBrowserSkeletonRows = ['row-1', 'row-2', 'row-3', 'row-4', 'row-5', 'row-6', 'row-7', 'row-8']

export const Route = createFileRoute('/buckets/$name')({
  validateSearch: (search: Record<string, unknown>): ObjectBrowserSearch => ({
    prefix: normalizePrefixSearch(search.prefix),
    marker: normalizeSearchString(search.marker),
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
          <Trash2 data-icon="inline-start" />
          Delete
        </Button>
      </DialogTrigger>
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
}: {
  bucketName: string
  ownerAccessKey: string | null
}) {
  const [open, setOpen] = useState(false)
  const [selectedOwner, setSelectedOwner] = useState(ownerAccessKey ?? '')
  const [reviewing, setReviewing] = useState(false)
  const { data: users = [], isLoading: usersLoading, error: usersError } = useS3Users()
  const updateOwner = useUpdateBucketOwner()

  useEffect(() => {
    if (!open) {
      setSelectedOwner(ownerAccessKey ?? '')
      setReviewing(false)
    }
  }, [ownerAccessKey, open])

  const reset = () => {
    setSelectedOwner(ownerAccessKey ?? '')
    setReviewing(false)
    updateOwner.reset()
  }

  const handleOpenChange = (next: boolean) => {
    reset()
    setOpen(next)
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
          setOpen(false)
          reset()
        },
      }
    )
  }

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogTrigger asChild>
        <Button variant="outline" size="sm">
          <UserRound data-icon="inline-start" />
          {ownerAccessKey ? 'Change owner' : 'Assign owner'}
        </Button>
      </DialogTrigger>
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
  object,
  open,
  onOpenChange,
}: {
  bucketName: string
  object: ObjectItem
  open: boolean
  onOpenChange: (open: boolean) => void
}) {
  const [versionMarker, setVersionMarker] = useState('')
  const versions = useBucketObjectVersions(bucketName, object.key, versionMarker, 50, open)

  useEffect(() => {
    if (open) setVersionMarker('')
  }, [open])

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="w-[calc(100vw-2rem)] max-w-[calc(100vw-2rem)] sm:max-w-6xl lg:p-6">
        <DialogHeader>
          <DialogTitle>Object versions</DialogTitle>
          <DialogDescription className="pr-8">
            <span className="block max-w-full truncate font-mono text-xs" title={object.key}>
              {object.key}
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
                  <TableHead className="px-2 text-right">Download</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {versions.data?.versions.map((version) => (
                  <TableRow key={version.version_id}>
                    <TableCell className="overflow-hidden px-2" title={version.version_id}>
                      <div className="flex min-w-0 items-center gap-2">
                        <span className="min-w-0 truncate font-mono text-xs">{version.version_id}</span>
                        <ObjectStatusIcon
                          bucketName={bucketName}
                          versionID={version.version_id}
                          state={version.state}
                          status={version.status}
                          uploadStatus={version.upload_status}
                          compact
                        />
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
                      <Button variant="ghost" size="icon-sm" asChild>
                        <a
                          href={api.getObjectDownloadUrl(bucketName, object.key, version.version_id)}
                          aria-label={`Download ${object.key} version ${version.version_id}`}
                          title="Download"
                        >
                          <Download />
                        </a>
                      </Button>
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
  compact = false,
}: {
  bucketName: string
  versionID: string
  state?: ObjectState
  status: ObjectStatus
  uploadStatus?: ObjectUploadStatus
  compact?: boolean
}) {
  const [detailEnabled, setDetailEnabled] = useState(false)
  const visualStatus = objectVisualStatus(status, uploadStatus)
  const detail = useObjectStatusDetail(bucketName, versionID, visualStatus === 'warning' && detailEnabled)
  const label = objectStateLabel(state, status, uploadStatus)

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
              ? 'inline-flex size-5 shrink-0 items-center justify-center rounded-sm text-muted-foreground hover:bg-muted focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring'
              : 'inline-flex size-8 items-center justify-center rounded-md text-muted-foreground hover:bg-muted focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring'
          }
          aria-label={`${label} status`}
          onMouseEnter={loadDetail}
          onFocus={loadDetail}
          onClick={loadDetail}
        >
          {objectStatusIcon(visualStatus, compact)}
        </button>
      </TooltipTrigger>
      <TooltipContent side="top" className="max-w-sm items-start whitespace-normal text-left">
        <div className="flex max-w-xs flex-col gap-1">
          <span className="font-medium">{label}</span>
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

function objectStatusIcon(status: ObjectStatus, compact = false) {
  const sizeClass = compact ? 'size-3.5' : 'size-4'
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

function objectStatusLabel(status: ObjectStatus) {
  switch (status) {
    case 'success':
      return 'Success'
    case 'warning':
      return 'Warning'
    case 'unavailable':
      return 'Unavailable'
    case 'syncing':
      return 'Syncing'
    case 'uploading':
      return 'Uploading'
  }
}

function objectVisualStatus(status: ObjectStatus, uploadStatus?: ObjectUploadStatus): ObjectStatus {
  switch (uploadStatus) {
    case 'partial':
    case 'failed':
    case 'rejected':
      return 'warning'
    default:
      return status
  }
}

function uploadStatusLabel(uploadStatus: ObjectUploadStatus) {
  switch (uploadStatus) {
    case 'running':
      return 'Upload running'
    case 'stored_on_primary':
      return 'Stored on primary'
    case 'primary_committed':
      return 'Primary committed'
    case 'partial':
      return 'Partial replication'
    case 'all_copies_committed':
      return 'All copies committed'
    case 'failed':
      return 'Upload failed'
    case 'rejected':
      return 'Upload rejected'
    case 'superseded':
      return 'Upload superseded'
  }
}

function objectStateLabel(state: ObjectState | undefined, status: ObjectStatus, uploadStatus?: ObjectUploadStatus) {
  if (uploadStatus) return uploadStatusLabel(uploadStatus)
  switch (state) {
    case 'cached':
      return 'Cached'
    case 'uploading':
      return 'Uploading'
    case 'committing':
      return 'Committing primary copy'
    case 'replicating':
      return 'Replicating'
    case 'stored':
      return 'Stored'
    case 'cache_evicted':
      return 'Cache evicted'
    case 'failed':
      return 'Warning'
    default:
      return objectStatusLabel(status)
  }
}

function failureStageLabel(state?: string) {
  switch (state) {
    case 'uploading':
      return 'Failed while uploading'
    case 'committing':
      return 'Failed while committing primary copy'
    case 'replicating':
      return 'Failed while syncing copies'
    case 'stored':
      return 'Failed after storage'
    case 'cached':
      return 'Failed while cached'
    case 'cache_evicted':
      return 'Failed after cache eviction'
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

  const bucket = useBucket(name)
  const objects = useBucketObjects(name, prefix, marker)
  const qc = useQueryClient()

  const pathCrumbs = bucketPrefixCrumbs(prefix)

  const navigateToPrefix = (newPrefix: string) => {
    navigate({
      to: '/buckets/$name',
      params: { name },
      search: {
        prefix: newPrefix || undefined,
        marker: undefined,
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
      },
    })
  }

  const handleRefresh = () => {
    qc.invalidateQueries({ queryKey: ['bucket', name] })
    qc.invalidateQueries({ queryKey: ['objects', name] })
  }

  const canDelete = bucket.data?.status === 'active'

  return (
    <div className="flex flex-col gap-4 p-6">
      <BucketBreadcrumb name={name} pathCrumbs={pathCrumbs} navigateToPrefix={navigateToPrefix} />

      <PageHeader
        title={name}
        description={bucket.data ? <BucketMetaLine bucket={bucket.data} /> : undefined}
        meta={
          bucket.data && <StatusBadge tone={bucketStatusTone(bucket.data.status)}>{bucket.data.status}</StatusBadge>
        }
        actions={
          <>
            {bucket.data && (
              <ChangeBucketOwnerDetailDialog bucketName={name} ownerAccessKey={bucket.data.owner_access_key} />
            )}
            {canDelete ? (
              <DeleteBucketDetailDialog bucketName={name} objectCount={bucket.data?.object_count ?? 0} />
            ) : (
              <Button variant="destructive" size="sm" disabled>
                <Trash2 data-icon="inline-start" />
                Delete
              </Button>
            )}
            <Button variant="outline" size="sm" onClick={handleRefresh}>
              <RefreshCw data-icon="inline-start" /> Refresh
            </Button>
          </>
        }
      />

      {bucket.error && <div className="text-sm text-destructive">Failed to load bucket details</div>}

      {objects.isLoading ? (
        <ObjectBrowserSkeleton />
      ) : objects.error ? (
        <div className="text-destructive">Failed to load objects</div>
      ) : (
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
      )}
    </div>
  )
}

function BucketMetaLine({ bucket }: { bucket: NonNullable<ReturnType<typeof useBucket>['data']> }) {
  const details = [
    formatObjectCount(bucket.object_count),
    formatBytes(bucket.total_size_bytes),
    `Versioning ${bucket.versioning_status.toLowerCase()}`,
  ].filter(Boolean)

  return (
    <div className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1 text-xs">
      {details.map((detail, index) => (
        <Fragment key={detail}>
          {index > 0 && (
            <span aria-hidden="true" className="text-border">
              ·
            </span>
          )}
          <span className="truncate">{detail}</span>
        </Fragment>
      ))}
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
          </DropdownMenuGroup>
        </DropdownMenuContent>
      </DropdownMenu>
      <ObjectVersionsDialog
        bucketName={bucketName}
        object={object}
        open={versionsOpen}
        onOpenChange={setVersionsOpen}
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
