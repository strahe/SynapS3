import { useQueryClient } from '@tanstack/react-query'
import { createFileRoute, type HistoryState, Link, useLocation, useNavigate } from '@tanstack/react-router'
import {
  CheckCircle2,
  CircleSlash,
  Clock3,
  Database,
  Download,
  FileIcon,
  Fingerprint,
  Folder,
  History,
  Info,
  Loader2,
  RefreshCw,
  Repeat2,
  RotateCcw,
  Trash2,
  TriangleAlert,
  Upload,
  UserRound,
} from 'lucide-react'
import { type ChangeEvent, Fragment, useEffect, useRef, useState } from 'react'
import { toast } from 'sonner'
import {
  APIError,
  api,
  type BucketDetail,
  type BucketStorageHealthSummary,
  type BucketStorageRiskVersion,
  type CopyHealthInfo,
  type DeletedObjectItem,
  maxFOCUploadSize,
  minFOCUploadSize,
  type ObjectFolderItem,
  type ObjectItem,
  type ObjectProvenance,
  type ObjectProvenanceCopy,
  type ObjectState,
  type ObjectStatus,
  type ObjectUploadClientProgress,
  type ObjectVersionItem,
  objectVersionAlreadyCurrentCode,
  type ProviderReplacement,
  type StorageDataSetSummary,
  type StorageHealthStatus,
  type UploadTransferProgress,
  validateFOCUploadSize,
} from '@/api/client'
import { BreadcrumbCurrentPage } from '@/components/app/BreadcrumbCurrentPage'
import { CopyableValue, OptionalCopyableValue } from '@/components/app/CopyableValue'
import { CursorPagination } from '@/components/app/CursorPagination'
import { DangerActionAlertDialog } from '@/components/app/DangerActionAlertDialog'
import { clickableRowProps, DataTableFrame, TableSkeleton, tableHeaderRowClassName } from '@/components/app/DataTable'
import { DetailField, DetailGrid } from '@/components/app/DetailPanel'
import { DetailTextDialog } from '@/components/app/DetailTextDialog'
import { PageHeader, RefreshButton } from '@/components/app/PageHeader'
import { EmptyState, PageError } from '@/components/app/PageState'
import { ProviderIdentityCell } from '@/components/app/ProviderIdentityCell'
import { ProviderProfileDetails } from '@/components/app/ProviderProfileDetails'
import { ProviderReplacementProgress as ReplacementProgressView } from '@/components/app/ProviderReplacementProgress'
import { ProviderSelect } from '@/components/app/ProviderSelect'
import { RelativeTime } from '@/components/app/RelativeTime'
import { ReviewDetails } from '@/components/app/ReviewDetails'
import { RowActionItem, RowActionsMenu } from '@/components/app/RowActionsMenu'
import { bucketStatusTone, StatusBadge, type StatusTone } from '@/components/app/StatusBadge'
import { UploadProgressRing, uploadProgressPercent } from '@/components/app/UploadProgress'
import { WarmStoragePriceDetails } from '@/components/app/WarmStoragePriceDetails'
import { BucketOwnerDialog } from '@/components/buckets/BucketOwnerDialog'
import { type RiskMarkers, StorageRiskHeader, StorageRiskView } from '@/components/buckets/StorageRiskView'
import { SettingsSection } from '@/components/settings/settings-form'
import { RetryButton } from '@/components/tasks/RetryButton'
import { Alert, AlertDescription } from '@/components/ui/alert'
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbSeparator,
} from '@/components/ui/breadcrumb'
import { Button } from '@/components/ui/button'
import { Card, CardAction, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { DropdownMenuSeparator } from '@/components/ui/dropdown-menu'
import { Field, FieldDescription, FieldGroup, FieldLabel } from '@/components/ui/field'
import { Input } from '@/components/ui/input'
import { Progress } from '@/components/ui/progress'
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area'
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import {
  useBucket,
  useBucketObjects,
  useBucketObjectVersions,
  useBucketStorageRiskVersions,
  useDeleteBucketObject,
  useDeletedBucketObjects,
  useObjectProvenance,
  useObjectStatusDetail,
  usePermanentDeleteBucketObjectVersion,
  usePermanentDeleteDeletedBucketObject,
  useRefreshDataSetStorageHealth,
  useRefreshProvider,
  useReplacementProviderCandidates,
  useRestoreBucketObject,
  useRestoreBucketObjectVersion,
  useRetryTask,
  useS3Users,
  useStartProviderReplacement,
  useTestProviderUploadSpeed,
  useUpdateBucketCopyPolicy,
  useWarmStoragePriceList,
} from '@/hooks/queries'
import {
  bucketCopyPolicyLabel,
  bucketCopyPolicySavedMessage,
  bucketCopyPolicyValue,
  clampMinimumDurableCopiesValue,
  copyPolicyOptions,
  minimumDurableCopiesChoiceNote,
  minimumDurableCopiesFixedCountNote,
  minimumDurableCopiesOptionLabel,
  minimumDurableCopiesOptions,
  minimumDurableCopiesValue,
  minimumDurableCopiesWarning,
  persistMinimumDurableCopies,
  replicaCountLabel,
  replicaTargetChoiceNote,
  replicaTargetLocked,
  replicaTargetLockNote,
  selectedTargetCopies,
  showsMinimumDurableCopiesWarning,
} from '@/lib/bucket-copy-policy'
import { type BucketRouteSearch, type BucketTab, normalizeBucketRouteSearch } from '@/lib/bucket-route-search'
import {
  bucketStorageHealthAffectedVersionsLabel,
  bucketStorageHealthLabel,
  bucketStorageHealthObservationLabel,
  bucketStorageHealthStatusTone,
  bucketStorageHealthTitle,
} from '@/lib/bucket-storage-health'
import {
  dataSetNeedsStorageRiskReview,
  dataSetStorageImpactLabel,
  dataSetStorageImpactTone,
} from '@/lib/bucket-storage-risk'
import {
  copyHealthInfoTitle,
  copyHealthStatusLabel,
  copyHealthStatusTone,
  copyHealthSummaryLabel,
  copyHealthSummaryTitle,
} from '@/lib/copy-health'
import { dataSetStorageHealthDetailParts, dataSetStorageHealthRefreshErrorMessage } from '@/lib/data-set-storage-health'
import { bucketStatusLabel, providerSelectionStrategyLabel, versioningStatusLabel } from '@/lib/display-labels'
import {
  activeReplacements,
  dataSetGenerationLabel,
  dataSetGenerationTone,
  dataSetSetupMessage,
  providerCandidateDisabledReason,
  replacementConfirmationDescription,
  replacementConfirmationSummary,
  replacementErrorMessage,
  replacementNextStep,
  replacementStatusLabel,
  replacementStatusTone,
} from '@/lib/provider-replacement'
import { providerUploadSpeedLabel } from '@/lib/provider-upload-speed'
import { ownerLabel } from '@/lib/s3-owner'
import { type BucketPrefixCrumb, bucketPrefixCrumbs, duplicateObjectUploadKeys, objectUploadKey } from '@/lib/s3-prefix'
import {
  storageConfirmationAttentionView,
  storageConfirmationRetryNote,
  storageConfirmationTasksSearch,
} from '@/lib/storage-confirmation-attention'
import { objectStateLabel, replicaLabel, transferMethodLabel } from '@/lib/storage-status-labels'
import { bucketStorageDataSetTopologyLinkModel, observabilityStatusLabel } from '@/lib/storage-topology'
import { formatBytes, formatNumber, formatTokenAmount, timeAgo } from '@/lib/utils'

export const Route = createFileRoute('/buckets/$name')({
  validateSearch: (search: Record<string, unknown>): BucketRouteSearch => normalizeBucketRouteSearch(search),
  component: ObjectBrowserPage,
})

declare module '@tanstack/react-router' {
  interface HistoryState {
    bucketObjectMarkers?: string[]
    bucketRiskMarkers?: RiskMarkers[]
  }
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
  const [versionMarkers, setVersionMarkers] = useState([''])
  const versionMarker = versionMarkers[versionMarkers.length - 1] ?? ''
  const titleRef = useRef<HTMLHeadingElement>(null)
  const versions = useBucketObjectVersions(bucketName, objectKey, versionMarker, 50, open)

  useEffect(() => {
    if (open) setVersionMarkers([''])
  }, [open])

  const hasPreviousPage = versionMarkers.length > 1
  const nextVersionMarker = versions.data?.has_more ? versions.data.next_version_marker : undefined

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
            <span className="sr-only">Object versions for selected object.</span>
          </DialogDescription>
          <div className="pr-8 text-muted-foreground">
            <CopyableValue
              label="Object key"
              value={objectKey}
              monospace
              maxLength={objectKey.length}
              className="max-w-full"
            />
          </div>
        </DialogHeader>
        {versions.isLoading ? (
          <div className="flex h-40 items-center justify-center">
            <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
          </div>
        ) : versions.error ? (
          <PageError title="Failed to load object versions" onRetry={() => versions.refetch()} />
        ) : (
          <DataTableFrame>
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
                <TableRow className={tableHeaderRowClassName}>
                  <TableHead className="px-2">Version</TableHead>
                  <TableHead className="px-2 text-right">Size</TableHead>
                  <TableHead className="px-2">Location</TableHead>
                  <TableHead className="px-2">ETag</TableHead>
                  <TableHead className="px-2">Piece CID</TableHead>
                  <TableHead className="px-2">Created</TableHead>
                  <TableHead className="px-2 text-right">
                    <span className="sr-only">Actions</span>
                  </TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {versions.data?.versions.map((version) => (
                  <TableRow key={version.version_id}>
                    <TableCell className="overflow-hidden px-2">
                      <div className="flex min-w-0 items-center gap-2">
                        <CopyableValue label="Version" value={version.version_id} monospace maxLength={22} />
                        {version.is_delete_marker ? (
                          <StatusBadge tone="neutral" className="shrink-0">
                            Delete marker
                          </StatusBadge>
                        ) : (
                          <ObjectStatusIcon
                            bucketName={bucketName}
                            versionID={version.version_id}
                            state={version.state}
                            status={version.status}
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
                    <TableCell className="overflow-hidden px-2 text-muted-foreground">
                      <CopyableValue label="ETag" value={version.etag} monospace maxLength={22} />
                    </TableCell>
                    <TableCell className="overflow-hidden px-2 text-muted-foreground">
                      {version.piece_cid ? (
                        <CopyableValue label="Piece CID" value={version.piece_cid} monospace maxLength={24} />
                      ) : (
                        '—'
                      )}
                    </TableCell>
                    <TableCell className="overflow-hidden truncate px-2 text-muted-foreground">
                      <RelativeTime value={version.created_at} />
                    </TableCell>
                    <TableCell className="px-2 text-right">
                      <VersionActions
                        bucketName={bucketName}
                        objectKey={objectKey}
                        version={version}
                        currentVersionID={versions.data?.current_version_id}
                        onVersionsChanged={() => setVersionMarkers([''])}
                      />
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
          </DataTableFrame>
        )}
        <CursorPagination
          hasPrevious={hasPreviousPage}
          hasNext={Boolean(nextVersionMarker)}
          onPrevious={() => setVersionMarkers((markers) => markers.slice(0, -1))}
          onNext={() => nextVersionMarker && setVersionMarkers((markers) => [...markers, nextVersionMarker])}
        />
      </DialogContent>
    </Dialog>
  )
}

function VersionActions({
  bucketName,
  objectKey,
  version,
  currentVersionID,
  onVersionsChanged,
}: {
  bucketName: string
  objectKey: string
  version: ObjectVersionItem
  currentVersionID?: string
  onVersionsChanged: () => void
}) {
  const queryClient = useQueryClient()
  const [provenanceOpen, setProvenanceOpen] = useState(false)
  const [restoreOpen, setRestoreOpen] = useState(false)
  const [permanentDeleteOpen, setPermanentDeleteOpen] = useState(false)
  const restore = useRestoreBucketObjectVersion()
  const permanentDelete = usePermanentDeleteBucketObjectVersion()

  if (version.is_delete_marker) return null

  const handlePermanentDelete = () => {
    permanentDelete.mutate(
      { name: bucketName, key: objectKey, versionID: version.version_id },
      {
        onSuccess: () => {
          toast.success('Version permanently deleted')
          setPermanentDeleteOpen(false)
          permanentDelete.reset()
        },
      }
    )
  }

  const handleRestoreOpenChange = (next: boolean) => {
    setRestoreOpen(next)
    if (!next) restore.reset()
  }

  const handleRestore = () => {
    if (!currentVersionID) return
    restore.mutate(
      {
        name: bucketName,
        key: objectKey,
        sourceVersionID: version.version_id,
        expectedCurrentVersionID: currentVersionID,
      },
      {
        onSuccess: () => {
          toast.success('Restored as a new current version')
          setRestoreOpen(false)
          restore.reset()
          onVersionsChanged()
        },
        onError: (error) => {
          if (error instanceof APIError && error.status === 409 && error.code !== objectVersionAlreadyCurrentCode) {
            queryClient.invalidateQueries({ queryKey: ['objectVersions', bucketName, objectKey] })
          }
        },
      }
    )
  }

  const restoreAlreadyCurrent =
    restore.error instanceof APIError && restore.error.code === objectVersionAlreadyCurrentCode
  const restoreConflict = restore.error instanceof APIError && restore.error.status === 409 && !restoreAlreadyCurrent

  return (
    <>
      <RowActionsMenu label={version.version_id}>
        <RowActionItem asChild>
          <a
            href={api.getObjectDownloadUrl(bucketName, objectKey, version.version_id)}
            aria-label={`Download ${objectKey} version ${version.version_id}`}
          >
            <Download data-icon="inline-start" />
            Download
          </a>
        </RowActionItem>
        <RowActionItem onSelect={() => setProvenanceOpen(true)}>
          <Fingerprint data-icon="inline-start" />
          Provenance
        </RowActionItem>
        {!version.is_current && (
          <RowActionItem onSelect={() => setRestoreOpen(true)}>
            <RotateCcw data-icon="inline-start" />
            Restore as new version
          </RowActionItem>
        )}
        <DropdownMenuSeparator />
        <RowActionItem variant="destructive" onSelect={() => setPermanentDeleteOpen(true)}>
          <Trash2 data-icon="inline-start" />
          Permanently delete
        </RowActionItem>
      </RowActionsMenu>
      <ObjectProvenanceDialog
        bucketName={bucketName}
        objectKey={objectKey}
        versionID={version.version_id}
        open={provenanceOpen}
        onOpenChange={setProvenanceOpen}
      />
      <Dialog open={restoreOpen} onOpenChange={handleRestoreOpenChange}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Restore as new version</DialogTitle>
            <DialogDescription>
              Create a new current version from this data. Existing versions and delete markers remain in history.
            </DialogDescription>
          </DialogHeader>
          <ReviewDetails
            rows={[
              { id: 'key', label: 'Object', value: objectKey, copyable: true, maxLength: 36 },
              { id: 'source-version', label: 'Source version', value: version.version_id, copyable: true },
              {
                id: 'current-version',
                label: 'Current version',
                value: currentVersionID ?? 'Unavailable',
                copyable: Boolean(currentVersionID),
              },
              { id: 'size', label: 'Size', value: formatBytes(version.size) },
            ]}
          />
          {restore.error && (
            <Alert variant="destructive">
              <AlertDescription>
                {restoreAlreadyCurrent
                  ? 'This version already matches the current object. No new version was created.'
                  : restoreConflict
                    ? 'Object versions changed while this dialog was open. The list has been refreshed; review the current version and confirm again.'
                    : restore.error.message}
              </AlertDescription>
            </Alert>
          )}
          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              onClick={() => handleRestoreOpenChange(false)}
              disabled={restore.isPending}
            >
              Cancel
            </Button>
            <Button
              type="button"
              onClick={handleRestore}
              disabled={restore.isPending || !currentVersionID || restoreAlreadyCurrent}
            >
              {restore.isPending && <Loader2 data-icon="inline-start" className="animate-spin" />}
              Restore as new version
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
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
            { id: 'key', label: 'Object', value: objectKey, copyable: true, maxLength: 36 },
            { id: 'version', label: 'Version', value: version.version_id, copyable: true },
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

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="w-[calc(100vw-2rem)] max-w-[calc(100vw-2rem)] sm:max-w-5xl lg:p-6">
        <DialogHeader>
          <DialogTitle>Storage provenance</DialogTitle>
          <DialogDescription className="pr-8">
            <span className="sr-only">Storage provenance for selected object version.</span>
          </DialogDescription>
          <div className="flex min-w-0 flex-col gap-1 pr-8 text-muted-foreground">
            <CopyableValue
              label="Object key"
              value={objectKey}
              monospace
              maxLength={objectKey.length}
              className="max-w-full"
            />
            <CopyableValue
              label="Version"
              value={versionID}
              monospace
              maxLength={versionID.length}
              className="max-w-full"
            />
          </div>
        </DialogHeader>

        {provenance.isLoading ? (
          <div className="flex h-40 items-center justify-center">
            <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
          </div>
        ) : provenance.error ? (
          <PageError title="Failed to load provenance" onRetry={() => provenance.refetch()} />
        ) : data ? (
          <div className="flex max-h-[70vh] flex-col gap-4 overflow-y-auto pr-1">
            <ProvenanceSummary data={data} />
            <ProvenanceCopies copies={data.copies} />
          </div>
        ) : null}
      </DialogContent>
    </Dialog>
  )
}

function ProvenanceSummary({ data }: { data: ObjectProvenance }) {
  const progressPercent = uploadProgressPercent(data.progress)

  return (
    <div className="rounded-lg border border-border p-3">
      <DetailGrid className="@xl:grid-cols-4">
        <DetailField label="Object status">{objectStateLabel(data.state, data.status, progressPercent)}</DetailField>
        <DetailField label="Replicas">{`${data.success_copies} / ${data.requested_copies}`}</DetailField>
        <DetailField label="Replica health">
          <span title={copyHealthSummaryTitle(data.copy_health)}>{copyHealthSummaryLabel(data.copy_health)}</span>
        </DetailField>
        <DetailField label="Updated">
          <RelativeTime value={data.updated_at} />
        </DetailField>
        <DetailField label="Piece CID" wide>
          <OptionalCopyableValue label="Piece CID" value={data.piece_cid} maxLength={72} />
        </DetailField>
      </DetailGrid>
    </div>
  )
}

function ProvenanceCopies({ copies }: { copies: ObjectProvenanceCopy[] }) {
  const retry = useRetryTask()
  return (
    <DataTableFrame>
      <h3 className="border-b border-border px-3 py-2 text-sm font-semibold">Replicas</h3>
      {retry.error && (
        <Alert>
          <AlertDescription>Could not retry. Refresh and try again.</AlertDescription>
        </Alert>
      )}
      <ScrollArea className="w-full">
        <Table className="min-w-[1080px]">
          <TableHeader>
            <TableRow className={tableHeaderRowClassName}>
              <TableHead className="px-3">Replica</TableHead>
              <TableHead className="px-3">Transfer</TableHead>
              <TableHead className="px-3">Status</TableHead>
              <TableHead className="px-3">Health</TableHead>
              <TableHead className="px-3">Provider</TableHead>
              <TableHead className="px-3">Data set ID</TableHead>
              <TableHead className="px-3">Piece ID</TableHead>
              <TableHead className="px-3">New data set</TableHead>
              <TableHead className="px-3">Retrieval URL</TableHead>
              <TableHead className="px-3">Recovery</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {copies.map((copy) => (
              <TableRow key={copy.copy_id}>
                <TableCell className="px-3 font-mono text-xs">{replicaLabel(copy.copy_index)}</TableCell>
                <TableCell className="px-3">
                  {transferMethodLabel(copy.transfer_method)}
                  {copy.transfer_method === 'cache_restore' && copy.progress && copy.status !== 'committed' && (
                    <div className="text-xs text-muted-foreground">
                      {formatBytes(copy.progress.uploaded_bytes)} of {formatBytes(copy.progress.total_bytes)} uploaded
                      {copy.progress.percent !== undefined && ` · ${copy.progress.percent}%`}
                    </div>
                  )}
                </TableCell>
                <TableCell className="px-3">
                  <StatusBadge tone={copyStatusTone(copy)}>{copyStatusLabel(copy)}</StatusBadge>
                  {copy.last_error && (
                    <div className="mt-1">
                      <CopyableValue label="Last error" value={copy.last_error} maxLength={60} />
                    </div>
                  )}
                  {copy.attention_code && (
                    <CopyAttentionDetails
                      reasonCode={copy.attention_code}
                      attentionAt={copy.attention_at}
                      submitError={copy.submit_error}
                    />
                  )}
                </TableCell>
                <TableCell className="px-3">
                  <CopyHealthCell health={copy.health} />
                </TableCell>
                <TableCell className="px-3">
                  <ProviderIdentityCell providerID={copy.provider_id} identity={copy.provider_identity} />
                </TableCell>
                <TableCell className="px-3 text-muted-foreground">
                  <OptionalCopyableValue label="Data set ID" value={copy.data_set_id} />
                </TableCell>
                <TableCell className="px-3 text-muted-foreground">
                  <OptionalCopyableValue label="Piece ID" value={copy.piece_id} maxLength={24} />
                </TableCell>
                <TableCell className="px-3 text-muted-foreground">{copy.is_new_data_set ? 'Yes' : 'No'}</TableCell>
                <TableCell className="max-w-72 overflow-hidden px-3 text-muted-foreground">
                  {copy.retrieval_url ? (
                    <CopyableValue
                      label="Retrieval URL"
                      value={copy.retrieval_url}
                      monospace
                      maxLength={36}
                      linkHref={copy.retrieval_url}
                      external
                    />
                  ) : (
                    '—'
                  )}
                </TableCell>
                <TableCell className="px-3">
                  {copy.retryable ? (
                    <RetryButton
                      taskID={copy.retry_task_id}
                      pending={retry.isPending && retry.variables === copy.retry_task_id}
                      disabled={retry.isPending}
                      onRetry={(id) => retry.mutate(id)}
                    />
                  ) : (
                    <span className="text-sm text-muted-foreground">{copy.retry_unavailable_reason || '—'}</span>
                  )}
                </TableCell>
              </TableRow>
            ))}
            {copies.length === 0 && (
              <TableRow>
                <TableCell colSpan={10} className="h-20 text-center text-muted-foreground">
                  No replicas recorded
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
        <ScrollBar orientation="horizontal" />
      </ScrollArea>
    </DataTableFrame>
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
  progress,
  compact = false,
}: {
  bucketName: string
  versionID: string
  state?: ObjectState
  status: ObjectStatus
  progress?: UploadTransferProgress
  compact?: boolean
}) {
  const [detailEnabled, setDetailEnabled] = useState(false)
  const detail = useObjectStatusDetail(bucketName, versionID, status === 'warning' && detailEnabled)
  const progressPercent = uploadProgressPercent(progress)
  const displayLabel = objectStateLabel(state, status, progressPercent)
  const progressDetail =
    progressPercent === null || !progress
      ? null
      : `${formatBytes(progress.uploaded_bytes)} of ${formatBytes(progress.total_bytes)} uploaded`

  const loadDetail = () => {
    if (status === 'warning') setDetailEnabled(true)
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
          {objectStatusIcon(status, compact, progressPercent)}
        </button>
      </TooltipTrigger>
      <TooltipContent side="top" className="max-w-sm items-start whitespace-normal text-left">
        <div className="flex max-w-xs flex-col gap-1">
          <span className="font-medium">{displayLabel}</span>
          {progressDetail && <span className="break-words opacity-90">{progressDetail}</span>}
          {status === 'warning' && (
            <span className="break-words opacity-90">
              {detail.isLoading
                ? 'Loading issue details'
                : detail.error
                  ? 'Failed to load issue details'
                  : detail.data?.message
                    ? detail.data.message
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

function copyStatusTone(copy: ObjectProvenanceCopy): StatusTone {
  if (copy.attention_code) return 'warning'
  switch (copy.status) {
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

function copyStatusLabel(copy: ObjectProvenanceCopy) {
  if (copy.attention_code) return 'Needs review'
  switch (copy.status) {
    case 'pending':
      return 'Waiting'
    case 'piece_ready':
      return 'Ready'
    case 'committing':
      return 'Submitting'
    case 'committed':
      return 'Stored'
    case 'failed':
      return 'Failed'
  }
}

function CopyAttentionDetails({
  reasonCode,
  attentionAt,
  submitError,
}: {
  reasonCode: string
  attentionAt?: string
  submitError?: string
}) {
  const attention = storageConfirmationAttentionView(reasonCode)

  return (
    <details className="group mt-1 max-w-72 text-xs text-muted-foreground">
      <summary className="cursor-pointer break-words text-status-warning marker:text-muted-foreground">
        {attention.label}
        {attentionAt ? ` · ${timeAgo(attentionAt)}` : ''}
      </summary>
      <div className="mt-2 space-y-2 whitespace-normal rounded-md border border-border bg-muted/30 p-2 leading-relaxed">
        {submitError && (
          <div className="space-y-1">
            <div>Provider response</div>
            <CopyableValue label="Provider response" value={submitError} displayValue={submitError} maxLength={60} />
          </div>
        )}
        <p>
          Use Retry in{' '}
          <Link to="/tasks" search={storageConfirmationTasksSearch} className="text-foreground underline">
            Tasks
          </Link>
          . {storageConfirmationRetryNote}
        </p>
        {!attention.known && (
          <div className="space-y-1">
            <div>Reason code</div>
            <CopyableValue label="Confirmation reason code" value={attention.reasonCode} monospace maxLength={28} />
          </div>
        )}
      </div>
    </details>
  )
}

function ObjectBrowserPage() {
  const { name } = Route.useParams()
  const search = Route.useSearch()
  const navigate = useNavigate()
  const qc = useQueryClient()
  const tab: BucketTab = search.tab ?? 'objects'
  const browsing = tab === 'objects' || tab === 'trash'
  const prefix = search.prefix ?? ''
  const marker = search.marker ?? ''
  const riskView = tab === 'storage' && Boolean(search.risk)
  const riskPrefix = search.risk_prefix ?? ''
  const riskKey = search.risk_key ?? ''
  const riskDatasetID = search.risk_dataset ? Number(search.risk_dataset) : undefined
  const riskKeyMarker = search.risk_key_marker ?? ''
  const riskVersionMarker = search.risk_version_marker ?? ''
  const riskCreatedAtMarker = search.risk_created_at_marker ?? ''
  const riskStaleBefore = search.risk_stale_before ?? ''
  // Each history entry keeps its own previous pages across browser navigation and panel reloads.
  const markerHistory = useLocation({ select: (location) => location.state.bucketObjectMarkers }) ?? []
  const [uploadOpen, setUploadOpen] = useState(false)
  const [riskProvenanceVersion, setRiskProvenanceVersion] = useState<BucketStorageRiskVersion | null>(null)

  const bucket = useBucket(name)
  const objects = useBucketObjects(name, prefix, marker, 50, '/', tab === 'objects')
  const deletedObjects = useDeletedBucketObjects(name, prefix, marker, 50, tab === 'trash')
  const storageRisk = useBucketStorageRiskVersions(
    name,
    {
      prefix: riskKey ? undefined : riskPrefix || undefined,
      key: riskKey || undefined,
      local_data_set_id: riskDatasetID,
      key_marker: riskKeyMarker || undefined,
      version_marker: riskVersionMarker || undefined,
      created_at_marker: riskCreatedAtMarker || undefined,
      stale_before: riskStaleBefore || undefined,
      limit: 50,
    },
    riskView
  )

  const go = (next: BucketRouteSearch, state: HistoryState = {}) =>
    navigate({ to: '/buckets/$name', params: { name }, search: next, state })
  const listTab = tab === 'trash' ? ('trash' as const) : undefined

  const selectTab = (next: BucketTab) => {
    go({
      tab: next === 'objects' ? undefined : next,
      prefix: next === 'objects' || next === 'trash' ? prefix || undefined : undefined,
    })
  }

  const navigateToPrefix = (newPrefix: string) => {
    go({ tab: listTab, prefix: newPrefix || undefined })
  }

  const goToMarker = (nextMarker: string, history: string[]) =>
    go({ tab: listTab, prefix: prefix || undefined, marker: nextMarker || undefined }, { bucketObjectMarkers: history })

  const nextPage = (nextMarker: string) => {
    goToMarker(nextMarker, [...markerHistory, marker])
  }

  const previousPage = () => {
    // Without a remembered page (a shared link), Previous returns to the first page.
    goToMarker(markerHistory[markerHistory.length - 1] ?? '', markerHistory.slice(0, -1))
  }

  const navigateToStorageRiskMarker = (
    keyMarker: string,
    nextVersionMarker: string,
    nextCreatedAtMarker: string,
    nextStaleBefore: string,
    history: RiskMarkers[]
  ) =>
    go(
      {
        tab: 'storage',
        risk: true,
        risk_prefix: riskKey ? undefined : riskPrefix || undefined,
        risk_dataset: search.risk_dataset,
        risk_key: riskKey || undefined,
        risk_key_marker: keyMarker || undefined,
        risk_version_marker: nextVersionMarker || undefined,
        risk_created_at_marker: nextCreatedAtMarker || undefined,
        risk_stale_before: nextStaleBefore || undefined,
      },
      { bucketRiskMarkers: history }
    )

  const navigateToStorageRiskFilters = (next: { prefix?: string; key?: string; dataSetID?: number }) =>
    go({
      tab: 'storage',
      risk: true,
      risk_prefix: next.key ? undefined : next.prefix || undefined,
      risk_dataset: next.dataSetID ? next.dataSetID.toString() : undefined,
      risk_key: next.key || undefined,
    })

  const handleRefresh = () => {
    qc.invalidateQueries({ queryKey: ['bucket', name] })
    qc.invalidateQueries({ queryKey: ['objects', name] })
    qc.invalidateQueries({ queryKey: ['deletedObjects', name] })
    qc.invalidateQueries({ queryKey: ['bucketStorageRiskVersions', name] })
  }

  const handleUploadCompleted = () => {
    qc.invalidateQueries({ queryKey: ['bucket', name] })
    qc.invalidateQueries({ queryKey: ['objects', name] })
    qc.invalidateQueries({ queryKey: ['bucketStorageRiskVersions', name] })
    qc.invalidateQueries({ queryKey: ['tasks'] })
    qc.invalidateQueries({ queryKey: ['taskStats'] })
    if (marker) {
      go({ prefix: prefix || undefined })
    }
  }

  const data = bucket.data
  const canUpload = data?.status === 'ready'

  return (
    <div className="flex flex-col gap-6 p-6">
      <div className="flex flex-col gap-2">
        <BucketBreadcrumb
          name={name}
          pathCrumbs={browsing ? bucketPrefixCrumbs(prefix) : []}
          navigateToPrefix={navigateToPrefix}
        />
        <PageHeader
          title={name}
          meta={
            data && <StatusBadge tone={bucketStatusTone(data.status)}>{bucketStatusLabel(data.status)}</StatusBadge>
          }
          description={data && <BucketSummary bucket={data} />}
          actions={
            <>
              <RefreshButton onClick={handleRefresh} refreshing={bucket.isFetching} />
              {tab === 'objects' && (
                <Button size="sm" onClick={() => setUploadOpen(true)} disabled={!canUpload}>
                  <Upload data-icon="inline-start" />
                  Upload
                </Button>
              )}
            </>
          }
        />
      </div>

      {bucket.error && (
        <PageError
          title="Failed to load bucket details"
          description={bucket.error.message}
          onRetry={() => bucket.refetch()}
          retrying={bucket.isFetching}
        />
      )}

      <Tabs value={tab} onValueChange={(value) => selectTab(value as BucketTab)} className="min-w-0 gap-6">
        <TabsList className="max-w-full justify-start overflow-x-auto">
          <TabsTrigger value="objects">Objects</TabsTrigger>
          <TabsTrigger value="trash">Trash</TabsTrigger>
          <TabsTrigger value="storage">Storage</TabsTrigger>
          <TabsTrigger value="settings">Settings</TabsTrigger>
        </TabsList>
        {/* Only the selected tab mounts, so one panel carries whichever view is open. */}
        <TabsContent value={tab} className="min-w-0 text-base">
          {tab === 'objects' ? (
            objects.isLoading ? (
              <TableSkeleton />
            ) : objects.error ? (
              <PageError
                title="Failed to load objects"
                description={objects.error.message}
                onRetry={() => objects.refetch()}
                retrying={objects.isFetching}
              />
            ) : (
              <ObjectBrowserTable
                bucketName={name}
                prefix={prefix}
                folders={objects.data?.folders ?? []}
                files={objects.data?.objects ?? []}
                hasPrevious={Boolean(marker)}
                nextMarker={objects.data?.has_more ? objects.data.next_marker : undefined}
                onPrevious={previousPage}
                onNext={nextPage}
                navigateToPrefix={navigateToPrefix}
                onUpload={canUpload ? () => setUploadOpen(true) : undefined}
              />
            )
          ) : tab === 'trash' ? (
            deletedObjects.isLoading ? (
              <TableSkeleton />
            ) : deletedObjects.error ? (
              <PageError
                title="Failed to load trash"
                description={deletedObjects.error.message}
                onRetry={() => deletedObjects.refetch()}
                retrying={deletedObjects.isFetching}
              />
            ) : (
              <DeletedObjectsTable
                bucketName={name}
                prefix={prefix}
                objects={deletedObjects.data?.objects ?? []}
                hasPrevious={Boolean(marker)}
                nextMarker={deletedObjects.data?.has_more ? deletedObjects.data.next_marker : undefined}
                onPrevious={previousPage}
                onNext={nextPage}
              />
            )
          ) : !data ? (
            bucket.isLoading && <TableSkeleton />
          ) : tab === 'settings' ? (
            <BucketSettingsTab bucket={data} />
          ) : riskView ? (
            <div className="flex min-w-0 flex-col gap-4">
              <StorageRiskHeader onBack={() => go({ tab: 'storage' })} />
              {storageRisk.isLoading ? (
                <TableSkeleton />
              ) : storageRisk.error ? (
                <PageError
                  title="Failed to load affected versions"
                  description={storageRisk.error.message}
                  onRetry={() => storageRisk.refetch()}
                  retrying={storageRisk.isFetching}
                />
              ) : (
                <StorageRiskView
                  prefix={riskKey ? '' : riskPrefix}
                  exactKey={riskKey}
                  dataSetID={riskDatasetID}
                  dataSets={data.data_sets ?? []}
                  versions={storageRisk.data?.versions ?? []}
                  hasMore={storageRisk.data?.has_more ?? false}
                  nextKeyMarker={storageRisk.data?.next_key_marker}
                  nextVersionMarker={storageRisk.data?.next_version_marker}
                  nextCreatedAtMarker={storageRisk.data?.next_created_at_marker}
                  staleBefore={storageRisk.data?.stale_before}
                  keyMarker={riskKeyMarker}
                  versionMarker={riskVersionMarker}
                  createdAtMarker={riskCreatedAtMarker}
                  staleBeforeMarker={riskStaleBefore}
                  navigateToMarker={navigateToStorageRiskMarker}
                  navigateToFilters={navigateToStorageRiskFilters}
                  onOpenProvenance={setRiskProvenanceVersion}
                />
              )}
            </div>
          ) : (
            <BucketStorageTab
              bucket={data}
              onReviewStorageRisk={() => navigateToStorageRiskFilters({})}
              onReviewStorageDataSetRisk={(dataSetID) => navigateToStorageRiskFilters({ dataSetID })}
            />
          )}
        </TabsContent>
      </Tabs>

      <ObjectProvenanceDialog
        bucketName={name}
        objectKey={riskProvenanceVersion?.key ?? ''}
        versionID={riskProvenanceVersion?.version_id ?? ''}
        open={Boolean(riskProvenanceVersion)}
        onOpenChange={(open) => {
          if (!open) setRiskProvenanceVersion(null)
        }}
      />
      <UploadObjectsDialog
        bucketName={name}
        prefix={prefix}
        open={uploadOpen}
        onOpenChange={setUploadOpen}
        onUploaded={handleUploadCompleted}
      />
    </div>
  )
}

function BucketSummary({ bucket }: { bucket: BucketDetail }) {
  const { data: users = [] } = useS3Users()
  const health = bucket.storage_health
  return (
    <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
      <span>{formatObjectCount(bucket.object_count)}</span>
      <span aria-hidden="true">·</span>
      <span>{formatBytes(bucket.total_size_bytes)}</span>
      <span aria-hidden="true">·</span>
      <span>{bucketCopyPolicyLabel(bucket)}</span>
      <span aria-hidden="true">·</span>
      <span>Owner {ownerLabel(bucket.owner_access_key, users)}</span>
      <span className="inline-flex" title={bucketStorageHealthTitle(health)}>
        <StatusBadge tone={bucketStorageHealthStatusTone(health)}>{bucketStorageHealthLabel(health)}</StatusBadge>
      </span>
    </div>
  )
}

function BucketStorageTab({
  bucket,
  onReviewStorageRisk,
  onReviewStorageDataSetRisk,
}: {
  bucket: BucketDetail
  onReviewStorageRisk: () => void
  onReviewStorageDataSetRisk: (dataSetID: number) => void
}) {
  const [storageHealthError, setStorageHealthError] = useState<string | null>(null)
  // Replacement diagnostics are not replica-health diagnostics; sharing one
  // dialog labelled them as the wrong kind of fault.
  const [replacementError, setReplacementError] = useState<string | null>(null)
  const [replacementTarget, setReplacementTarget] = useState<StorageDataSetSummary | null>(null)

  return (
    <div className="flex min-w-0 flex-col gap-6">
      <BucketStorageHealthPanel
        health={bucket.storage_health}
        onOpenLastError={setStorageHealthError}
        onReviewVersions={onReviewStorageRisk}
      />
      <ProviderReplacementProgress replacements={bucket.replacements ?? []} onOpenLastError={setReplacementError} />
      <BucketStorageDataSets
        bucketName={bucket.name}
        dataSets={bucket.data_sets ?? []}
        onReviewStorageRisk={onReviewStorageDataSetRisk}
        onReplaceProvider={setReplacementTarget}
      />
      <DetailTextDialog
        title="Storage health error"
        text={storageHealthError}
        onClose={() => setStorageHealthError(null)}
      />
      <DetailTextDialog
        title="Provider replacement diagnostics"
        text={replacementError}
        onClose={() => setReplacementError(null)}
      />
      <ReplaceProviderDialog
        bucketName={bucket.name}
        dataSet={replacementTarget}
        onClose={() => setReplacementTarget(null)}
      />
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
  const finished = !uploading && items.length > 0 && items.every((item) => item.status === 'success')

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
      <DialogContent className="w-[calc(100vw-2rem)] max-w-[calc(100vw-2rem)] sm:max-w-3xl [&>*]:min-w-0">
        <DialogHeader>
          <DialogTitle>Upload objects</DialogTitle>
          <DialogDescription>
            Files upload to <span className="font-mono">{`${bucketName}/${prefix}`}</span>
          </DialogDescription>
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
          <EmptyState icon={<Upload />} title="No files selected" className="min-h-44" />
        ) : (
          <DataTableFrame>
            <ScrollArea className="max-h-80 w-full">
              <Table className="min-w-[680px] table-fixed">
                <TableHeader>
                  <TableRow className={tableHeaderRowClassName}>
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
                        <span className="block truncate" title={item.key}>
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
                          {item.error ?? '—'}
                        </span>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
              <ScrollBar orientation="horizontal" />
            </ScrollArea>
          </DataTableFrame>
        )}

        <DialogFooter>
          {finished ? (
            <Button onClick={() => handleOpenChange(false)}>Done</Button>
          ) : (
            <>
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
            </>
          )}
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
      <Progress
        value={percent}
        aria-label="Upload progress"
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={percent}
        className="min-w-0 flex-1"
      />
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

function ReplaceProviderDialog({
  bucketName,
  dataSet,
  onClose,
}: {
  bucketName: string
  dataSet: StorageDataSetSummary | null
  onClose: () => void
}) {
  const startReplacement = useStartProviderReplacement()
  const refreshProvider = useRefreshProvider()
  const testUploadSpeed = useTestProviderUploadSpeed()
  const [mode, setMode] = useState<'automatic' | 'manual'>('automatic')
  const [providerID, setProviderID] = useState('')
  const [inspectedProviderID, setInspectedProviderID] = useState('')
  const [clientRequestID, setClientRequestID] = useState('')
  const [confirmationRevision, setConfirmationRevision] = useState(0)
  const [error, setError] = useState<string | null>(null)
  const dataSetID = dataSet?.id
  const priceList = useWarmStoragePriceList(Boolean(dataSet))
  const candidates = useReplacementProviderCandidates(
    bucketName,
    dataSet?.id ?? null,
    Boolean(dataSet) && mode === 'manual'
  )

  useEffect(() => {
    if (priceList.data?.fingerprint) setClientRequestID(crypto.randomUUID())
  }, [priceList.data?.fingerprint])

  useEffect(() => {
    if (priceList.isError) setConfirmationRevision((current) => current + 1)
  }, [priceList.isError])

  useEffect(() => {
    if (!providerID || !candidates.data) return
    const selected = candidates.data.providers.find((item) => item.provider_id === providerID)
    if (!selected?.manual_selectable) {
      setProviderID('')
      setError('The selected provider is no longer available. Review its current details.')
      setClientRequestID(crypto.randomUUID())
      setConfirmationRevision((current) => current + 1)
    }
  }, [providerID, candidates.data])

  useEffect(() => {
    if (dataSetID !== undefined) {
      setMode('automatic')
      setProviderID('')
      setInspectedProviderID('')
      setError(null)
      setClientRequestID(crypto.randomUUID())
      setConfirmationRevision((current) => current + 1)
    }
  }, [dataSetID])

  if (!dataSet) return null

  const namedProvider = dataSet.provider_identity?.name?.trim()
  const providerLabel = namedProvider || (dataSet.provider_id ? `Registry ${dataSet.provider_id}` : '—')
  const inspectedProvider = candidates.data?.providers.find((item) => item.provider_id === inspectedProviderID)
  const selectedProvider = candidates.data?.providers.find((item) => item.provider_id === providerID)
  const canChooseProvider =
    mode === 'automatic' ||
    (!candidates.isLoading && !candidates.isError && selectedProvider?.manual_selectable === true)
  const canConfirmPrice = Boolean(priceList.data?.supported_token && !priceList.isError)

  const submit = () => {
    if (!canChooseProvider || !canConfirmPrice || startReplacement.isPending || !priceList.data) return
    const requestID = clientRequestID || crypto.randomUUID()
    if (!clientRequestID) setClientRequestID(requestID)
    setError(null)
    startReplacement.mutate(
      {
        bucket: bucketName,
        dataSetID: dataSet.id,
        mode,
        providerID: providerID.trim(),
        clientRequestID: requestID,
        priceListFingerprint: priceList.data.fingerprint,
      },
      {
        onSuccess: () => onClose(),
        onError: (mutationError) => {
          if (mutationError instanceof APIError && mutationError.code === 'price_list_changed') {
            setClientRequestID(crypto.randomUUID())
            setConfirmationRevision((current) => current + 1)
            void priceList.refetch()
            setError('The price list changed. Review the new prices and type replace again.')
            return
          }
          setError(replacementErrorMessage(mutationError))
        },
      }
    )
  }

  return (
    <DangerActionAlertDialog
      open
      onOpenChange={(open) => {
        if (!open) onClose()
      }}
      title="Replace provider"
      description={replacementConfirmationDescription(dataSet)}
      confirmLabel="Replace provider"
      typedTarget="replace"
      confirmationResetKey={`${priceList.data?.fingerprint ?? 'unavailable'}:${confirmationRevision}`}
      pending={startReplacement.isPending}
      confirmDisabled={!canChooseProvider || !canConfirmPrice}
      error={error}
      contentClassName="flex max-h-[90vh] w-[calc(100vw-2rem)] flex-col overflow-hidden data-[size=default]:max-w-3xl data-[size=default]:sm:max-w-3xl"
      bodyClassName="min-h-0 flex-1 space-y-4 overflow-y-auto pr-2"
      confirmationNotice={
        priceList.data?.supported_token ? (
          <p className="text-xs text-muted-foreground">
            Storage rate: {formatTokenAmount(priceList.data.rates.storage_per_tib_per_month, 18, 'USDFC')} / TiB /
            month. Review the full price list before confirming.
          </p>
        ) : undefined
      }
      onConfirm={submit}
    >
      <ReviewDetails
        rows={[
          { id: 'replica', label: 'Replica', value: replicaLabel(dataSet.copy_index), monospace: false },
          {
            id: 'provider',
            label: 'Current provider',
            value: dataSet.provider_id,
            displayValue: providerLabel,
            copyable: true,
            monospace: !namedProvider,
          },
          ...(dataSet.data_set_id
            ? [{ id: 'scope', label: 'Data to copy', value: replacementConfirmationSummary(dataSet), monospace: false }]
            : []),
        ]}
      />
      <Alert>
        <AlertDescription>
          {dataSet.data_set_id
            ? 'Existing objects copy from another replica or from cache. If an object has neither, the old provider stays.'
            : 'Objects waiting for this replica use another replica or cache. Objects without either need attention.'}
        </AlertDescription>
      </Alert>
      <FieldGroup>
        <Field data-disabled={startReplacement.isPending}>
          <FieldLabel htmlFor="replacement-mode">New provider</FieldLabel>
          <Select
            value={mode}
            onValueChange={(value) => {
              setMode(value as 'automatic' | 'manual')
              setProviderID('')
              setInspectedProviderID('')
              setClientRequestID(crypto.randomUUID())
              setConfirmationRevision((current) => current + 1)
            }}
            disabled={startReplacement.isPending}
          >
            <SelectTrigger id="replacement-mode" className="w-full">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectGroup>
                <SelectItem value="automatic">Choose automatically</SelectItem>
                <SelectItem value="manual">Choose a provider</SelectItem>
              </SelectGroup>
            </SelectContent>
          </Select>
          {mode === 'automatic' && (
            <FieldDescription>Uses an available, FWSS approved provider this bucket has not used.</FieldDescription>
          )}
        </Field>
        {mode === 'manual' && (
          <Field data-disabled={startReplacement.isPending}>
            <FieldLabel htmlFor="replacement-provider">Provider to review</FieldLabel>
            {candidates.isError ? (
              <FieldDescription>Provider choices couldn’t be loaded. Try again.</FieldDescription>
            ) : (
              <ProviderSelect
                id="replacement-provider"
                candidates={candidates.data?.providers ?? []}
                value={providerID}
                onInspect={(nextProviderID) => {
                  setInspectedProviderID(nextProviderID)
                  if (providerID && providerID !== nextProviderID) {
                    setProviderID('')
                    setClientRequestID(crypto.randomUUID())
                    setConfirmationRevision((current) => current + 1)
                  }
                  setError(null)
                }}
                disabled={startReplacement.isPending || candidates.isLoading}
              />
            )}
            {inspectedProvider && (
              <div className="mt-3 space-y-3 rounded-md border p-3">
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <div>
                    <strong>
                      {inspectedProvider.provider_profile?.name || `Registry ${inspectedProvider.provider_id}`}
                    </strong>
                    <p className="text-xs text-muted-foreground">
                      {providerID === inspectedProvider.provider_id
                        ? 'Selected for replacement'
                        : 'Review before selecting'}
                    </p>
                  </div>
                  {providerID !== inspectedProvider.provider_id && (
                    <Button
                      type="button"
                      size="sm"
                      disabled={!inspectedProvider.manual_selectable || startReplacement.isPending}
                      onClick={() => {
                        setProviderID(inspectedProvider.provider_id)
                        setClientRequestID(crypto.randomUUID())
                        setConfirmationRevision((current) => current + 1)
                      }}
                    >
                      Select this provider
                    </Button>
                  )}
                </div>
                {!inspectedProvider.manual_selectable && (
                  <p className="text-sm text-muted-foreground">{providerCandidateDisabledReason(inspectedProvider)}</p>
                )}
                <ProviderProfileDetails
                  profile={inspectedProvider.provider_profile}
                  supportedTokenAddress={priceList.data?.supported_token ? priceList.data.token : undefined}
                />
                <p className="text-xs text-muted-foreground">
                  Health: {inspectedProvider.observation?.signal.status ?? 'Not checked'} ·{' '}
                  {inspectedProvider.observation?.signal.freshness.last_checked_at
                    ? timeAgo(inspectedProvider.observation.signal.freshness.last_checked_at)
                    : 'No recent result'}
                </p>
                <p className="text-xs text-muted-foreground">
                  Upload speed: {providerUploadSpeedLabel(inspectedProvider.upload_speed_test)}
                </p>
                <div className="flex flex-wrap gap-2">
                  <Button
                    type="button"
                    size="sm"
                    variant="outline"
                    disabled={refreshProvider.isPending}
                    onClick={() => refreshProvider.mutate(inspectedProvider.provider_id)}
                  >
                    Refresh provider
                  </Button>

                  {inspectedProvider.upload_speed_test?.retryable ? (
                    <RetryButton taskID={inspectedProvider.upload_speed_test.retry_task_id} />
                  ) : (
                    <Button
                      type="button"
                      size="sm"
                      variant="outline"
                      disabled={testUploadSpeed.isPending || !inspectedProvider.manual_selectable}
                      onClick={() => testUploadSpeed.mutate(inspectedProvider.provider_id)}
                    >
                      {testUploadSpeed.isPending && testUploadSpeed.variables === inspectedProvider.provider_id
                        ? 'Starting speed test…'
                        : 'Test upload speed (32 MiB)'}
                    </Button>
                  )}
                </div>
                {inspectedProvider.upload_speed_test?.retry_unavailable_reason && (
                  <p className="text-xs text-muted-foreground">
                    {inspectedProvider.upload_speed_test.retry_unavailable_reason}
                  </p>
                )}
                {refreshProvider.isError && refreshProvider.variables === inspectedProvider.provider_id && (
                  <p className="text-xs text-destructive">Could not refresh this provider. Try again.</p>
                )}
                {refreshProvider.data?.provider_id === inspectedProvider.provider_id &&
                  !refreshProvider.data.profile_result.success && (
                    <p className="text-xs text-destructive">
                      Registry refresh failed {timeAgo(refreshProvider.data.profile_result.attempted_at)}. The previous
                      details remain available.
                    </p>
                  )}
                {refreshProvider.data?.provider_id === inspectedProvider.provider_id &&
                  refreshProvider.data.profile_result.success &&
                  !refreshProvider.data.health_result.success && (
                    <p className="text-xs text-destructive">
                      Health check failed {timeAgo(refreshProvider.data.health_result.attempted_at)}. The previous
                      result remains visible.
                    </p>
                  )}
                {testUploadSpeed.isSuccess &&
                  testUploadSpeed.variables === inspectedProvider.provider_id &&
                  !inspectedProvider.upload_speed_test && (
                    <p className="text-xs text-muted-foreground">Upload speed test started.</p>
                  )}
                {testUploadSpeed.isError && testUploadSpeed.variables === inspectedProvider.provider_id && (
                  <p className="text-xs text-destructive">Could not start the speed test. Try again.</p>
                )}
              </div>
            )}
          </Field>
        )}
      </FieldGroup>
      <div className="space-y-2 rounded-md border p-3">
        <h3 className="font-medium">Warm Storage prices</h3>
        {priceList.isError ? (
          <p className="text-sm text-destructive">Prices could not be loaded. Retry before replacing this provider.</p>
        ) : priceList.data ? (
          <WarmStoragePriceDetails price={priceList.data} />
        ) : (
          <p className="text-sm text-muted-foreground">Loading current prices…</p>
        )}
        <Button
          type="button"
          size="sm"
          variant="outline"
          disabled={priceList.isFetching}
          onClick={() => void priceList.refetch()}
        >
          Refresh prices
        </Button>
      </div>
    </DangerActionAlertDialog>
  )
}

/**
 * ProviderReplacementProgress shows the replacement that still needs something,
 * including a retry the operator owns. A failed retry request is reported here
 * rather than disappearing.
 */
function ProviderReplacementProgress({
  replacements,
  onOpenLastError,
}: {
  replacements: ProviderReplacement[]
  onOpenLastError: (text: string) => void
}) {
  const active = activeReplacements(replacements)
  if (active.length === 0) return null
  return (
    <div className="flex flex-col gap-3">
      {active.map((replacement) => (
        <ProviderReplacementProgressCard
          key={replacement.id}
          replacement={replacement}
          onOpenLastError={onOpenLastError}
        />
      ))}
    </div>
  )
}

function ProviderReplacementProgressCard({
  replacement,
  onOpenLastError,
}: {
  replacement: ProviderReplacement
  onOpenLastError: (text: string) => void
}) {
  const nextStep = replacementNextStep(replacement)

  return (
    <div className="flex flex-col gap-2 rounded-md border border-border p-3">
      <div className="flex flex-wrap items-center gap-2">
        <span className="text-sm font-medium">
          {replacement.retirement_attention ? (
            <>
              Retiring storage for {replicaLabel(replacement.copy_index)} ·{' '}
              {replacement.status === 'superseded' ? replacement.target.provider_id : replacement.source.provider_id}
            </>
          ) : (
            <>
              Replacing {replicaLabel(replacement.copy_index)} · {replacement.source.provider_id} →{' '}
              {replacement.target.provider_id}
            </>
          )}
        </span>
        <StatusBadge tone={replacement.retirement_attention ? 'danger' : replacementStatusTone(replacement.status)}>
          {replacement.retirement_attention ? 'Retirement stopped' : replacementStatusLabel(replacement.status)}
        </StatusBadge>
      </div>
      <ReplacementProgressView progress={replacement.progress} />
      {nextStep && <div className="text-sm text-muted-foreground">{nextStep}</div>}
      {replacement.last_error && (
        <div>
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={() => onOpenLastError(replacement.last_error ?? '')}
          >
            <Info data-icon="inline-start" />
            Error details
          </Button>
        </div>
      )}
      {replacement.retryable ? (
        <div>
          <RetryButton taskID={replacement.retry_task_id} />
        </div>
      ) : replacement.retry_unavailable_reason ? (
        <p className="text-sm text-muted-foreground">{replacement.retry_unavailable_reason}</p>
      ) : null}
    </div>
  )
}

function BucketStorageHealthPanel({
  health,
  onOpenLastError,
  onReviewVersions,
}: {
  health: BucketStorageHealthSummary
  onOpenLastError: (error: string) => void
  onReviewVersions: () => void
}) {
  const lastError = health.last_error
  const hasAffectedVersions = !lastError && health.affected_versions_capped > 0

  return (
    <Card size="sm">
      <CardHeader>
        <CardTitle>Storage health</CardTitle>
        <CardAction title={bucketStorageHealthTitle(health)}>
          <StatusBadge tone={bucketStorageHealthStatusTone(health)}>{bucketStorageHealthLabel(health)}</StatusBadge>
        </CardAction>
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
        <DetailGrid className="@xl:grid-cols-4">
          <DetailField label="Retained versions at risk">
            <span title="Capped list triage count, not a diagnostic total">
              {bucketStorageHealthAffectedVersionsLabel(health)}
            </span>
          </DetailField>
          <DetailField label="Data sets with issues">{formatNumber(health.abnormal_data_sets)}</DetailField>
          <DetailField label="Observation">{bucketStorageHealthObservationLabel(health)}</DetailField>
          <DetailField label="Checked">
            {health.last_checked_at ? <RelativeTime value={health.last_checked_at} /> : 'Not checked'}
          </DetailField>
        </DetailGrid>
        {(lastError || hasAffectedVersions) && (
          <div className="flex flex-wrap gap-2">
            {lastError && (
              <Button type="button" variant="outline" size="sm" onClick={() => onOpenLastError(lastError)}>
                <Info data-icon="inline-start" />
                Error details
              </Button>
            )}
            {hasAffectedVersions && (
              <Button type="button" variant="outline" size="sm" onClick={onReviewVersions}>
                <TriangleAlert data-icon="inline-start" />
                Review affected versions
              </Button>
            )}
          </div>
        )}
      </CardContent>
    </Card>
  )
}

function BucketStorageDataSets({
  bucketName,
  dataSets,
  onReviewStorageRisk,
  onReplaceProvider,
}: {
  bucketName: string
  dataSets: StorageDataSetSummary[]
  onReviewStorageRisk: (dataSetID: number) => void
  onReplaceProvider: (dataSet: StorageDataSetSummary) => void
}) {
  const refreshStorageHealth = useRefreshDataSetStorageHealth()
  const [refreshError, setRefreshError] = useState<string | null>(null)

  if (dataSets.length === 0) {
    return (
      <EmptyState
        icon={<Database />}
        title="No data sets yet"
        description="Data sets appear once this bucket stores its first replica with a provider."
      />
    )
  }

  return (
    <DataTableFrame>
      <div className="flex min-w-0 items-center justify-between gap-2 border-b border-border px-4 py-2">
        <h2 className="text-sm font-semibold">Data sets</h2>
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
      <div className="max-w-full min-w-0 overflow-x-auto overflow-y-hidden">
        <Table className="min-w-[860px] table-fixed">
          <TableHeader>
            <TableRow className={tableHeaderRowClassName}>
              <TableHead className="w-[10%] px-3">Replica</TableHead>
              <TableHead className="w-[18%] px-3">Provider</TableHead>
              <TableHead className="w-[12%] px-3">Data set</TableHead>
              <TableHead className="w-[19%] px-3">Storage health</TableHead>
              <TableHead className="w-[17%] px-3">Impact</TableHead>
              <TableHead
                className="w-[8%] px-2 text-right"
                aria-label="Current object versions referencing this data set"
              >
                Current
              </TableHead>
              <TableHead
                className="w-[9%] px-2 text-right"
                aria-label="Total non-delete-marker versions referencing this data set"
              >
                Total
              </TableHead>
              <TableHead className="w-[6%] px-2 text-right">
                <span className="sr-only">Actions</span>
              </TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {dataSets.map((dataSet) => {
              const topologyLink = bucketStorageDataSetTopologyLinkModel(bucketName, dataSet)
              const link = (
                <Link
                  to="/storage-topology"
                  search={topologyLink.search}
                  className="block min-w-0 max-w-full truncate font-mono text-xs hover:text-foreground hover:underline"
                >
                  {topologyLink.label}
                </Link>
              )

              return (
                <TableRow key={dataSet.id}>
                  <TableCell className="overflow-hidden px-3">
                    <span className="block max-w-full truncate font-medium">{replicaLabel(dataSet.copy_index)}</span>
                    <StatusBadge
                      tone={dataSetGenerationTone(dataSet)}
                      className="mt-1 max-w-full truncate"
                      title={dataSetGenerationLabel(dataSet)}
                    >
                      {dataSetGenerationLabel(dataSet)}
                    </StatusBadge>
                  </TableCell>
                  <TableCell className="overflow-hidden px-3">
                    <ProviderIdentityCell providerID={dataSet.provider_id} identity={dataSet.provider_identity} />
                  </TableCell>
                  <TableCell className="overflow-hidden px-3 text-muted-foreground">
                    {topologyLink.copyValue ? (
                      <CopyableValue label="Data set ID" value={topologyLink.copyValue} monospace maxLength={24}>
                        {link}
                      </CopyableValue>
                    ) : (
                      link
                    )}
                  </TableCell>
                  <TableCell className="overflow-hidden px-3">
                    <DataSetStorageHealthCell dataSet={dataSet} />
                    {dataSetSetupMessage(dataSet) && (
                      <p
                        className="mt-1 truncate text-xs text-muted-foreground"
                        title={dataSetSetupMessage(dataSet) ?? undefined}
                      >
                        {dataSetSetupMessage(dataSet)}
                      </p>
                    )}
                  </TableCell>
                  <TableCell className="overflow-hidden px-3">
                    <DataSetImpactCell dataSet={dataSet} />
                  </TableCell>
                  <TableCell
                    className="px-2 text-right text-muted-foreground"
                    aria-label={`Current object versions referencing this data set: ${formatNumber(dataSet.current_version_count)}`}
                    title="Current object versions referencing this data set"
                  >
                    {formatNumber(dataSet.current_version_count)}
                  </TableCell>
                  <TableCell
                    className="px-2 text-right text-muted-foreground"
                    aria-label={`Total non-delete-marker versions referencing this data set: ${formatNumber(dataSet.referenced_version_count)}`}
                    title="Total non-delete-marker versions referencing this data set"
                  >
                    {formatNumber(dataSet.referenced_version_count)}
                  </TableCell>
                  <TableCell className="px-2 text-right">
                    {(dataSetNeedsStorageRiskReview(dataSet) || dataSet.replaceable) && (
                      <RowActionsMenu label={replicaLabel(dataSet.copy_index)}>
                        {dataSetNeedsStorageRiskReview(dataSet) && (
                          <RowActionItem onSelect={() => onReviewStorageRisk(dataSet.id)}>
                            <TriangleAlert data-icon="inline-start" />
                            Review affected versions
                          </RowActionItem>
                        )}
                        {dataSet.replaceable && (
                          <RowActionItem onSelect={() => onReplaceProvider(dataSet)}>
                            <Repeat2 data-icon="inline-start" />
                            Replace provider
                          </RowActionItem>
                        )}
                      </RowActionsMenu>
                    )}
                  </TableCell>
                </TableRow>
              )
            })}
          </TableBody>
        </Table>
      </div>
    </DataTableFrame>
  )
}

function DataSetImpactCell({ dataSet }: { dataSet: StorageDataSetSummary }) {
  const label = dataSetStorageImpactLabel(dataSet)

  return (
    <StatusBadge tone={dataSetStorageImpactTone(dataSet)} className="max-w-full truncate" title={label}>
      {label}
    </StatusBadge>
  )
}

function DataSetStorageHealthCell({ dataSet }: { dataSet: StorageDataSetSummary }) {
  const storageHealth = dataSet.storage_health
  const details = dataSetStorageHealthDetailParts(dataSet).join(' · ')
  const status = storageHealth?.status ?? 'unknown'

  return (
    <div className="flex min-w-0 max-w-full flex-col gap-1 overflow-hidden">
      <div className="flex min-w-0 flex-wrap items-center gap-2">
        <StatusBadge tone={storageHealthStatusTone(status)}>{observabilityStatusLabel(status)}</StatusBadge>
        {storageHealth?.stale && (
          <StatusBadge tone="warning" className="shrink-0">
            Stale
          </StatusBadge>
        )}
      </div>
      <div className="min-w-0 truncate text-xs text-muted-foreground" title={details}>
        {details}
      </div>
    </div>
  )
}

function CopyHealthCell({ health }: { health: CopyHealthInfo }) {
  return (
    <span className="inline-flex" title={copyHealthInfoTitle(health)}>
      <StatusBadge tone={copyHealthStatusTone(health)} className="whitespace-nowrap">
        {copyHealthStatusLabel(health.status)}
      </StatusBadge>
    </span>
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

function BucketSettingsTab({ bucket }: { bucket: BucketDetail }) {
  const { data: users = [] } = useS3Users()
  const [ownerOpen, setOwnerOpen] = useState(false)
  const owner = bucket.owner_access_key

  return (
    <div className="flex min-w-0 max-w-3xl flex-col gap-6">
      <SettingsSection title="General">
        <DetailGrid>
          <DetailField label="Name">
            <CopyableValue label="Bucket" value={bucket.name} />
          </DetailField>
          <DetailField label="Status">
            <StatusBadge tone={bucketStatusTone(bucket.status)}>{bucketStatusLabel(bucket.status)}</StatusBadge>
          </DetailField>
          <DetailField label="Versioning">{versioningStatusLabel(bucket.versioning_status)}</DetailField>
          <DetailField label="Provider preference">
            {providerSelectionStrategyLabel(bucket.provider_selection_strategy)}
          </DetailField>
          <DetailField label="Created">
            <RelativeTime value={bucket.created_at} />
          </DetailField>
          <DetailField label="Updated">
            <RelativeTime value={bucket.updated_at} />
          </DetailField>
        </DetailGrid>
      </SettingsSection>
      <SettingsSection
        title="Owner"
        action={
          <Button variant="outline" size="sm" onClick={() => setOwnerOpen(true)}>
            <UserRound data-icon="inline-start" />
            {owner ? 'Change owner' : 'Assign owner'}
          </Button>
        }
      >
        <div className="flex flex-col gap-1 text-sm">
          {owner ? (
            <CopyableValue
              label="Owner"
              value={owner}
              displayValue={ownerLabel(owner, users)}
              maxLength={ownerLabel(owner, users).length}
            />
          ) : (
            <span className="text-muted-foreground">{ownerLabel(owner)}</span>
          )}
          <span className="text-muted-foreground">The owner has full control of this bucket through the S3 API.</span>
        </div>
      </SettingsSection>
      <ReplicaPolicySection bucket={bucket} />
      <BucketOwnerDialog bucketName={bucket.name} ownerAccessKey={owner} open={ownerOpen} onOpenChange={setOwnerOpen} />
    </div>
  )
}

function ReplicaPolicySection({ bucket }: { bucket: BucketDetail }) {
  const updateCopyPolicy = useUpdateBucketCopyPolicy()
  const currentCopyPolicy = bucketCopyPolicyValue(bucket)
  const currentMinimumDurableCopies = minimumDurableCopiesValue(bucket)
  const [copyPolicy, setCopyPolicy] = useState(currentCopyPolicy)
  const [minimumDurableCopies, setMinimumDurableCopies] = useState(currentMinimumDurableCopies)
  const [copyPolicyError, setCopyPolicyError] = useState<string | null>(null)
  const [justSaved, setJustSaved] = useState(false)
  const targetCopies = selectedTargetCopies(copyPolicy)
  const minimumOptions = minimumDurableCopiesOptions(targetCopies)

  useEffect(() => {
    setCopyPolicy(currentCopyPolicy)
    setMinimumDurableCopies(currentMinimumDurableCopies)
    setCopyPolicyError(null)
  }, [currentCopyPolicy, currentMinimumDurableCopies])

  const nextMinimumDurableCopies = persistMinimumDurableCopies(
    minimumDurableCopies,
    bucket.minimum_durable_copies,
    targetCopies
  )
  const copyPolicyChanged =
    (copyPolicy !== currentCopyPolicy ||
      minimumDurableCopies !== currentMinimumDurableCopies ||
      nextMinimumDurableCopies !== undefined) &&
    !justSaved
  const handleCopyPolicyChange = (next: string) => {
    const nextTarget = selectedTargetCopies(next)
    setCopyPolicy(next)
    // "All replicas" follows the target: a minimum that matched the old target
    // keeps matching the new one instead of quietly becoming a fixed count.
    setMinimumDurableCopies((current) =>
      Number(current) === Number(currentCopyPolicy) && nextTarget != null
        ? nextTarget.toString()
        : clampMinimumDurableCopiesValue(current, nextTarget)
    )
    setCopyPolicyError(null)
    setJustSaved(false)
  }
  const handleMinimumDurableCopiesChange = (next: string) => {
    setMinimumDurableCopies(next)
    setCopyPolicyError(null)
    setJustSaved(false)
  }
  const saveCopyPolicy = () => {
    setCopyPolicyError(null)
    updateCopyPolicy.mutate(
      {
        name: bucket.name,
        defaultCopies: copyPolicy === currentCopyPolicy ? undefined : Number(copyPolicy),
        minimumDurableCopies: nextMinimumDurableCopies,
      },
      {
        onSuccess: (savedBucket) => {
          setCopyPolicy(bucketCopyPolicyValue(savedBucket))
          setMinimumDurableCopies(minimumDurableCopiesValue(savedBucket))
          setJustSaved(true)
          toast.success(bucketCopyPolicySavedMessage())
        },
        onError: (mutationError) => {
          setCopyPolicyError(mutationError instanceof Error ? mutationError.message : 'Failed to update replica policy')
        },
      }
    )
  }

  return (
    <SettingsSection
      title="Replica policy"
      action={
        <Button size="sm" onClick={saveCopyPolicy} disabled={!copyPolicyChanged || updateCopyPolicy.isPending}>
          {updateCopyPolicy.isPending && <Loader2 data-icon="inline-start" className="animate-spin" />}
          Save
        </Button>
      }
    >
      <div className="flex max-w-xl flex-col gap-4">
        <FieldGroup>
          <Field>
            <FieldLabel htmlFor={`bucket-copies-${bucket.id}`}>Replicas</FieldLabel>
            <Select value={copyPolicy} onValueChange={handleCopyPolicyChange} disabled={updateCopyPolicy.isPending}>
              <SelectTrigger id={`bucket-copies-${bucket.id}`} className="w-full">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectGroup>
                  {copyPolicyOptions.map((copies) => (
                    <SelectItem
                      key={copies}
                      value={copies.toString()}
                      disabled={replicaTargetLocked(copies, bucket.default_copies)}
                    >
                      {replicaCountLabel(copies)}
                    </SelectItem>
                  ))}
                </SelectGroup>
              </SelectContent>
            </Select>
            <FieldDescription>{replicaTargetChoiceNote()}</FieldDescription>
            {bucket.default_copies > 1 && <FieldDescription>{replicaTargetLockNote()}</FieldDescription>}
          </Field>
          <Field>
            <FieldLabel htmlFor={`bucket-minimum-durable-copies-${bucket.id}`}>Release cache after</FieldLabel>
            <Select
              value={minimumDurableCopies}
              onValueChange={handleMinimumDurableCopiesChange}
              disabled={updateCopyPolicy.isPending}
            >
              <SelectTrigger id={`bucket-minimum-durable-copies-${bucket.id}`} className="w-full">
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
        </FieldGroup>
        {showsMinimumDurableCopiesWarning(minimumDurableCopies, targetCopies) && (
          <Alert>
            <AlertDescription>{minimumDurableCopiesWarning()}</AlertDescription>
          </Alert>
        )}
        {copyPolicyError && (
          <Alert variant="destructive">
            <AlertDescription>{copyPolicyError}</AlertDescription>
          </Alert>
        )}
      </div>
    </SettingsSection>
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
  hasPrevious,
  nextMarker,
  onPrevious,
  onNext,
  navigateToPrefix,
  onUpload,
}: {
  bucketName: string
  prefix: string
  folders: ObjectFolderItem[]
  files: ObjectItem[]
  hasPrevious: boolean
  nextMarker?: string
  onPrevious: () => void
  onNext: (marker: string) => void
  navigateToPrefix: (prefix: string) => void
  onUpload?: () => void
}) {
  if (folders.length === 0 && files.length === 0 && !hasPrevious) {
    return (
      <EmptyState
        icon={<Folder />}
        title={prefix ? 'This folder is empty' : 'No objects yet'}
        description="Upload files here or write them with any S3 client."
        action={
          onUpload && (
            <Button size="sm" variant="outline" onClick={onUpload}>
              <Upload data-icon="inline-start" />
              Upload files
            </Button>
          )
        }
      />
    )
  }

  return (
    <div className="flex flex-col gap-3">
      <DataTableFrame>
        <Table className="min-w-[780px]">
          <TableHeader>
            <TableRow className={tableHeaderRowClassName}>
              <TableHead className="w-[38%] px-4">Name</TableHead>
              <TableHead className="w-[10%] px-4 text-right">Size</TableHead>
              <TableHead className="w-[16%] px-4">Location</TableHead>
              <TableHead className="w-[18%] px-4">Type</TableHead>
              <TableHead className="w-[12%] px-4">Updated</TableHead>
              <TableHead className="w-[6%] px-4 text-right">
                <span className="sr-only">Actions</span>
              </TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {folders.map((folder) => (
              <TableRow key={folder.prefix} {...clickableRowProps(() => navigateToPrefix(folder.prefix))}>
                <TableCell className="px-4">
                  <button
                    type="button"
                    className="inline-flex max-w-full items-center gap-1.5 font-medium hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                    onClick={() => navigateToPrefix(folder.prefix)}
                  >
                    <Folder className="size-4 shrink-0 text-status-info" />
                    <span className="truncate">{folder.name}</span>
                  </button>
                </TableCell>
                <TableCell className="px-4 text-right text-muted-foreground">—</TableCell>
                <TableCell className="px-4 text-muted-foreground">—</TableCell>
                <TableCell className="px-4 text-muted-foreground">Folder</TableCell>
                <TableCell className="px-4 text-muted-foreground">—</TableCell>
                <TableCell className="px-4" />
              </TableRow>
            ))}
            {files.map((object) => (
              <TableRow key={object.id}>
                <TableCell className="px-4">
                  <div className="flex min-w-0 items-center gap-1.5">
                    <FileIcon className="size-4 shrink-0 text-muted-foreground" />
                    <CopyableValue
                      label="Object key"
                      value={object.key}
                      displayValue={objectDisplayName(object.key, prefix)}
                      maxLength={36}
                    />
                    <ObjectStatusIcon
                      bucketName={bucketName}
                      versionID={object.current_version_id}
                      state={object.state}
                      status={object.status}
                      progress={object.progress}
                      compact
                    />
                  </div>
                </TableCell>
                <TableCell className="px-4 text-right tabular-nums">{formatBytes(object.size)}</TableCell>
                <TableCell className="px-4">
                  <LocationBadges location={object.location} />
                </TableCell>
                <TableCell className="px-4 text-muted-foreground">
                  <span className="block max-w-48 truncate" title={object.content_type}>
                    {object.content_type || '—'}
                  </span>
                </TableCell>
                <TableCell className="px-4 text-muted-foreground">
                  <RelativeTime value={object.updated_at} />
                </TableCell>
                <TableCell className="px-4 text-right">
                  <ObjectActions bucketName={bucketName} object={object} />
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </DataTableFrame>
      <CursorPagination
        summary={formatBrowserCount(folders.length, files.length)}
        hasPrevious={hasPrevious}
        hasNext={Boolean(nextMarker)}
        onPrevious={onPrevious}
        onNext={() => nextMarker && onNext(nextMarker)}
      />
    </div>
  )
}

function DeletedObjectsTable({
  bucketName,
  prefix,
  objects,
  hasPrevious,
  nextMarker,
  onPrevious,
  onNext,
}: {
  bucketName: string
  prefix: string
  objects: DeletedObjectItem[]
  hasPrevious: boolean
  nextMarker?: string
  onPrevious: () => void
  onNext: (marker: string) => void
}) {
  if (objects.length === 0 && !hasPrevious) {
    return (
      <EmptyState
        icon={<Trash2 />}
        title="Trash is empty"
        description="Deleted objects stay here, ready to restore, until they are permanently deleted."
      />
    )
  }

  return (
    <div className="flex flex-col gap-3">
      <DataTableFrame>
        <Table className="min-w-[860px]">
          <TableHeader>
            <TableRow className={tableHeaderRowClassName}>
              <TableHead className="w-[34%] px-4">Name</TableHead>
              <TableHead className="w-[22%] px-4">Restore version</TableHead>
              <TableHead className="w-[10%] px-4 text-right">Size</TableHead>
              <TableHead className="w-[16%] px-4">Type</TableHead>
              <TableHead className="w-[12%] px-4">Deleted</TableHead>
              <TableHead className="w-[6%] px-4 text-right">
                <span className="sr-only">Actions</span>
              </TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {objects.map((object) => (
              <TableRow key={object.delete_marker_version_id}>
                <TableCell className="px-4">
                  <div className="flex min-w-0 items-center gap-1.5">
                    <FileIcon className="size-4 shrink-0 text-muted-foreground" />
                    <CopyableValue
                      label="Object key"
                      value={object.key}
                      displayValue={objectDisplayName(object.key, prefix)}
                      maxLength={36}
                    />
                  </div>
                </TableCell>
                <TableCell className="overflow-hidden px-4">
                  <CopyableValue label="Restore version" value={object.restore_version_id} monospace maxLength={24} />
                </TableCell>
                <TableCell className="px-4 text-right tabular-nums">{formatBytes(object.restore_size)}</TableCell>
                <TableCell className="px-4 text-muted-foreground">
                  <span className="block max-w-48 truncate" title={object.restore_content_type}>
                    {object.restore_content_type || '—'}
                  </span>
                </TableCell>
                <TableCell className="px-4 text-muted-foreground">
                  <RelativeTime value={object.deleted_at} />
                </TableCell>
                <TableCell className="px-4 text-right">
                  <DeletedObjectActions bucketName={bucketName} object={object} />
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </DataTableFrame>
      <CursorPagination
        summary={formatCountLabel(objects.length, 'item')}
        hasPrevious={hasPrevious}
        hasNext={Boolean(nextMarker)}
        onPrevious={onPrevious}
        onNext={() => nextMarker && onNext(nextMarker)}
      />
    </div>
  )
}

function DeletedObjectActions({ bucketName, object }: { bucketName: string; object: DeletedObjectItem }) {
  const [restoreOpen, setRestoreOpen] = useState(false)
  const [versionsOpen, setVersionsOpen] = useState(false)
  const [permanentDeleteOpen, setPermanentDeleteOpen] = useState(false)

  return (
    <>
      <RowActionsMenu label={object.key}>
        <RowActionItem onSelect={() => setRestoreOpen(true)}>
          <RotateCcw data-icon="inline-start" />
          Restore
        </RowActionItem>
        <RowActionItem onSelect={() => setVersionsOpen(true)}>
          <History data-icon="inline-start" />
          Versions
        </RowActionItem>
        <DropdownMenuSeparator />
        <RowActionItem variant="destructive" onSelect={() => setPermanentDeleteOpen(true)}>
          <Trash2 data-icon="inline-start" />
          Permanently delete
        </RowActionItem>
      </RowActionsMenu>
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
          toast.success(`Permanently deleted ${object.key}`)
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
          { id: 'key', label: 'Object', value: object.key, copyable: true, maxLength: 36 },
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
          toast.success(`Restored ${object.key}`)
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
            { id: 'key', label: 'Object', value: object.key, copyable: true, maxLength: 36 },
            { id: 'marker', label: 'Deletion record', value: object.delete_marker_version_id, copyable: true },
            { id: 'target', label: 'Restore version', value: object.restore_version_id, copyable: true },
            { id: 'size', label: 'Size', value: formatBytes(object.restore_size) },
          ]}
        />
        {restoreObject.error && (
          <Alert variant="destructive">
            <AlertDescription>{restoreObject.error.message}</AlertDescription>
          </Alert>
        )}
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
          toast.success(`Moved ${object.key} to Trash`)
          setDeleteOpen(false)
          deleteObject.reset()
        },
      }
    )
  }

  return (
    <>
      <RowActionsMenu label={object.key}>
        <RowActionItem asChild>
          <a href={api.getObjectDownloadUrl(bucketName, object.key)} aria-label={`Download ${object.key}`}>
            <Download data-icon="inline-start" />
            Download
          </a>
        </RowActionItem>
        <RowActionItem onSelect={() => setVersionsOpen(true)}>
          <History data-icon="inline-start" />
          Versions
        </RowActionItem>
        <RowActionItem onSelect={() => setProvenanceOpen(true)}>
          <Fingerprint data-icon="inline-start" />
          Provenance
        </RowActionItem>
        <DropdownMenuSeparator />
        <RowActionItem variant="destructive" onSelect={() => setDeleteOpen(true)}>
          <Trash2 data-icon="inline-start" />
          Delete
        </RowActionItem>
      </RowActionsMenu>
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
      >
        <ReviewDetails
          rows={[
            { id: 'key', label: 'Object', value: object.key, copyable: true, maxLength: 36 },
            { id: 'size', label: 'Size', value: formatBytes(object.size) },
          ]}
        />
      </DangerActionAlertDialog>
    </>
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
