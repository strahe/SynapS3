import { useMutation, useQueryClient } from '@tanstack/react-query'
import { createFileRoute, Link, useNavigate } from '@tanstack/react-router'
import { Layers, Loader2 } from 'lucide-react'
import { useEffect, useRef, useState } from 'react'
import { toast } from 'sonner'

import { api, type CommitBatch, type CommitBatchDetails, type CommitBatchStatus } from '@/api/client'
import { CopyableValue } from '@/components/app/CopyableValue'
import { CursorPagination } from '@/components/app/CursorPagination'
import {
  clickableRowProps,
  DataTableFrame,
  RowDetailsButton,
  TableSkeleton,
  tableHeaderRowClassName,
} from '@/components/app/DataTable'
import { DetailBody, DetailField, DetailGrid, DetailHeader, DetailSection } from '@/components/app/DetailPanel'
import { PageHeader, RefreshButton } from '@/components/app/PageHeader'
import { EmptyState, PageError } from '@/components/app/PageState'
import { RelativeTime } from '@/components/app/RelativeTime'
import { StatusBadge, type StatusTone } from '@/components/app/StatusBadge'
import { RetryButton } from '@/components/tasks/RetryButton'
import { StorageConfirmationDetails } from '@/components/tasks/StorageConfirmationDetails'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Sheet, SheetContent } from '@/components/ui/sheet'
import { Skeleton } from '@/components/ui/skeleton'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { useCommitBatch, useCommitBatches } from '@/hooks/queries'
import { formatBytes } from '@/lib/utils'

type StatusFilter = CommitBatchStatus | 'all'

const statuses: { value: StatusFilter; label: string; tone: StatusTone }[] = [
  { value: 'all', label: 'All statuses', tone: 'neutral' },
  { value: 'collecting', label: 'Collecting data', tone: 'neutral' },
  { value: 'ready', label: 'Ready to submit', tone: 'info' },
  { value: 'submitted', label: 'Awaiting confirmation', tone: 'info' },
  { value: 'confirmed', label: 'Completed', tone: 'success' },
  { value: 'abandoned', label: 'Stopped', tone: 'warning' },
]
const statusValues = new Set<string>(statuses.map((entry) => entry.value))

export const Route = createFileRoute('/commit-batches')({
  validateSearch: (search: Record<string, unknown>): { status?: StatusFilter } => ({
    status:
      typeof search.status === 'string' && statusValues.has(search.status) && search.status !== 'collecting'
        ? (search.status as StatusFilter)
        : undefined,
  }),
  component: CommitBatchesPage,
})

function BatchStatus({ batch }: { batch: CommitBatch }) {
  if (batch.task_status === 'failed') return <StatusBadge tone="danger">Needs attention</StatusBadge>
  if (batch.seal_requested_at) return <StatusBadge tone="info">Submission requested</StatusBadge>
  const status = statuses.find((entry) => entry.value === batch.status)
  return <StatusBadge tone={status?.tone}>{status?.label ?? batch.status}</StatusBadge>
}

function CommitBatchesPage() {
  const search = Route.useSearch()
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const detailTitle = useRef<HTMLHeadingElement>(null)
  const status: StatusFilter = search.status ?? 'collecting'
  const [pagination, setPagination] = useState<{ status: StatusFilter; cursors: string[] }>({ status, cursors: [] })
  const cursors = pagination.status === status ? pagination.cursors : []
  const cursor = cursors[cursors.length - 1]
  const [selected, setSelected] = useState<string | null>(null)
  const batches = useCommitBatches(status === 'all' ? undefined : status, cursor)
  const detail = useCommitBatch(selected)
  useEffect(() => {
    setPagination((current) => (current.status === status ? current : { status, cursors: [] }))
  }, [status])
  const refresh = async () => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: ['commitBatches'] }),
      queryClient.invalidateQueries({ queryKey: ['commitBatch'] }),
      queryClient.invalidateQueries({ queryKey: ['tasks'] }),
    ])
  }
  const seal = useMutation({
    mutationFn: api.sealCommitBatch,
    onSuccess: (result) => {
      toast.success('Submission requested', { description: `Batch ${result.request_id}` })
      void refresh()
    },
    onError: (error) => {
      toast.error("Couldn't submit the batch", { description: error.message })
      void refresh()
    },
  })
  const sealButton = (batch: CommitBatch) =>
    batch.can_seal && (
      <Button variant="outline" size="sm" disabled={seal.isPending} onClick={() => seal.mutate(batch.request_id)}>
        {seal.isPending && seal.variables === batch.request_id && (
          <Loader2 className="animate-spin" data-icon="inline-start" />
        )}
        Submit next batch
      </Button>
    )

  const changeStatus = (next: StatusFilter) => {
    navigate({ to: '/commit-batches', search: { status: next === 'collecting' ? undefined : next }, replace: true })
  }

  return (
    <div className="flex flex-col gap-6 p-6">
      <PageHeader
        title="Batches"
        description="Uploaded pieces collect into batches that are submitted to each provider in one transaction."
        actions={<RefreshButton onClick={() => void refresh()} refreshing={batches.isFetching} />}
      />
      <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
        <div className="flex items-center gap-2">
          <Label htmlFor="batch-status" className="text-sm text-muted-foreground">
            Status
          </Label>
          <Select value={status} onValueChange={(next) => changeStatus(next as StatusFilter)}>
            <SelectTrigger id="batch-status" className="w-56">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectGroup>
                {statuses.map((entry) => (
                  <SelectItem key={entry.value} value={entry.value}>
                    {entry.label}
                  </SelectItem>
                ))}
              </SelectGroup>
            </SelectContent>
          </Select>
        </div>
      </div>
      {batches.data?.batches.some((batch) => batch.can_seal) && (
        <Alert>
          <AlertDescription>
            Batches submit on their own when full or after the configured wait. Submit next batch sends one now; the
            remaining pieces keep collecting.
          </AlertDescription>
        </Alert>
      )}
      {batches.isError ? (
        <PageError
          title="Failed to load batches"
          description={batches.error.message}
          onRetry={() => batches.refetch()}
          retrying={batches.isFetching}
        />
      ) : batches.isPending ? (
        <TableSkeleton />
      ) : batches.data.batches.length === 0 ? (
        <EmptyState
          icon={<Layers />}
          title="No batches in this view"
          description="Choose another status to see other batches."
        />
      ) : (
        <DataTableFrame>
          <Table className="min-w-[960px]">
            <TableHeader>
              <TableRow className={tableHeaderRowClassName}>
                <TableHead className="px-4">Batch</TableHead>
                <TableHead className="px-4">Bucket</TableHead>
                <TableHead className="px-4">Provider</TableHead>
                <TableHead className="px-4 text-right">Pieces</TableHead>
                <TableHead className="px-4 text-right">Size</TableHead>
                <TableHead className="px-4">Waiting since</TableHead>
                <TableHead className="px-4">Submitted</TableHead>
                <TableHead className="px-4">Status</TableHead>
                <TableHead className="px-4 text-right">
                  <span className="sr-only">Actions</span>
                </TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {batches.data.batches.map((batch) => (
                <TableRow key={batch.request_id} {...clickableRowProps(() => setSelected(batch.request_id))}>
                  <TableCell className="px-4">
                    <CopyableValue value={batch.request_id} label="Batch ID" monospace maxLength={16} />
                  </TableCell>
                  <TableCell className="px-4">
                    <Link
                      to="/buckets/$name"
                      params={{ name: batch.bucket_name }}
                      className="font-medium hover:underline"
                    >
                      {batch.bucket_name}
                    </Link>
                  </TableCell>
                  <TableCell className="px-4">{batch.provider_name ?? batch.provider_id}</TableCell>
                  <TableCell className="px-4 text-right tabular-nums">{batch.member_count}</TableCell>
                  <TableCell className="px-4 text-right tabular-nums">
                    {batch.total_bytes === null ? '—' : formatBytes(batch.total_bytes)}
                  </TableCell>
                  <TableCell className="px-4 text-muted-foreground">
                    {batch.status === 'collecting' ? <RelativeTime value={batch.oldest_ready_at} /> : '—'}
                  </TableCell>
                  <TableCell className="px-4 text-muted-foreground">
                    <RelativeTime value={batch.submitted_at} />
                  </TableCell>
                  <TableCell className="px-4">
                    <BatchStatus batch={batch} />
                  </TableCell>
                  <TableCell className="px-4">
                    <div className="flex items-center justify-end gap-2">
                      {sealButton(batch)}
                      <RowDetailsButton
                        label={`batch ${batch.request_id}`}
                        onClick={() => setSelected(batch.request_id)}
                      />
                    </div>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </DataTableFrame>
      )}
      <CursorPagination
        hasPrevious={cursors.length > 0}
        hasNext={Boolean(batches.data?.next_cursor)}
        onPrevious={() => {
          setPagination({ status, cursors: cursors.slice(0, -1) })
        }}
        onNext={() => {
          const next = batches.data?.next_cursor
          if (next) setPagination({ status, cursors: [...cursors, next] })
        }}
      />
      <Sheet
        open={selected !== null}
        onOpenChange={(open) => {
          if (!open) setSelected(null)
        }}
      >
        <SheetContent
          side="right"
          className="gap-0 data-[side=right]:w-[min(720px,calc(100vw-2rem))] data-[side=right]:sm:max-w-[720px]"
          onOpenAutoFocus={(event) => {
            event.preventDefault()
            detailTitle.current?.focus({ preventScroll: true })
          }}
        >
          <DetailHeader
            titleRef={detailTitle}
            kind="Batch"
            title={
              detail.data
                ? `${detail.data.bucket_name} → ${detail.data.provider_name ?? detail.data.provider_id}`
                : 'Batch'
            }
            badge={detail.data && <BatchStatus batch={detail.data} />}
            subtitle={
              detail.data && `${detail.data.member_count} ${detail.data.member_count === 1 ? 'piece' : 'pieces'}`
            }
            actions={
              detail.data &&
              (detail.data.can_seal || detail.data.task?.retryable) && (
                <>
                  {sealButton(detail.data)}
                  {detail.data.task?.retryable && <RetryButton taskID={detail.data.task.retry_task_id} />}
                </>
              )
            }
          />
          <DetailBody>
            {detail.isError ? (
              <PageError
                title="Failed to load this batch"
                description={detail.error.message}
                onRetry={() => detail.refetch()}
                retrying={detail.isFetching}
              />
            ) : detail.isPending ? (
              <Skeleton className="h-48 w-full" />
            ) : (
              <BatchDetails batch={detail.data} />
            )}
          </DetailBody>
        </SheetContent>
      </Sheet>
    </div>
  )
}

function BatchDetails({ batch }: { batch: CommitBatchDetails }) {
  return (
    <>
      <DetailSection title="Overview">
        <DetailGrid>
          <DetailField label="Batch ID">
            <CopyableValue label="Batch ID" value={batch.request_id} monospace maxLength={24} />
          </DetailField>
          <DetailField label="Bucket">
            <Link to="/buckets/$name" params={{ name: batch.bucket_name }} className="font-medium hover:underline">
              {batch.bucket_name}
            </Link>
          </DetailField>
          <DetailField label="Provider">
            <CopyableValue
              label="Provider"
              value={batch.provider_id}
              displayValue={batch.provider_name ?? batch.provider_id}
              maxLength={28}
            />
          </DetailField>
          {batch.data_set_id !== null && (
            <DetailField label="Data set">
              <CopyableValue label="Data set ID" value={batch.data_set_id} monospace />
            </DetailField>
          )}
          {batch.task_id !== null && (
            <DetailField label="Task">
              <CopyableValue label="Task ID" value={String(batch.task_id)} monospace />
            </DetailField>
          )}
          {batch.transaction_id && (
            <DetailField label="Transaction">
              <CopyableValue label="Transaction" value={batch.transaction_id} monospace maxLength={32} />
            </DetailField>
          )}
        </DetailGrid>
      </DetailSection>
      <BatchMembers members={batch.members} />
      <DetailSection title="Submission">
        <DetailGrid>
          {batch.seal_requested_at && (
            <DetailField label="Requested">
              <BatchTimestamp value={batch.seal_requested_at} />
            </DetailField>
          )}
          {batch.submitted_at && (
            <DetailField label="Submitted">
              <BatchTimestamp value={batch.submitted_at} />
            </DetailField>
          )}
          {batch.confirmed_at && (
            <DetailField label="Confirmed">
              <BatchTimestamp value={batch.confirmed_at} />
            </DetailField>
          )}
        </DetailGrid>
        {batch.status_message && <p className="text-sm text-muted-foreground">{batch.status_message}</p>}
        {batch.last_error && (
          <Alert variant="destructive">
            <AlertTitle>Batch needs attention</AlertTitle>
            <AlertDescription>
              <CopyableValue
                label="Batch error"
                value={batch.last_error}
                displayValue={batch.last_error}
                maxLength={120}
              />
            </AlertDescription>
          </Alert>
        )}
        {batch.task?.storage_confirmation && (
          <StorageConfirmationDetails confirmation={batch.task.storage_confirmation} />
        )}
        {batch.can_seal && (
          <p className="text-xs text-muted-foreground">
            Smaller batches may increase transaction costs. Confirmation is required before cached data can be removed.
          </p>
        )}
      </DetailSection>
    </>
  )
}

function BatchMembers({ members }: { members: CommitBatchDetails['members'] }) {
  return (
    <DetailSection title="Pieces">
      {members.length === 0 ? (
        <p className="text-sm text-muted-foreground">No pieces in this batch.</p>
      ) : (
        <DataTableFrame>
          <Table>
            <TableHeader>
              <TableRow className={tableHeaderRowClassName}>
                <TableHead className="px-3">Object</TableHead>
                <TableHead className="px-3 text-right">Size</TableHead>
                <TableHead className="px-3">Piece CID</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {members.map((member) => (
                <TableRow key={member.content_id}>
                  <TableCell className="px-3">
                    {member.file ? (
                      <div className="flex flex-col gap-1">
                        <CopyableValue label="Object key" value={member.file.key} maxLength={24} />
                        {(member.file.source !== 'current' || member.file.other_versions > 0) && (
                          <span className="text-xs text-muted-foreground">
                            {member.file.source === 'deleted'
                              ? 'Deleted object'
                              : member.file.source === 'historical'
                                ? 'Previous version'
                                : null}
                            {member.file.other_versions > 0 && (
                              <>
                                {member.file.source !== 'current' && ' · '}
                                {member.file.other_versions} other{' '}
                                {member.file.other_versions === 1 ? 'version' : 'versions'}
                              </>
                            )}
                          </span>
                        )}
                      </div>
                    ) : (
                      <span className="text-muted-foreground">Object unavailable</span>
                    )}
                  </TableCell>
                  <TableCell className="px-3 text-right tabular-nums">
                    {member.size === null ? '—' : formatBytes(member.size)}
                  </TableCell>
                  <TableCell className="px-3">
                    <CopyableValue label="Piece CID" value={member.piece_cid} monospace maxLength={20} />
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </DataTableFrame>
      )}
    </DetailSection>
  )
}

function BatchTimestamp({ value }: { value: string }) {
  return <time dateTime={value}>{new Date(value).toLocaleString()}</time>
}
