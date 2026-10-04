import { useMutation, useQueryClient } from '@tanstack/react-query'
import { createFileRoute } from '@tanstack/react-router'
import { ChevronLeft, ChevronRight, Eye, Layers, Loader2, RefreshCw, RotateCcw } from 'lucide-react'
import { type ReactNode, useRef, useState } from 'react'

import { api, type CommitBatch, type CommitBatchStatus } from '@/api/client'
import { CopyableValue } from '@/components/app/CopyableValue'
import { PageErrorState } from '@/components/app/PageErrorState'
import { PageHeader } from '@/components/app/PageHeader'
import { StatusBadge, type StatusTone } from '@/components/app/StatusBadge'
import { StorageConfirmationDetails } from '@/components/tasks/StorageConfirmationDetails'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from '@/components/ui/empty'
import { Label } from '@/components/ui/label'
import { Pagination, PaginationContent, PaginationItem } from '@/components/ui/pagination'
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from '@/components/ui/sheet'
import { Skeleton } from '@/components/ui/skeleton'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { useCommitBatch, useCommitBatches } from '@/hooks/queries'
import { cn, formatBytes, timeAgo } from '@/lib/utils'

export const Route = createFileRoute('/commit-batches')({ component: CommitBatchesPage })

const statuses: { value: CommitBatchStatus | 'all'; label: string; tone: StatusTone }[] = [
  { value: 'collecting', label: 'Waiting to submit', tone: 'neutral' },
  { value: 'ready', label: 'Queued', tone: 'info' },
  { value: 'submitted', label: 'Awaiting confirmation', tone: 'info' },
  { value: 'confirmed', label: 'Completed', tone: 'success' },
  { value: 'abandoned', label: 'Stopped', tone: 'warning' },
  { value: 'all', label: 'All statuses', tone: 'neutral' },
]

function BatchStatus({ batch }: { batch: CommitBatch }) {
  if (batch.task_status === 'failed') return <StatusBadge tone="danger">Needs attention</StatusBadge>
  if (batch.seal_requested_at) return <StatusBadge tone="info">Submission requested</StatusBadge>
  const status = statuses.find((entry) => entry.value === batch.status)
  return <StatusBadge tone={status?.tone}>{status?.label ?? batch.status}</StatusBadge>
}

function remainingTime(deadline: string | null) {
  if (!deadline) return '—'
  const seconds = Math.max(0, Math.ceil((new Date(deadline).getTime() - Date.now()) / 1000))
  if (seconds === 0) return 'Due now'
  return seconds < 60 ? `${seconds}s` : `${Math.ceil(seconds / 60)}m`
}

function CommitBatchesPage() {
  const queryClient = useQueryClient()
  const detailTitle = useRef<HTMLHeadingElement>(null)
  const [status, setStatus] = useState<CommitBatchStatus | 'all'>('collecting')
  const [cursor, setCursor] = useState<string>()
  const [history, setHistory] = useState<Array<string | undefined>>([])
  const [selected, setSelected] = useState<string | null>(null)
  const batches = useCommitBatches(status === 'all' ? undefined : status, cursor)
  const detail = useCommitBatch(selected)
  const refresh = async () => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: ['commitBatches'] }),
      queryClient.invalidateQueries({ queryKey: ['commitBatch'] }),
      queryClient.invalidateQueries({ queryKey: ['tasks'] }),
    ])
  }
  const seal = useMutation({
    mutationFn: api.sealCommitBatch,
    onSuccess: async (batch) => {
      queryClient.setQueryData(['commitBatch', batch.request_id], batch)
      await refresh()
    },
    onError: () => {
      void refresh()
    },
  })
  const recover = useMutation({ mutationFn: api.retryTask, onSuccess: refresh })
  const requestSeal = (batch: CommitBatch) => {
    setSelected(batch.request_id)
    seal.reset()
    seal.mutate(batch.request_id)
  }
  const sealButton = (batch: CommitBatch) =>
    batch.can_seal && (
      <Button variant="outline" size="sm" disabled={seal.isPending} onClick={() => requestSeal(batch)}>
        {seal.isPending && seal.variables === batch.request_id && (
          <Loader2 className="animate-spin" data-icon="inline-start" />
        )}
        Submit batch
      </Button>
    )

  return (
    <div className="flex flex-col gap-6 p-6">
      <PageHeader
        title="Batches"
        actions={
          <Button variant="outline" size="sm" onClick={() => void refresh()} disabled={batches.isFetching}>
            <RefreshCw data-icon="inline-start" className={cn(batches.isFetching && 'animate-spin')} />
            Refresh
          </Button>
        }
      />
      <div className="flex flex-wrap items-end gap-3">
        <div className="flex flex-col gap-1.5">
          <Label htmlFor="batch-status">Status</Label>
          <Select
            value={status}
            onValueChange={(next) => {
              setStatus(next as typeof status)
              setCursor(undefined)
              setHistory([])
            }}
          >
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
            Submit a batch before it fills or its wait ends. Smaller batches may increase transaction costs.
          </AlertDescription>
        </Alert>
      )}
      {batches.isError ? (
        <PageErrorState title="Couldn't load batches" description="Refresh to try again." />
      ) : batches.isPending ? (
        <BatchTableSkeleton />
      ) : batches.data.batches.length === 0 ? (
        <Empty>
          <EmptyHeader>
            <EmptyMedia variant="icon">
              <Layers />
            </EmptyMedia>
            <EmptyTitle>No batches in this view</EmptyTitle>
            <EmptyDescription>Choose another status to see other batches.</EmptyDescription>
          </EmptyHeader>
        </Empty>
      ) : (
        <div className="overflow-hidden rounded-lg border border-border">
          <Table>
            <TableHeader>
              <TableRow className="bg-muted/50">
                <TableHead className="px-4">Batch</TableHead>
                <TableHead className="px-4">Bucket</TableHead>
                <TableHead className="px-4">Storage service</TableHead>
                <TableHead className="px-4 text-right">Members</TableHead>
                <TableHead className="px-4 text-right">Size</TableHead>
                <TableHead className="px-4">Oldest wait</TableHead>
                <TableHead className="px-4">Wait remaining</TableHead>
                <TableHead className="px-4">Status</TableHead>
                <TableHead className="px-4 text-right">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {batches.data.batches.map((batch) => (
                <TableRow key={batch.request_id}>
                  <TableCell className="px-4">
                    <CopyableValue value={batch.request_id} label="Batch ID" monospace maxLength={16} />
                  </TableCell>
                  <TableCell className="px-4 font-medium">{batch.bucket_name}</TableCell>
                  <TableCell className="px-4">{batch.provider_name ?? batch.provider_id}</TableCell>
                  <TableCell className="px-4 text-right tabular-nums">
                    {batch.member_count} / {batch.max_pieces}
                  </TableCell>
                  <TableCell className="px-4 text-right tabular-nums">
                    {batch.total_bytes === null ? '—' : formatBytes(batch.total_bytes)}
                  </TableCell>
                  <TableCell className="px-4 text-muted-foreground">
                    {batch.status === 'collecting' && batch.oldest_ready_at ? timeAgo(batch.oldest_ready_at) : '—'}
                  </TableCell>
                  <TableCell className="px-4 tabular-nums text-muted-foreground">
                    {remainingTime(batch.collection_deadline)}
                  </TableCell>
                  <TableCell className="px-4">
                    <BatchStatus batch={batch} />
                  </TableCell>
                  <TableCell className="px-4">
                    <div className="flex justify-end gap-2">
                      <Button
                        variant="outline"
                        size="sm"
                        aria-label={`Details for batch ${batch.request_id}`}
                        onClick={() => {
                          setSelected(batch.request_id)
                          seal.reset()
                          recover.reset()
                        }}
                      >
                        <Eye data-icon="inline-start" />
                        Details
                      </Button>
                      {sealButton(batch)}
                    </div>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}
      {(history.length > 0 || batches.data?.next_cursor) && (
        <Pagination>
          <PaginationContent>
            <PaginationItem>
              <Button
                variant="outline"
                size="sm"
                disabled={history.length === 0}
                onClick={() => {
                  setCursor(history[history.length - 1])
                  setHistory(history.slice(0, -1))
                }}
              >
                <ChevronLeft data-icon="inline-start" />
                Previous
              </Button>
            </PaginationItem>
            <PaginationItem>
              <Button
                variant="outline"
                size="sm"
                disabled={!batches.data?.next_cursor}
                onClick={() => {
                  setHistory([...history, cursor])
                  setCursor(batches.data?.next_cursor)
                }}
              >
                Next
                <ChevronRight data-icon="inline-end" />
              </Button>
            </PaginationItem>
          </PaginationContent>
        </Pagination>
      )}
      <Sheet
        open={selected !== null}
        onOpenChange={(open) => {
          if (!open) {
            setSelected(null)
            seal.reset()
            recover.reset()
          }
        }}
      >
        <SheetContent
          className="min-w-0 overflow-hidden !w-[min(32rem,calc(100vw-2rem))] !max-w-[calc(100vw-2rem)]"
          onOpenAutoFocus={(event) => {
            event.preventDefault()
            detailTitle.current?.focus({ preventScroll: true })
          }}
        >
          <SheetHeader>
            <SheetTitle ref={detailTitle} tabIndex={-1} className="outline-none">
              Batch details
            </SheetTitle>
            <SheetDescription>Track batch submission and confirmation.</SheetDescription>
            {detail.data && (
              <div className="pr-8 text-muted-foreground">
                <CopyableValue label="Batch ID" value={detail.data.request_id} monospace />
              </div>
            )}
          </SheetHeader>
          <div className="min-h-0 min-w-0 flex-1 overflow-y-auto overflow-x-hidden">
            <div className="flex min-w-0 max-w-full flex-col gap-6 px-4 pb-4">
              {detail.isError ? (
                <PageErrorState title="Couldn't load this batch" description="Refresh to try again." />
              ) : detail.isPending ? (
                <Skeleton className="h-48 w-full" />
              ) : (
                detail.data && (
                  <>
                    <section className="flex min-w-0 max-w-full flex-col gap-3">
                      <h3 className="text-sm font-medium">Overview</h3>
                      <dl className="grid min-w-0 gap-x-6 gap-y-4 text-sm sm:grid-cols-2">
                        <BatchDetailField label="Bucket">
                          <CopyableValue label="Bucket" value={detail.data.bucket_name} maxLength={28} />
                        </BatchDetailField>
                        <BatchDetailField label="Storage service">
                          <CopyableValue
                            label="Storage service"
                            value={detail.data.provider_id}
                            displayValue={detail.data.provider_name ?? detail.data.provider_id}
                            maxLength={28}
                          />
                        </BatchDetailField>
                        <BatchDetailField label="Data set">
                          {detail.data.data_set_id === null ? (
                            '—'
                          ) : (
                            <CopyableValue label="Data set ID" value={detail.data.data_set_id} monospace />
                          )}
                        </BatchDetailField>
                        <BatchDetailField label="Members">
                          {detail.data.member_count} / {detail.data.max_pieces}
                        </BatchDetailField>
                        <BatchDetailField label="Size">
                          {detail.data.total_bytes === null ? '—' : formatBytes(detail.data.total_bytes)}
                        </BatchDetailField>
                        <BatchDetailField label="Status">
                          <BatchStatus batch={detail.data} />
                        </BatchDetailField>
                      </dl>
                    </section>
                    <section className="flex min-w-0 max-w-full flex-col gap-3">
                      <h3 className="text-sm font-medium">Progress</h3>
                      <dl className="grid min-w-0 gap-x-6 gap-y-4 text-sm sm:grid-cols-2">
                        <BatchDetailField label="Wait remaining">
                          {remainingTime(detail.data.collection_deadline)}
                        </BatchDetailField>
                        <BatchDetailField label="Task">
                          {detail.data.task_id === null ? (
                            '—'
                          ) : (
                            <CopyableValue label="Task ID" value={String(detail.data.task_id)} monospace />
                          )}
                        </BatchDetailField>
                        {detail.data.transaction_id && (
                          <BatchDetailField label="Transaction">
                            <CopyableValue
                              label="Transaction"
                              value={detail.data.transaction_id}
                              monospace
                              maxLength={32}
                            />
                          </BatchDetailField>
                        )}
                      </dl>
                      {detail.data.status_message && (
                        <p className="text-sm text-muted-foreground">{detail.data.status_message}</p>
                      )}
                      {detail.data.last_error && (
                        <Alert variant="destructive">
                          <AlertTitle>Batch needs attention</AlertTitle>
                          <AlertDescription>
                            <CopyableValue
                              label="Batch error"
                              value={detail.data.last_error}
                              displayValue={detail.data.last_error}
                              maxLength={120}
                            />
                          </AlertDescription>
                        </Alert>
                      )}
                      {detail.data.task?.storage_confirmation && (
                        <StorageConfirmationDetails confirmation={detail.data.task.storage_confirmation} />
                      )}
                    </section>
                    {(detail.data.can_seal || detail.data.task?.retryable) && (
                      <div className="flex flex-col gap-2">
                        <div className="flex flex-wrap items-center gap-2">
                          {sealButton(detail.data)}
                          {detail.data.task?.retryable && (
                            <Button
                              variant="outline"
                              size="sm"
                              disabled={recover.isPending}
                              onClick={() => {
                                if (detail.data.task) recover.mutate(detail.data.task.id)
                              }}
                            >
                              {recover.isPending ? (
                                <Loader2 data-icon="inline-start" className="animate-spin" />
                              ) : (
                                <RotateCcw data-icon="inline-start" />
                              )}
                              Recover
                            </Button>
                          )}
                        </div>
                        {detail.data.can_seal && (
                          <p className="text-xs text-muted-foreground">
                            Smaller batches may increase transaction costs. Confirmation is required before cached data
                            can be removed.
                          </p>
                        )}
                      </div>
                    )}
                  </>
                )
              )}
              {(seal.isError || recover.isError) && (
                <Alert variant="destructive">
                  <AlertTitle>Couldn't complete the action</AlertTitle>
                  <AlertDescription>
                    {(seal.error ?? recover.error)?.message ?? 'Refresh and try again.'}
                  </AlertDescription>
                </Alert>
              )}
            </div>
          </div>
        </SheetContent>
      </Sheet>
    </div>
  )
}

function BatchDetailField({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="min-w-0">
      <dt className="text-xs text-muted-foreground">{label}</dt>
      <dd className="mt-1 min-w-0 font-medium">{children}</dd>
    </div>
  )
}

function BatchTableSkeleton() {
  return (
    <div className="rounded-lg border p-4">
      <div className="flex flex-col gap-3">
        {['first', 'second', 'third', 'fourth', 'fifth', 'sixth'].map((row) => (
          <Skeleton key={row} className="h-10 w-full" />
        ))}
      </div>
    </div>
  )
}
