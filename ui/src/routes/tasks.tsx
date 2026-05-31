import { useMutation, useQueryClient } from '@tanstack/react-query'
import { createFileRoute } from '@tanstack/react-router'
import { ChevronDown, ListTodo, Loader2, RefreshCw, RotateCcw, Stethoscope, TriangleAlert } from 'lucide-react'
import { type ReactNode, useEffect, useState } from 'react'
import { api, type TaskDiagnostic, type TaskItem, type TaskStorageCleanupDetail } from '@/api/client'
import { CopyableValue } from '@/components/app/CopyableValue'
import { DangerActionAlertDialog } from '@/components/app/DangerActionAlertDialog'
import { DetailTextDialog } from '@/components/app/DetailTextDialog'
import { PageErrorState } from '@/components/app/PageErrorState'
import { PageHeader } from '@/components/app/PageHeader'
import { ReviewDetails } from '@/components/app/ReviewDetails'
import { StatusBadge, taskStatusTone } from '@/components/app/StatusBadge'
import { UploadProgressBar } from '@/components/app/UploadProgress'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible'
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from '@/components/ui/empty'
import { Label } from '@/components/ui/label'
import { Pagination, PaginationContent, PaginationItem } from '@/components/ui/pagination'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Separator } from '@/components/ui/separator'
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from '@/components/ui/sheet'
import { Skeleton } from '@/components/ui/skeleton'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { useTaskRefDetail, useTasks } from '@/hooks/queries'
import {
  storageCleanupCopyStatusLabel,
  storageCleanupCopyStatusTone,
  storageCleanupStatusLabel,
  taskHasByteTransfer,
  taskOperationLabel,
  taskOperationOptionLabel,
  taskReplicaLabel,
  taskStageOptions,
  taskTypeLabel,
} from '@/lib/storage-status-labels'
import {
  buildTaskDiagnosticViewModel,
  shouldRefreshTaskDiagnostic,
  type TaskDiagnosticFactRow,
  taskDiagnosticSheetContentClassName,
  taskDiagnosticStateLabel,
  taskDiagnosticStateTone,
} from '@/lib/task-diagnostics'
import { cn, formatBytes, timeAgo } from '@/lib/utils'

const taskTypeTabs = ['all', 'upload', 'evict_cache', 'storage_cleanup'] as const
const statusOptions = [
  'all',
  'queued',
  'scheduled',
  'waiting',
  'running',
  'completed',
  'failed',
  'exhausted',
  'cancelled',
] as const
const statusLabels: Record<string, string> = {
  all: 'All',
  queued: 'Queued',
  scheduled: 'Scheduled',
  waiting: 'Waiting',
  running: 'Running',
  completed: 'Completed',
  failed: 'Failed',
  cancelled: 'Cancelled',
  exhausted: 'Exhausted',
}

const PAGE_SIZE = 20

type TaskTypeTab = (typeof taskTypeTabs)[number]
type TaskStatusFilter = Exclude<(typeof statusOptions)[number], 'all'>
type TasksSearch = {
  type?: TaskTypeTab
  status?: TaskStatusFilter
}
type TaskDetailDialogState = { title: string; text: string }
type TaskTableProps = {
  tasks: TaskItem[]
  retryPending: boolean
  onRetry: (task: TaskItem) => void
  onOpenDiagnostic: (task: TaskItem) => void
  onOpenDetail: (dialog: TaskDetailDialogState) => void
}

const taskTypeSearchValues = new Set<string>(taskTypeTabs)
const taskStatusSearchValues = new Set<string>(statusOptions.filter((option) => option !== 'all'))

export const Route = createFileRoute('/tasks')({
  validateSearch: (search: Record<string, unknown>): TasksSearch => {
    const type = typeof search.type === 'string' && taskTypeSearchValues.has(search.type) ? search.type : undefined
    const status =
      typeof search.status === 'string' && taskStatusSearchValues.has(search.status) ? search.status : undefined
    return {
      type: type as TaskTypeTab | undefined,
      status: status as TaskStatusFilter | undefined,
    }
  },
  component: TasksPage,
})

function taskDetailText(task: TaskItem) {
  return task.status_message || task.last_error || ''
}

function taskDetailTitle(task: TaskItem) {
  return task.last_error ? 'Error Details' : 'Status Details'
}

function TaskRefCell({ task }: { task: TaskItem }) {
  const [detailsOpen, setDetailsOpen] = useState(false)
  const detail = useTaskRefDetail(task.id, detailsOpen)
  const refLabel = task.type === 'storage_cleanup' ? 'Deleted object' : `${task.ref_type}:${task.ref_id}`

  return (
    <Popover open={detailsOpen} onOpenChange={setDetailsOpen}>
      <PopoverTrigger asChild>
        <button
          type="button"
          className="inline-flex max-w-48 truncate font-mono text-xs text-muted-foreground underline decoration-dotted underline-offset-2 hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
          aria-label={`${refLabel} details`}
        >
          {refLabel}
        </button>
      </PopoverTrigger>
      <PopoverContent
        side="top"
        aria-label={`${refLabel} details`}
        className="max-h-[min(calc(100vh-2rem),32rem)] w-max max-w-[min(calc(100vw-2rem),28rem)] overflow-y-auto whitespace-normal p-3 text-left"
      >
        <TaskRefPopoverContent detail={detail} enabled={detailsOpen} />
      </PopoverContent>
    </Popover>
  )
}

function TaskRefPopoverContent({ detail, enabled }: { detail: ReturnType<typeof useTaskRefDetail>; enabled: boolean }) {
  if (detail.error) {
    return <span>Details unavailable</span>
  }
  if (!detail.data) {
    return <span>{enabled ? 'Loading details' : ''}</span>
  }
  if (!detail.data.object && !detail.data.storage_cleanup) {
    return <span>Details unavailable</span>
  }

  if (detail.data.storage_cleanup) {
    return <TaskStorageCleanupContent cleanup={detail.data.storage_cleanup} />
  }

  if (!detail.data.object) {
    return <span>Details unavailable</span>
  }

  return <TaskObjectRefContent object={detail.data.object} />
}

function TaskStorageCleanupContent({ cleanup }: { cleanup: TaskStorageCleanupDetail }) {
  const versions = cleanup.deleted_versions ?? []
  if (versions.length === 0) {
    return <span>Details unavailable</span>
  }

  const totalSize = versions.reduce((sum, version) => sum + version.size, 0)
  const firstDeletedAt = versions.find((version) => version.deleted_at)?.deleted_at
  const copies = cleanup.copies ?? []

  return (
    <div className="flex max-w-sm flex-col gap-3 text-xs">
      <div className="grid grid-cols-[auto_1fr] gap-x-2 gap-y-1">
        <span className="text-muted-foreground">Bucket</span>
        <span className="min-w-0 truncate font-medium">
          {storageCleanupValueLabel(
            versions.map((version) => version.bucket_name),
            'buckets'
          )}
        </span>
        <span className="text-muted-foreground">Key</span>
        <span className="min-w-0 truncate font-medium">
          {storageCleanupValueLabel(
            versions.map((version) => version.key),
            'objects'
          )}
        </span>
        <span className="text-muted-foreground">Version</span>
        <span className="min-w-0 truncate font-mono">{storageCleanupVersionLabel(versions)}</span>
        <span className="text-muted-foreground">Status</span>
        <span>{storageCleanupStatusLabel(copies)}</span>
        <span className="text-muted-foreground">Size</span>
        <span>{formatBytes(totalSize)}</span>
        <span className="text-muted-foreground">Deleted</span>
        <span>{firstDeletedAt ? timeAgo(firstDeletedAt) : '—'}</span>
      </div>
      {copies.length > 0 && (
        <div className="flex flex-col gap-2">
          <span className="font-medium">Replica cleanup</span>
          <div className="flex flex-col gap-2">
            {copies.map((copy) => (
              <div key={copy.copy_index} className="flex min-w-0 flex-col gap-1">
                <div className="flex min-w-0 items-center gap-2">
                  <span className="font-mono text-muted-foreground">
                    {taskReplicaLabel({ copy_index: copy.copy_index })}
                  </span>
                  <StatusBadge tone={storageCleanupCopyStatusTone(copy.status)}>
                    {storageCleanupCopyStatusLabel(copy.status)}
                  </StatusBadge>
                </div>
                {copy.delete_tx_hash && (
                  <CopyableValue label="Delete transaction" value={copy.delete_tx_hash} monospace maxLength={24} />
                )}
                {copy.last_error && (
                  <span className="whitespace-normal break-all text-destructive">{copy.last_error}</span>
                )}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

function TaskObjectRefContent({
  object,
}: {
  object: NonNullable<ReturnType<typeof useTaskRefDetail>['data']>['object']
}) {
  if (!object) {
    return <span>Details unavailable</span>
  }

  return (
    <div className="grid max-w-xs grid-cols-[auto_1fr] gap-x-2 gap-y-1 text-xs">
      <span className="text-muted-foreground">Bucket</span>
      <span className="truncate font-medium">{object.bucket_name}</span>
      <span className="text-muted-foreground">Key</span>
      <span className="truncate font-medium">{object.key}</span>
      <span className="text-muted-foreground">Version</span>
      <span className="truncate font-mono">{object.version_id}</span>
      <span className="text-muted-foreground">State</span>
      <span>{object.state}</span>
      {object.upload_status && (
        <>
          <span className="text-muted-foreground">Upload</span>
          <span>{object.upload_status}</span>
        </>
      )}
      <span className="text-muted-foreground">Size</span>
      <span>{formatBytes(object.size)}</span>
      <span className="text-muted-foreground">Location</span>
      <span>{taskObjectLocationLabel(object.location)}</span>
      <span className="text-muted-foreground">Updated</span>
      <span>{timeAgo(object.updated_at)}</span>
    </div>
  )
}

function storageCleanupValueLabel(values: string[], pluralName: string) {
  const uniqueValues = uniqueNonEmpty(values)
  if (uniqueValues.length === 0) return '—'
  if (uniqueValues.length === 1) return uniqueValues[0]
  return `${uniqueValues[0]} +${uniqueValues.length - 1} ${pluralName}`
}

function storageCleanupVersionLabel(versions: TaskStorageCleanupDetail['deleted_versions']) {
  const first = versions[0]
  if (!first) return '—'
  if (versions.length === 1) return first.version_id
  return `${first.version_id} +${versions.length - 1} versions`
}

function uniqueNonEmpty(values: Array<string | undefined>) {
  return Array.from(new Set(values.filter((value): value is string => Boolean(value))))
}

function taskObjectLocationLabel(location: { cache: boolean; filecoin: boolean }) {
  if (location.cache && location.filecoin) return 'Cache + Filecoin'
  if (location.cache) return 'Cache'
  if (location.filecoin) return 'Filecoin'
  return 'None'
}

function TaskDetailsCell({
  task,
  onOpenDetail,
}: {
  task: TaskItem
  onOpenDetail: (dialog: TaskDetailDialogState) => void
}) {
  const detailText = taskDetailText(task)
  if (!detailText) {
    return <span className="text-muted-foreground">—</span>
  }
  return (
    <Button
      type="button"
      variant="link"
      onClick={() => {
        onOpenDetail({ title: taskDetailTitle(task), text: detailText })
      }}
      className="h-auto max-w-full justify-start p-0 text-left text-xs font-normal text-muted-foreground hover:text-foreground"
    >
      <span className="truncate">{detailText}</span>
    </Button>
  )
}

function TaskActionsCell({
  task,
  retryPending,
  onRetry,
  onOpenDiagnostic,
}: {
  task: TaskItem
  retryPending: boolean
  onRetry: (task: TaskItem) => void
  onOpenDiagnostic: (task: TaskItem) => void
}) {
  const showDiagnostic = task.type === 'upload'
  const showRetry = task.status === 'exhausted'
  if (!showDiagnostic && !showRetry) return <span className="text-muted-foreground">—</span>

  return (
    <div className="flex items-center gap-1">
      {showDiagnostic && (
        <Tooltip>
          <TooltipTrigger asChild>
            <Button
              type="button"
              variant="outline"
              size="icon-xs"
              onClick={() => onOpenDiagnostic(task)}
              aria-label="Open task diagnostics"
            >
              <Stethoscope data-icon="inline-start" />
            </Button>
          </TooltipTrigger>
          <TooltipContent>Diagnostics</TooltipContent>
        </Tooltip>
      )}
      {showRetry && (
        <Tooltip>
          <TooltipTrigger asChild>
            <Button
              type="button"
              variant="outline"
              size="icon-xs"
              onClick={() => onRetry(task)}
              disabled={retryPending}
              aria-label="Retry task"
            >
              <RotateCcw data-icon="inline-start" />
            </Button>
          </TooltipTrigger>
          <TooltipContent>Retry</TooltipContent>
        </Tooltip>
      )}
    </div>
  )
}

function TaskCommonCells({
  task,
  retryPending,
  onRetry,
  onOpenDiagnostic,
  onOpenDetail,
}: {
  task: TaskItem
  retryPending: boolean
  onRetry: (task: TaskItem) => void
  onOpenDiagnostic: (task: TaskItem) => void
  onOpenDetail: (dialog: TaskDetailDialogState) => void
}) {
  return (
    <>
      <TableCell className="whitespace-nowrap px-3 py-2 text-muted-foreground">
        <TaskRefCell task={task} />
      </TableCell>
      <TableCell className="whitespace-nowrap px-3 py-2">
        <StatusBadge tone={taskStatusTone(task.status)}>{task.status}</StatusBadge>
      </TableCell>
      <TableCell className="whitespace-nowrap px-3 py-2 text-right">
        {task.retry_count}/{task.max_retries}
      </TableCell>
      <TableCell className="max-w-xs whitespace-nowrap px-3 py-2 text-xs text-muted-foreground">
        <TaskDetailsCell task={task} onOpenDetail={onOpenDetail} />
      </TableCell>
      <TableCell className="whitespace-nowrap px-3 py-2 text-muted-foreground">{timeAgo(task.scheduled_at)}</TableCell>
      <TableCell className="whitespace-nowrap px-3 py-2">
        <TaskActionsCell
          task={task}
          retryPending={retryPending}
          onRetry={onRetry}
          onOpenDiagnostic={onOpenDiagnostic}
        />
      </TableCell>
    </>
  )
}

function AllTasksTable({ tasks, retryPending, onRetry, onOpenDiagnostic, onOpenDetail }: TaskTableProps) {
  return (
    <TaskTableFrame>
      <Table>
        <TableHeader>
          <TableRow className="bg-muted/50">
            <TableHead className="whitespace-nowrap px-3 py-2">ID</TableHead>
            <TableHead className="whitespace-nowrap px-3 py-2">Type</TableHead>
            <TableHead className="whitespace-nowrap px-3 py-2">Operation</TableHead>
            <TableHead className="whitespace-nowrap px-3 py-2">Object</TableHead>
            <TableHead className="whitespace-nowrap px-3 py-2">Status</TableHead>
            <TableHead className="whitespace-nowrap px-3 py-2 text-right">Retries</TableHead>
            <TableHead className="whitespace-nowrap px-3 py-2">Details</TableHead>
            <TableHead className="whitespace-nowrap px-3 py-2">Scheduled</TableHead>
            <TableHead className="whitespace-nowrap px-3 py-2">Actions</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {tasks.length > 0 ? (
            tasks.map((task) => (
              <TableRow key={task.id}>
                <TableCell className="whitespace-nowrap px-3 py-2 font-mono text-xs">{task.id}</TableCell>
                <TableCell className="whitespace-nowrap px-3 py-2 text-muted-foreground">
                  {taskTypeLabel(task.type)}
                </TableCell>
                <TableCell className="whitespace-nowrap px-3 py-2">{taskOperationLabel(task)}</TableCell>
                <TaskCommonCells
                  task={task}
                  retryPending={retryPending}
                  onRetry={onRetry}
                  onOpenDiagnostic={onOpenDiagnostic}
                  onOpenDetail={onOpenDetail}
                />
              </TableRow>
            ))
          ) : (
            <TaskEmptyRow colSpan={9} />
          )}
        </TableBody>
      </Table>
    </TaskTableFrame>
  )
}

function UploadTasksTable({ tasks, retryPending, onRetry, onOpenDiagnostic, onOpenDetail }: TaskTableProps) {
  return (
    <TaskTableFrame>
      <Table>
        <TableHeader>
          <TableRow className="bg-muted/50">
            <TableHead className="whitespace-nowrap px-3 py-2">ID</TableHead>
            <TableHead className="whitespace-nowrap px-3 py-2">Operation</TableHead>
            <TableHead className="whitespace-nowrap px-3 py-2">Replica</TableHead>
            <TableHead className="whitespace-nowrap px-3 py-2">Object</TableHead>
            <TableHead className="whitespace-nowrap px-3 py-2">Status</TableHead>
            <TableHead className="whitespace-nowrap px-3 py-2 text-right">Retries</TableHead>
            <TableHead className="whitespace-nowrap px-3 py-2">Details</TableHead>
            <TableHead className="whitespace-nowrap px-3 py-2">Scheduled</TableHead>
            <TableHead className="whitespace-nowrap px-3 py-2">Actions</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {tasks.length > 0 ? (
            tasks.map((task) => (
              <TableRow key={task.id}>
                <TableCell className="whitespace-nowrap px-3 py-2 font-mono text-xs">{task.id}</TableCell>
                <TableCell className="whitespace-nowrap px-3 py-2">
                  <div className="flex items-center gap-3">
                    <span className="text-sm">{taskOperationLabel(task)}</span>
                    {taskHasByteTransfer(task) && task.progress && <UploadProgressBar progress={task.progress} />}
                  </div>
                </TableCell>
                <TableCell className="whitespace-nowrap px-3 py-2 text-muted-foreground">
                  {taskReplicaLabel(task)}
                </TableCell>
                <TaskCommonCells
                  task={task}
                  retryPending={retryPending}
                  onRetry={onRetry}
                  onOpenDiagnostic={onOpenDiagnostic}
                  onOpenDetail={onOpenDetail}
                />
              </TableRow>
            ))
          ) : (
            <TaskEmptyRow colSpan={9} />
          )}
        </TableBody>
      </Table>
    </TaskTableFrame>
  )
}

function EvictCacheTasksTable({ tasks, retryPending, onRetry, onOpenDiagnostic, onOpenDetail }: TaskTableProps) {
  return (
    <TaskTableFrame>
      <Table>
        <TableHeader>
          <TableRow className="bg-muted/50">
            <TableHead className="whitespace-nowrap px-3 py-2">ID</TableHead>
            <TableHead className="whitespace-nowrap px-3 py-2">Object</TableHead>
            <TableHead className="whitespace-nowrap px-3 py-2">Status</TableHead>
            <TableHead className="whitespace-nowrap px-3 py-2 text-right">Retries</TableHead>
            <TableHead className="whitespace-nowrap px-3 py-2">Details</TableHead>
            <TableHead className="whitespace-nowrap px-3 py-2">Scheduled</TableHead>
            <TableHead className="whitespace-nowrap px-3 py-2">Actions</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {tasks.length > 0 ? (
            tasks.map((task) => (
              <TableRow key={task.id}>
                <TableCell className="whitespace-nowrap px-3 py-2 font-mono text-xs">{task.id}</TableCell>
                <TaskCommonCells
                  task={task}
                  retryPending={retryPending}
                  onRetry={onRetry}
                  onOpenDiagnostic={onOpenDiagnostic}
                  onOpenDetail={onOpenDetail}
                />
              </TableRow>
            ))
          ) : (
            <TaskEmptyRow colSpan={7} />
          )}
        </TableBody>
      </Table>
    </TaskTableFrame>
  )
}

function TaskTableFrame({ children }: { children: ReactNode }) {
  return <div className="overflow-hidden rounded-lg border border-border">{children}</div>
}

function TaskEmptyRow({ colSpan }: { colSpan: number }) {
  return (
    <TableRow>
      <TableCell colSpan={colSpan} className="h-60">
        <Empty className="border-0">
          <EmptyHeader>
            <EmptyMedia variant="icon">
              <ListTodo />
            </EmptyMedia>
            <EmptyTitle>No tasks found</EmptyTitle>
            <EmptyDescription>There are no tasks matching your current filters.</EmptyDescription>
          </EmptyHeader>
        </Empty>
      </TableCell>
    </TableRow>
  )
}

function TaskDiagnosticSheet({
  task,
  diagnostic,
  loading,
  refreshing,
  error,
  onClose,
}: {
  task: TaskItem | null
  diagnostic: TaskDiagnostic | null
  loading: boolean
  refreshing: boolean
  error: string | null
  onClose: () => void
}) {
  const checking = refreshing
  const [detailsOpen, setDetailsOpen] = useState(true)
  const view = diagnostic ? buildTaskDiagnosticViewModel(diagnostic) : null

  return (
    <Sheet
      open={Boolean(task)}
      onOpenChange={(open) => {
        if (!open) {
          setDetailsOpen(true)
          onClose()
        }
      }}
    >
      <SheetContent className={taskDiagnosticSheetContentClassName}>
        <SheetHeader>
          <SheetTitle>Upload diagnostics</SheetTitle>
          <SheetDescription>{task ? `Task #${task.id}` : 'Upload task diagnostics'}</SheetDescription>
        </SheetHeader>
        <ScrollArea className="min-h-0 flex-1">
          <div className="flex flex-col gap-5 px-4 pb-4">
            {loading && !diagnostic ? (
              <TaskDiagnosticSkeleton />
            ) : diagnostic && view ? (
              <>
                <section className="flex flex-col gap-3">
                  <div className="flex items-center justify-between gap-3">
                    <h2 className="text-xs font-medium text-muted-foreground">Current status</h2>
                    <StatusBadge tone={taskDiagnosticStateTone(diagnostic.current_state)}>
                      {taskDiagnosticStateLabel(diagnostic.current_state)}
                    </StatusBadge>
                  </div>
                  <div className="flex flex-col gap-1">
                    <p className="text-base font-semibold leading-6">{view.title}</p>
                  </div>
                  {checking && (
                    <div className="flex items-center gap-2 text-sm text-muted-foreground">
                      <Loader2 className="animate-spin" />
                      Checking latest storage status
                    </div>
                  )}
                </section>

                <Separator />

                <section className="flex flex-col gap-3">
                  <h2 className="text-sm font-medium">Evidence</h2>
                  <TaskDiagnosticFactList rows={view.primaryFacts} />
                </section>

                <Separator />

                <Collapsible open={detailsOpen} onOpenChange={setDetailsOpen} className="flex flex-col gap-3">
                  <div className="flex items-center justify-between gap-3">
                    <h2 className="text-sm font-medium">Recorded details</h2>
                    <CollapsibleTrigger asChild>
                      <Button type="button" variant="ghost" size="sm">
                        {detailsOpen ? 'Hide' : 'Show'}
                        <ChevronDown
                          data-icon="inline-end"
                          className={cn('transition-transform', detailsOpen && 'rotate-180')}
                        />
                      </Button>
                    </CollapsibleTrigger>
                  </div>
                  <CollapsibleContent>
                    <TaskDiagnosticFactList rows={view.detailFacts} />
                  </CollapsibleContent>
                </Collapsible>
              </>
            ) : null}

            {error && (
              <Alert variant="destructive">
                <TriangleAlert />
                <AlertTitle>Diagnostics unavailable</AlertTitle>
                <AlertDescription>{error}</AlertDescription>
              </Alert>
            )}
          </div>
        </ScrollArea>
      </SheetContent>
    </Sheet>
  )
}

function TaskDiagnosticSkeleton() {
  return (
    <div className="flex flex-col gap-5">
      <section className="flex flex-col gap-3">
        <div className="flex items-center justify-between gap-3">
          <Skeleton className="h-3 w-24" />
          <Skeleton className="h-6 w-24 rounded-full" />
        </div>
        <Skeleton className="h-6 w-64 max-w-full" />
      </section>
      <Separator />
      <section className="flex flex-col gap-3">
        <Skeleton className="h-4 w-16" />
        <div className="grid grid-cols-1 gap-x-3 gap-y-2 sm:grid-cols-[9rem_minmax(0,1fr)]">
          <Skeleton className="h-4 w-20" />
          <Skeleton className="h-4 w-48 max-w-full" />
          <Skeleton className="h-4 w-24" />
          <Skeleton className="h-4 w-56 max-w-full" />
          <Skeleton className="h-4 w-20" />
          <Skeleton className="h-4 w-40 max-w-full" />
        </div>
      </section>
    </div>
  )
}

function TaskDiagnosticFactList({ rows }: { rows: TaskDiagnosticFactRow[] }) {
  return (
    <dl className="grid grid-cols-1 gap-x-3 gap-y-2 text-sm sm:grid-cols-[9rem_minmax(0,1fr)]">
      {rows.map((row) => (
        <TaskDiagnosticFactRowView key={`${row.label}:${row.value}`} row={row} />
      ))}
    </dl>
  )
}

function TaskDiagnosticFactRowView({ row }: { row: TaskDiagnosticFactRow }) {
  return (
    <>
      <dt className="text-muted-foreground">{row.label}</dt>
      <dd className="min-w-0">
        {row.detail ? (
          <CopyableValue
            label={row.label}
            value={row.detailValue ?? row.value}
            displayValue={row.detailValue ? row.value : undefined}
            monospace={row.monospace}
            maxLength={row.displayMaxLength}
          />
        ) : (
          <span className={row.monospace ? 'truncate font-mono text-xs' : 'truncate'}>{row.value}</span>
        )}
      </dd>
    </>
  )
}

function isAbortError(err: unknown) {
  return typeof err === 'object' && err !== null && 'name' in err && err.name === 'AbortError'
}

function TasksPage() {
  const search = Route.useSearch()
  const [status, setStatus] = useState(search.status ?? '')
  const [taskType, setTaskType] = useState<TaskTypeTab>(search.type ?? 'all')
  const [stage, setStage] = useState('')
  const [offset, setOffset] = useState(0)
  const queryTaskType = taskType === 'all' ? '' : taskType
  const { data, isLoading, error } = useTasks(queryTaskType, stage, status, PAGE_SIZE, offset)
  const qc = useQueryClient()

  const [retryTarget, setRetryTarget] = useState<TaskItem | null>(null)
  const [detailDialog, setDetailDialog] = useState<{ title: string; text: string } | null>(null)
  const [diagnosticTarget, setDiagnosticTarget] = useState<TaskItem | null>(null)
  const [diagnostic, setDiagnostic] = useState<TaskDiagnostic | null>(null)
  const [diagnosticLoading, setDiagnosticLoading] = useState(false)
  const [diagnosticRefreshing, setDiagnosticRefreshing] = useState(false)
  const [diagnosticError, setDiagnosticError] = useState<string | null>(null)
  const retryMutation = useMutation({
    mutationFn: (taskId: number) => api.retryTask(taskId),
    onSuccess: () => setRetryTarget(null),
    onSettled: () => {
      qc.invalidateQueries({ queryKey: ['tasks'] })
    },
  })
  const retryPending = retryMutation.isPending

  const totalPages = data ? Math.ceil(data.total / PAGE_SIZE) : 0
  const currentPage = Math.floor(offset / PAGE_SIZE) + 1

  useEffect(() => {
    setTaskType(search.type ?? 'all')
    setStatus(search.status ?? '')
    setStage('')
    setOffset(0)
  }, [search.type, search.status])

  useEffect(() => {
    if (!diagnosticTarget) {
      setDiagnostic(null)
      setDiagnosticLoading(false)
      setDiagnosticRefreshing(false)
      setDiagnosticError(null)
      return
    }
    let active = true
    const controller = new AbortController()
    setDiagnostic(null)
    setDiagnosticError(null)
    setDiagnosticLoading(true)
    setDiagnosticRefreshing(false)

    api
      .getTaskDiagnostic(diagnosticTarget.id, { signal: controller.signal })
      .then((initial) => {
        if (!active) return null
        setDiagnostic(initial)
        setDiagnosticLoading(false)
        if (!shouldRefreshTaskDiagnostic(initial)) return null
        setDiagnosticRefreshing(true)
        return api.refreshTaskDiagnostic(diagnosticTarget.id, { signal: controller.signal })
      })
      .then((refreshed) => {
        if (active && refreshed) setDiagnostic(refreshed)
      })
      .catch((err: unknown) => {
        if (isAbortError(err)) return
        if (active) setDiagnosticError(err instanceof Error ? err.message : 'Failed to load task diagnostics')
      })
      .finally(() => {
        if (!active) return
        setDiagnosticLoading(false)
        setDiagnosticRefreshing(false)
      })

    return () => {
      active = false
      controller.abort()
    }
  }, [diagnosticTarget])

  function openRetryDialog(task: TaskItem) {
    if (retryPending) return
    retryMutation.reset()
    setRetryTarget(task)
  }

  function closeDiagnostic() {
    setDiagnosticTarget(null)
  }

  return (
    <div className="flex flex-col gap-4 p-6">
      <PageHeader
        title="Tasks"
        actions={
          <Button variant="outline" size="sm" onClick={() => qc.invalidateQueries({ queryKey: ['tasks'] })}>
            <RefreshCw data-icon="inline-start" /> Refresh
          </Button>
        }
      />

      <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
        <Tabs
          value={taskType}
          onValueChange={(value) => {
            setTaskType(value as TaskTypeTab)
            setStage('')
            setOffset(0)
          }}
          className="min-w-0"
        >
          <TabsList className="max-w-full justify-start overflow-x-auto">
            {taskTypeTabs.map((tab) => (
              <TabsTrigger key={tab} value={tab}>
                {taskTypeLabel(tab)}
              </TabsTrigger>
            ))}
          </TabsList>
        </Tabs>

        <div className="flex flex-wrap items-center gap-2">
          {taskType === 'upload' && (
            <>
              <Label htmlFor="task-stage-filter" className="text-sm text-muted-foreground">
                Operation:
              </Label>
              <Select
                value={stage || 'all'}
                onValueChange={(value) => {
                  setStage(value === 'all' ? '' : value)
                  setOffset(0)
                }}
              >
                <SelectTrigger id="task-stage-filter" className="w-48">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectGroup>
                    {taskStageOptions.map((option) => (
                      <SelectItem key={option} value={option}>
                        {taskOperationOptionLabel(option)}
                      </SelectItem>
                    ))}
                  </SelectGroup>
                </SelectContent>
              </Select>
            </>
          )}
          <Label htmlFor="task-status-filter" className="text-sm text-muted-foreground">
            Status:
          </Label>
          <Select
            value={status || 'all'}
            onValueChange={(value) => {
              setStatus(value === 'all' ? '' : value)
              setOffset(0)
            }}
          >
            <SelectTrigger id="task-status-filter" className="w-44">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectGroup>
                {statusOptions.map((option) => (
                  <SelectItem key={option} value={option}>
                    {statusLabels[option]}
                  </SelectItem>
                ))}
              </SelectGroup>
            </SelectContent>
          </Select>
        </div>
      </div>

      {isLoading ? (
        <div className="flex h-60 items-center justify-center">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      ) : error ? (
        <PageErrorState title="Failed to load tasks" className="h-60" />
      ) : (
        <>
          {taskType === 'all' ? (
            <AllTasksTable
              tasks={data?.tasks ?? []}
              retryPending={retryPending}
              onRetry={openRetryDialog}
              onOpenDiagnostic={setDiagnosticTarget}
              onOpenDetail={setDetailDialog}
            />
          ) : taskType === 'upload' ? (
            <UploadTasksTable
              tasks={data?.tasks ?? []}
              retryPending={retryPending}
              onRetry={openRetryDialog}
              onOpenDiagnostic={setDiagnosticTarget}
              onOpenDetail={setDetailDialog}
            />
          ) : (
            <EvictCacheTasksTable
              tasks={data?.tasks ?? []}
              retryPending={retryPending}
              onRetry={openRetryDialog}
              onOpenDiagnostic={setDiagnosticTarget}
              onOpenDetail={setDetailDialog}
            />
          )}

          {totalPages > 1 && (
            <div className="flex items-center justify-between">
              <span className="text-sm text-muted-foreground">
                Page {currentPage} of {totalPages} ({data?.total} total)
              </span>
              <Pagination className="mx-0 w-auto justify-end">
                <PaginationContent>
                  <PaginationItem>
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      disabled={offset === 0}
                      onClick={() => setOffset(Math.max(0, offset - PAGE_SIZE))}
                    >
                      Prev
                    </Button>
                  </PaginationItem>
                  <PaginationItem>
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      disabled={currentPage >= totalPages}
                      onClick={() => setOffset(offset + PAGE_SIZE)}
                    >
                      Next
                    </Button>
                  </PaginationItem>
                </PaginationContent>
              </Pagination>
            </div>
          )}
        </>
      )}

      <DangerActionAlertDialog
        open={Boolean(retryTarget)}
        onOpenChange={(open) => !open && setRetryTarget(null)}
        title="Retry exhausted task?"
        description={retryTarget ? retryTaskDescription(retryTarget) : ''}
        confirmLabel="Retry task"
        pending={Boolean(retryTarget && retryMutation.isPending && retryMutation.variables === retryTarget.id)}
        error={retryMutation.error?.message}
        onConfirm={() => {
          if (retryTarget && !retryPending) retryMutation.mutate(retryTarget.id)
        }}
      >
        {retryTarget && (
          <ReviewDetails
            rows={[
              { id: 'task-id', label: 'Task ID', value: retryTarget.id.toString() },
              { id: 'type', label: 'Type', value: taskTypeLabel(retryTarget.type) },
              { id: 'ref', label: 'Ref', value: `${retryTarget.ref_type}:${retryTarget.ref_id}`, copyable: true },
              { id: 'retries', label: 'Retries', value: `${retryTarget.retry_count}/${retryTarget.max_retries}` },
              {
                id: 'last-error',
                label: 'Last error',
                value: retryTarget.last_error ?? 'None',
                copyable: Boolean(retryTarget.last_error),
              },
              {
                id: 'status-message',
                label: 'Status message',
                value: retryTarget.status_message ?? 'None',
                copyable: Boolean(retryTarget.status_message),
              },
            ]}
          />
        )}
      </DangerActionAlertDialog>

      <DetailTextDialog
        title={detailDialog?.title ?? 'Task Details'}
        text={detailDialog?.text ?? null}
        onClose={() => setDetailDialog(null)}
      />

      <TaskDiagnosticSheet
        key={diagnosticTarget?.id ?? 'empty'}
        task={diagnosticTarget}
        diagnostic={diagnostic}
        loading={diagnosticLoading}
        refreshing={diagnosticRefreshing}
        error={diagnosticError}
        onClose={closeDiagnostic}
      />
    </div>
  )
}

function retryTaskDescription(task: TaskItem) {
  switch (task.type) {
    case 'upload':
      return 'This will requeue upload work and may trigger provider and on-chain operations again.'
    case 'evict_cache':
      return 'This will requeue cache eviction work and may remove local cached data.'
    case 'storage_cleanup':
      return 'This will retry deleting remote replicas for deleted object versions.'
    default:
      return 'This will requeue the exhausted task for background processing.'
  }
}
