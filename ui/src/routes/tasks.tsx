import { useMutation, useQueryClient } from '@tanstack/react-query'
import { createFileRoute } from '@tanstack/react-router'
import { Loader2, RefreshCw, RotateCcw } from 'lucide-react'
import { type ReactNode, useEffect, useState } from 'react'
import { api, type TaskItem, type TaskStorageCleanupDetail } from '@/api/client'
import { DangerActionAlertDialog } from '@/components/app/DangerActionAlertDialog'
import { DetailTextDialog } from '@/components/app/DetailTextDialog'
import { PageHeader } from '@/components/app/PageHeader'
import { ReviewDetails } from '@/components/app/ReviewDetails'
import { StatusBadge, taskStatusTone } from '@/components/app/StatusBadge'
import { UploadProgressBar } from '@/components/app/UploadProgress'
import { Button } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { Pagination, PaginationContent, PaginationItem } from '@/components/ui/pagination'
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { useTaskRefDetail, useTasks } from '@/hooks/queries'
import {
  storageCleanupStatusLabel,
  taskHasByteTransfer,
  taskOperationLabel,
  taskOperationOptionLabel,
  taskReplicaLabel,
  taskStageOptions,
  taskTypeLabel,
} from '@/lib/storage-status-labels'
import { formatBytes, timeAgo } from '@/lib/utils'

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
  const [detailEnabled, setDetailEnabled] = useState(false)
  const detail = useTaskRefDetail(task.id, detailEnabled)
  const refLabel = task.type === 'storage_cleanup' ? 'Deleted object' : `${task.ref_type}:${task.ref_id}`

  const enableDetail = () => setDetailEnabled(true)

  return (
    <Tooltip
      delayDuration={250}
      onOpenChange={(open) => {
        if (open) enableDetail()
      }}
    >
      <TooltipTrigger asChild>
        <button
          type="button"
          className="inline-flex max-w-48 truncate font-mono text-xs text-muted-foreground hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
          onClick={enableDetail}
          aria-label={`${refLabel} details`}
        >
          {refLabel}
        </button>
      </TooltipTrigger>
      <TooltipContent side="top" className="max-w-sm items-start whitespace-normal text-left">
        <TaskRefTooltipContent detail={detail} enabled={detailEnabled} />
      </TooltipContent>
    </Tooltip>
  )
}

function TaskRefTooltipContent({ detail, enabled }: { detail: ReturnType<typeof useTaskRefDetail>; enabled: boolean }) {
  if (!enabled || detail.isLoading || (detail.isFetching && !detail.data)) {
    return <span>Loading details</span>
  }
  if (detail.error || (!detail.data?.object && !detail.data?.storage_cleanup)) {
    return <span>Details unavailable</span>
  }

  if (detail.data.storage_cleanup) {
    return <TaskStorageCleanupTooltip cleanup={detail.data.storage_cleanup} />
  }

  if (!detail.data.object) {
    return <span>Details unavailable</span>
  }

  return <TaskObjectRefTooltip object={detail.data.object} />
}

function TaskStorageCleanupTooltip({ cleanup }: { cleanup: TaskStorageCleanupDetail }) {
  const versions = cleanup.deleted_versions ?? []
  if (versions.length === 0) {
    return <span>Details unavailable</span>
  }

  const totalSize = versions.reduce((sum, version) => sum + version.size, 0)
  const firstDeletedAt = versions.find((version) => version.deleted_at)?.deleted_at
  return (
    <div className="grid max-w-xs grid-cols-[auto_1fr] gap-x-2 gap-y-1 text-xs">
      <span className="text-muted-foreground">Bucket</span>
      <span className="truncate font-medium">
        {storageCleanupValueLabel(
          versions.map((version) => version.bucket_name),
          'buckets'
        )}
      </span>
      <span className="text-muted-foreground">Key</span>
      <span className="truncate font-medium">
        {storageCleanupValueLabel(
          versions.map((version) => version.key),
          'objects'
        )}
      </span>
      <span className="text-muted-foreground">Version</span>
      <span className="truncate font-mono">{storageCleanupVersionLabel(versions)}</span>
      <span className="text-muted-foreground">Status</span>
      <span>{storageCleanupStatusLabel(cleanup.copies)}</span>
      <span className="text-muted-foreground">Size</span>
      <span>{formatBytes(totalSize)}</span>
      <span className="text-muted-foreground">Deleted</span>
      <span>{firstDeletedAt ? timeAgo(firstDeletedAt) : '—'}</span>
    </div>
  )
}

function TaskObjectRefTooltip({
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
}: {
  task: TaskItem
  retryPending: boolean
  onRetry: (task: TaskItem) => void
}) {
  if (task.status !== 'exhausted') return <span className="text-muted-foreground">—</span>
  return (
    <Button type="button" variant="outline" size="xs" onClick={() => onRetry(task)} disabled={retryPending}>
      <RotateCcw data-icon="inline-start" /> Retry
    </Button>
  )
}

function TaskCommonCells({
  task,
  retryPending,
  onRetry,
  onOpenDetail,
}: {
  task: TaskItem
  retryPending: boolean
  onRetry: (task: TaskItem) => void
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
        <TaskActionsCell task={task} retryPending={retryPending} onRetry={onRetry} />
      </TableCell>
    </>
  )
}

function AllTasksTable({ tasks, retryPending, onRetry, onOpenDetail }: TaskTableProps) {
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

function UploadTasksTable({ tasks, retryPending, onRetry, onOpenDetail }: TaskTableProps) {
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

function EvictCacheTasksTable({ tasks, retryPending, onRetry, onOpenDetail }: TaskTableProps) {
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
      <TableCell colSpan={colSpan} className="h-24 text-center text-muted-foreground">
        No tasks found
      </TableCell>
    </TableRow>
  )
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

  function openRetryDialog(task: TaskItem) {
    if (retryPending) return
    retryMutation.reset()
    setRetryTarget(task)
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
        <div className="text-destructive">Failed to load tasks</div>
      ) : (
        <>
          {taskType === 'all' ? (
            <AllTasksTable
              tasks={data?.tasks ?? []}
              retryPending={retryPending}
              onRetry={openRetryDialog}
              onOpenDetail={setDetailDialog}
            />
          ) : taskType === 'upload' ? (
            <UploadTasksTable
              tasks={data?.tasks ?? []}
              retryPending={retryPending}
              onRetry={openRetryDialog}
              onOpenDetail={setDetailDialog}
            />
          ) : (
            <EvictCacheTasksTable
              tasks={data?.tasks ?? []}
              retryPending={retryPending}
              onRetry={openRetryDialog}
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
              { id: 'ref', label: 'Ref', value: `${retryTarget.ref_type}:${retryTarget.ref_id}` },
              { id: 'retries', label: 'Retries', value: `${retryTarget.retry_count}/${retryTarget.max_retries}` },
              { id: 'last-error', label: 'Last error', value: retryTarget.last_error ?? 'None' },
              { id: 'status-message', label: 'Status message', value: retryTarget.status_message ?? 'None' },
            ]}
          />
        )}
      </DangerActionAlertDialog>

      <DetailTextDialog
        title={detailDialog?.title ?? 'Task Details'}
        text={detailDialog?.text ?? null}
        onClose={() => setDetailDialog(null)}
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
