import { useMutation, useQueryClient } from '@tanstack/react-query'
import { createFileRoute, useNavigate } from '@tanstack/react-router'
import { ChevronLeft, ChevronRight, ListTodo, Loader2, RefreshCw, RotateCcw, X } from 'lucide-react'
import { useEffect, useRef, useState } from 'react'

import { api, type TaskItem } from '@/api/client'
import { CopyableValue } from '@/components/app/CopyableValue'
import { PageErrorState } from '@/components/app/PageErrorState'
import { PageHeader } from '@/components/app/PageHeader'
import { StatusBadge, taskStatusTone } from '@/components/app/StatusBadge'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from '@/components/ui/empty'
import { Label } from '@/components/ui/label'
import { Pagination, PaginationContent, PaginationItem } from '@/components/ui/pagination'
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Skeleton } from '@/components/ui/skeleton'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { useTasks } from '@/hooks/queries'
import { timeAgo } from '@/lib/utils'

const PAGE_SIZE = 20

const taskOperations = [
  { value: 'all', label: 'All operations' },
  { value: 'bucket_provision', label: 'Prepare bucket storage' },
  { value: 'upload_plan', label: 'Prepare upload' },
  { value: 'storage_dataset_ensure', label: 'Prepare storage' },
  { value: 'storage_transfer_plan', label: 'Plan storage transfer' },
  { value: 'storage_store', label: 'Store content' },
  { value: 'storage_pull', label: 'Transfer stored content' },
  { value: 'storage_commit_coordinate', label: 'Prepare storage confirmation' },
  { value: 'storage_commit', label: 'Confirm storage' },
  { value: 'provider_replacement_coordinate', label: 'Replace storage provider' },
  { value: 'cache_evict', label: 'Remove local cached copy' },
  { value: 'cache_reconcile_durability', label: 'Review cache durability' },
  { value: 'storage_cleanup', label: 'Remove remote storage copy' },
  { value: 'storage_dataset_retire', label: 'Retire storage service' },
  { value: 'wallet_operation', label: 'Process wallet request' },
] as const

const taskStatuses = [
  { value: 'all', label: 'All statuses' },
  { value: 'pending', label: 'Pending' },
  { value: 'running', label: 'Running' },
  { value: 'completed', label: 'Completed' },
  { value: 'failed', label: 'Failed' },
  { value: 'cancelled', label: 'Cancelled' },
] as const

const presentationLabels: Record<TaskItem['presentation_status'], string> = {
  queued: 'Queued',
  scheduled: 'Scheduled',
  waiting: 'Waiting',
  running: 'Running',
  completed: 'Completed',
  failed: 'Failed',
  cancelled: 'Cancelled',
  dismissed: 'Dismissed',
}

type TaskOperationFilter = (typeof taskOperations)[number]['value']
type TaskStatusFilter = (typeof taskStatuses)[number]['value']
type TasksSearch = {
  type?: Exclude<TaskOperationFilter, 'all'>
  status?: Exclude<TaskStatusFilter, 'all'>
}

const taskOperationValues = new Set<string>(
  taskOperations.map((option) => option.value).filter((value) => value !== 'all')
)
const taskStatusValues = new Set<string>(taskStatuses.map((option) => option.value).filter((value) => value !== 'all'))

export const Route = createFileRoute('/tasks')({
  validateSearch: (search: Record<string, unknown>): TasksSearch => ({
    type:
      typeof search.type === 'string' && taskOperationValues.has(search.type)
        ? (search.type as TasksSearch['type'])
        : undefined,
    status:
      typeof search.status === 'string' && taskStatusValues.has(search.status)
        ? (search.status as TasksSearch['status'])
        : undefined,
  }),
  component: TasksPage,
})

function TasksPage() {
  const search = Route.useSearch()
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const [cursor, setCursor] = useState<number>()
  const [cursorHistory, setCursorHistory] = useState<Array<number | undefined>>([])
  const taskType = search.type ?? ''
  const status = search.status ?? ''
  const filterKey = `${taskType}:${status}`
  const previousFilterKey = useRef(filterKey)
  const tasks = useTasks(taskType, status, PAGE_SIZE, cursor)

  useEffect(() => {
    if (previousFilterKey.current === filterKey) return
    previousFilterKey.current = filterKey
    setCursor(undefined)
    setCursorHistory([])
  }, [filterKey])

  const refreshTasks = () => {
    queryClient.invalidateQueries({ queryKey: ['tasks'] })
    queryClient.invalidateQueries({ queryKey: ['taskStats'] })
  }
  const retry = useMutation({ mutationFn: api.retryTask, onSuccess: refreshTasks })
  const acknowledge = useMutation({ mutationFn: api.acknowledgeTask, onSuccess: refreshTasks })
  const actionError = retry.error ?? acknowledge.error

  const setFilters = (nextType: TaskOperationFilter, nextStatus: TaskStatusFilter) => {
    retry.reset()
    acknowledge.reset()
    navigate({
      to: '/tasks',
      search: {
        type: nextType === 'all' ? undefined : nextType,
        status: nextStatus === 'all' ? undefined : nextStatus,
      },
      replace: true,
    })
  }

  const nextPage = () => {
    if (!tasks.data?.next_cursor) return
    setCursorHistory((current) => [...current, cursor])
    setCursor(tasks.data.next_cursor)
  }

  const previousPage = () => {
    const previous = cursorHistory[cursorHistory.length - 1]
    setCursorHistory((current) => current.slice(0, -1))
    setCursor(previous)
  }

  if (tasks.error) {
    return <PageErrorState title="Unable to load tasks" description={errorMessage(tasks.error)} />
  }

  return (
    <div className="flex flex-col gap-6 p-6">
      <PageHeader
        title="Tasks"
        description="Review background operations and recover work that needs attention."
        actions={
          <Button variant="outline" size="sm" onClick={() => tasks.refetch()} disabled={tasks.isFetching}>
            <RefreshCw data-icon="inline-start" className={tasks.isFetching ? 'animate-spin' : undefined} />
            Refresh
          </Button>
        }
      />

      <div className="flex flex-wrap items-end gap-3">
        <div className="flex flex-col gap-1.5">
          <Label htmlFor="task-operation-filter">Operation</Label>
          <Select
            value={(search.type ?? 'all') as TaskOperationFilter}
            onValueChange={(value) =>
              setFilters(value as TaskOperationFilter, (search.status ?? 'all') as TaskStatusFilter)
            }
          >
            <SelectTrigger id="task-operation-filter" className="w-64">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectGroup>
                {taskOperations.map((option) => (
                  <SelectItem key={option.value} value={option.value}>
                    {option.label}
                  </SelectItem>
                ))}
              </SelectGroup>
            </SelectContent>
          </Select>
        </div>
        <div className="flex flex-col gap-1.5">
          <Label htmlFor="task-status-filter">Status</Label>
          <Select
            value={(search.status ?? 'all') as TaskStatusFilter}
            onValueChange={(value) =>
              setFilters((search.type ?? 'all') as TaskOperationFilter, value as TaskStatusFilter)
            }
          >
            <SelectTrigger id="task-status-filter" className="w-44">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectGroup>
                {taskStatuses.map((option) => (
                  <SelectItem key={option.value} value={option.value}>
                    {option.label}
                  </SelectItem>
                ))}
              </SelectGroup>
            </SelectContent>
          </Select>
        </div>
      </div>

      {actionError && (
        <Alert variant="destructive">
          <AlertTitle>Task action failed</AlertTitle>
          <AlertDescription>{errorMessage(actionError)}</AlertDescription>
        </Alert>
      )}

      {tasks.isLoading ? (
        <TaskTableSkeleton />
      ) : tasks.data?.tasks.length ? (
        <>
          <TaskTable
            tasks={tasks.data.tasks}
            retryingID={retry.isPending ? retry.variables : undefined}
            acknowledgingID={acknowledge.isPending ? acknowledge.variables : undefined}
            onRetry={(id) => {
              acknowledge.reset()
              retry.mutate(id)
            }}
            onAcknowledge={(id) => {
              retry.reset()
              acknowledge.mutate(id)
            }}
          />
          {(cursorHistory.length > 0 || tasks.data.next_cursor) && (
            <Pagination>
              <PaginationContent>
                <PaginationItem>
                  <Button variant="outline" size="sm" onClick={previousPage} disabled={cursorHistory.length === 0}>
                    <ChevronLeft data-icon="inline-start" />
                    Previous
                  </Button>
                </PaginationItem>
                <PaginationItem>
                  <Button variant="outline" size="sm" onClick={nextPage} disabled={!tasks.data.next_cursor}>
                    Next
                    <ChevronRight data-icon="inline-end" />
                  </Button>
                </PaginationItem>
              </PaginationContent>
            </Pagination>
          )}
        </>
      ) : (
        <Empty>
          <EmptyHeader>
            <EmptyMedia variant="icon">
              <ListTodo />
            </EmptyMedia>
            <EmptyTitle>No tasks found</EmptyTitle>
            <EmptyDescription>Try another operation or status filter.</EmptyDescription>
          </EmptyHeader>
        </Empty>
      )}
    </div>
  )
}

function TaskTable({
  tasks,
  retryingID,
  acknowledgingID,
  onRetry,
  onAcknowledge,
}: {
  tasks: TaskItem[]
  retryingID?: number
  acknowledgingID?: number
  onRetry: (id: number) => void
  onAcknowledge: (id: number) => void
}) {
  return (
    <div className="overflow-hidden rounded-lg border border-border">
      <Table>
        <TableHeader>
          <TableRow className="bg-muted/50">
            <TableHead className="w-20 px-4">ID</TableHead>
            <TableHead className="px-4">Operation</TableHead>
            <TableHead className="px-4">Status</TableHead>
            <TableHead className="px-4">Subject</TableHead>
            <TableHead className="w-24 px-4">Retries</TableHead>
            <TableHead className="w-28 px-4">Updated</TableHead>
            <TableHead className="min-w-64 px-4">Details</TableHead>
            <TableHead className="px-4 text-right">Actions</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {tasks.map((task) => (
            <TableRow key={task.id}>
              <TableCell className="px-4">
                <CopyableValue value={String(task.id)} label="Task ID" monospace />
              </TableCell>
              <TableCell className="px-4 font-medium">{task.operation}</TableCell>
              <TableCell className="px-4">
                <StatusBadge tone={taskStatusTone(task.presentation_status)}>
                  {presentationLabels[task.presentation_status]}
                </StatusBadge>
              </TableCell>
              <TableCell className="px-4">{taskSubject(task)}</TableCell>
              <TableCell className="px-4">
                {task.retry_limit === undefined ? task.retry_count : `${task.retry_count}/${task.retry_limit}`}
              </TableCell>
              <TableCell className="px-4 text-muted-foreground">{timeAgo(task.updated_at)}</TableCell>
              <TableCell className="max-w-96 px-4">
                <TaskDetails task={task} />
              </TableCell>
              <TableCell className="px-4">
                <div className="flex justify-end gap-2">
                  {task.retryable && (
                    <Button
                      variant="outline"
                      size="sm"
                      disabled={retryingID !== undefined || acknowledgingID !== undefined}
                      onClick={() => onRetry(task.id)}
                    >
                      {retryingID === task.id ? (
                        <Loader2 data-icon="inline-start" className="animate-spin" />
                      ) : (
                        <RotateCcw data-icon="inline-start" />
                      )}
                      Recover
                    </Button>
                  )}
                  {task.acknowledgeable && (
                    <Button
                      variant="ghost"
                      size="sm"
                      disabled={retryingID !== undefined || acknowledgingID !== undefined}
                      onClick={() => onAcknowledge(task.id)}
                    >
                      {acknowledgingID === task.id ? (
                        <Loader2 data-icon="inline-start" className="animate-spin" />
                      ) : (
                        <X data-icon="inline-start" />
                      )}
                      Dismiss
                    </Button>
                  )}
                </div>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  )
}

function TaskDetails({ task }: { task: TaskItem }) {
  const value = task.last_error || task.status_message || '—'
  if (value === '—') return <span className="text-muted-foreground">—</span>
  return (
    <CopyableValue
      value={value}
      label={task.last_error ? 'Task error' : 'Task details'}
      displayValue={value}
      maxLength={80}
    />
  )
}

function taskSubject(task: TaskItem) {
  if (!task.subject_type || !task.subject_key) return 'System'
  const labels: Record<string, string> = {
    object_version: 'Object version',
    bucket: 'Bucket',
    storage_copy: 'Storage copy',
    storage_data_set: 'Storage service',
    storage_upload: 'Stored content',
    wallet_operation: 'Wallet request',
    system: 'System',
  }
  return `${labels[task.subject_type] ?? 'Resource'} ${task.subject_key}`
}

function TaskTableSkeleton() {
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

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : 'Please try again.'
}
