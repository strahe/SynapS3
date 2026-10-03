import { useMutation, useQueryClient } from '@tanstack/react-query'
import { createFileRoute, useNavigate } from '@tanstack/react-router'
import { ChevronLeft, ChevronRight, ListTodo, Loader2, RefreshCw, RotateCcw, X } from 'lucide-react'
import { Fragment, useEffect, useRef, useState } from 'react'

import { api, type TaskItem } from '@/api/client'
import { CopyableValue } from '@/components/app/CopyableValue'
import { DangerActionAlertDialog } from '@/components/app/DangerActionAlertDialog'
import { PageErrorState } from '@/components/app/PageErrorState'
import { PageHeader } from '@/components/app/PageHeader'
import { StatusBadge, taskStatusTone } from '@/components/app/StatusBadge'
import { StorageConfirmationDetails } from '@/components/tasks/StorageConfirmationDetails'
import { TaskSubject } from '@/components/tasks/TaskSubject'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from '@/components/ui/empty'
import { Label } from '@/components/ui/label'
import { Pagination, PaginationContent, PaginationItem } from '@/components/ui/pagination'
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Skeleton } from '@/components/ui/skeleton'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { useTasks } from '@/hooks/queries'
import { storageConfirmationAttentionView } from '@/lib/storage-confirmation-attention'
import { taskOperationLabel, taskTook } from '@/lib/tasks'
import { timeAgo } from '@/lib/utils'

const PAGE_SIZE = 20

const taskOperations = [
  { value: 'all', label: 'All operations' },
  { value: 'bucket_provision', label: taskOperationLabel('bucket_provision') },
  { value: 'upload_plan', label: taskOperationLabel('upload_plan') },
  { value: 'storage_dataset_ensure', label: taskOperationLabel('storage_dataset_ensure') },
  { value: 'storage_transfer_plan', label: taskOperationLabel('storage_transfer_plan') },
  { value: 'storage_store', label: taskOperationLabel('storage_store') },
  { value: 'storage_pull', label: taskOperationLabel('storage_pull') },
  { value: 'storage_commit', label: taskOperationLabel('storage_commit') },
  { value: 'provider_replacement_coordinate', label: taskOperationLabel('provider_replacement_coordinate') },
  { value: 'cache_evict', label: taskOperationLabel('cache_evict') },
  { value: 'cache_reconcile_durability', label: taskOperationLabel('cache_reconcile_durability') },
  { value: 'storage_cleanup', label: taskOperationLabel('storage_cleanup') },
  { value: 'storage_dataset_retire', label: taskOperationLabel('storage_dataset_retire') },
  { value: 'wallet_operation', label: taskOperationLabel('wallet_operation') },
  { value: 'provider_upload_speed_test', label: taskOperationLabel('provider_upload_speed_test') },
] as const

const taskStatuses = [
  { value: 'all', label: 'All statuses' },
  { value: 'pending', label: 'Pending' },
  { value: 'running', label: 'Running' },
  { value: 'completed', label: 'Completed' },
  { value: 'failed', label: 'Failed' },
  { value: 'dismissed', label: 'Dismissed' },
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
  // What the operator is asked to confirm: the server's own count, the moment
  // it counted, and the operation it counted for. Confirming sends all three
  // back, so nothing that failed while the dialog was open is swept up.
  const [dismissAllScope, setDismissAllScope] = useState<DismissAllScope | null>(null)

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
  const previewDismissAll = useMutation({
    mutationFn: api.previewAcknowledgeTasks,
    onSuccess: (preview, variables) => setDismissAllScope({ ...preview, type: variables.type }),
  })
  const dismissAll = useMutation({
    mutationFn: api.acknowledgeTasks,
    onSuccess: () => {
      setDismissAllScope(null)
      refreshTasks()
    },
  })
  const actionError = retry.error ?? acknowledge.error ?? previewDismissAll.error

  const setFilters = (nextType: TaskOperationFilter, nextStatus: TaskStatusFilter) => {
    retry.reset()
    acknowledge.reset()
    previewDismissAll.reset()
    dismissAll.reset()
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
        actions={
          <div className="flex items-center gap-2">
            {status === 'failed' && (
              <Button
                variant="outline"
                size="sm"
                disabled={!tasks.data?.tasks.length || previewDismissAll.isPending}
                onClick={() => {
                  dismissAll.reset()
                  previewDismissAll.mutate({ type: search.type })
                }}
              >
                <X data-icon="inline-start" />
                Dismiss all
              </Button>
            )}
            <Button variant="outline" size="sm" onClick={() => tasks.refetch()} disabled={tasks.isFetching}>
              <RefreshCw data-icon="inline-start" className={tasks.isFetching ? 'animate-spin' : undefined} />
              Refresh
            </Button>
          </div>
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

      <DangerActionAlertDialog
        open={dismissAllScope !== null}
        onOpenChange={(open) => {
          if (open) return
          dismissAll.reset()
          setDismissAllScope(null)
        }}
        title="Dismiss failed tasks"
        description={dismissAllDescription(dismissAllScope?.count ?? 0, dismissAllScope?.type)}
        confirmLabel={dismissAllScope?.count === 1 ? 'Dismiss 1 task' : `Dismiss ${dismissAllScope?.count ?? 0} tasks`}
        pending={dismissAll.isPending}
        confirmDisabled={dismissAllScope?.count === 0}
        error={dismissAll.error ? errorMessage(dismissAll.error) : null}
        onConfirm={() => {
          if (!dismissAllScope) return
          dismissAll.mutate({ type: dismissAllScope.type, failed_before: dismissAllScope.as_of })
        }}
      />
    </div>
  )
}

type DismissAllScope = { count: number; as_of: string; type?: string }

function dismissAllDescription(count: number, operation?: string) {
  const label = operation ? (taskOperations.find((option) => option.value === operation)?.label ?? operation) : ''
  const scope = label ? ` for ${label}` : ''
  if (count === 0) return `No failed tasks${scope} are left to dismiss.`
  const tasks = count === 1 ? '1 failed task' : `${count} failed tasks`
  return `${tasks}${scope} will move to Dismissed and be removed after the retention period. Tasks that fail after you confirm stay in the list, and nothing is retried.`
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
            <TableHead className="w-28 px-4">Created</TableHead>
            <TableHead className="w-20 px-4">
              <Tooltip>
                <TooltipTrigger asChild>
                  <button type="button">Took</button>
                </TooltipTrigger>
                <TooltipContent className="max-w-72">
                  Time from first start to finish, including waits, retries, and time awaiting recovery.
                </TooltipContent>
              </Tooltip>
            </TableHead>
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
                <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
                  <StatusBadge tone={taskStatusTone(task.presentation_status)}>
                    {presentationLabels[task.presentation_status]}
                  </StatusBadge>
                  {task.retry_count > 0 && (
                    <span className="whitespace-nowrap text-xs text-muted-foreground">{taskRetriesLabel(task)}</span>
                  )}
                </div>
              </TableCell>
              <TableCell className="px-4">
                <TaskSubject task={task} />
              </TableCell>
              <TableCell className="px-4 text-muted-foreground">
                <TaskCreatedTime task={task} />
              </TableCell>
              <TableCell className="w-20 whitespace-nowrap px-4 tabular-nums text-muted-foreground">
                {taskTook(task)}
              </TableCell>
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
                      {task.type === 'storage_store' ? 'Retry upload' : 'Recover'}
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
  const confirmation = task.storage_confirmation
  if (confirmation) {
    const attention = storageConfirmationAttentionView(confirmation.reason_code)
    return (
      <div className="flex flex-col gap-1">
        <span className="text-sm text-status-warning">{attention.label}</span>
        {confirmation.submit_error && (
          <CopyableValue
            value={confirmation.submit_error}
            label="Provider response"
            displayValue={confirmation.submit_error}
            maxLength={80}
          />
        )}
        <details className="text-sm">
          <summary className="cursor-pointer text-muted-foreground">Confirmation details</summary>
          <div className="mt-2">
            <StorageConfirmationDetails confirmation={confirmation} />
          </div>
        </details>
      </div>
    )
  }
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

// Created, not last updated: a waiting task is rechecked every minute, so its
// update time says nothing about when the work was requested or how long it has waited.
function TaskCreatedTime({ task }: { task: TaskItem }) {
  const lifecycle: Array<[label: string, time: string | undefined]> = [
    ['Created', task.created_at],
    ['Started', task.started_at],
    ['Finished', task.finished_at],
    [
      'Next attempt',
      task.status === 'pending' && new Date(task.available_at).getTime() > Date.now() ? task.available_at : undefined,
    ],
  ]
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className="whitespace-nowrap">{timeAgo(task.created_at)}</span>
      </TooltipTrigger>
      <TooltipContent>
        <dl className="grid grid-cols-[auto_auto] gap-x-3 gap-y-0.5">
          {lifecycle
            .filter((entry): entry is [string, string] => Boolean(entry[1]))
            .map(([label, time]) => (
              <Fragment key={label}>
                <dt>{label}</dt>
                <dd>{new Date(time).toLocaleString()}</dd>
              </Fragment>
            ))}
        </dl>
      </TooltipContent>
    </Tooltip>
  )
}

function taskRetriesLabel(task: TaskItem) {
  if (task.retry_limit !== undefined) return `${task.retry_count}/${task.retry_limit} retries`
  return task.retry_count === 1 ? '1 retry' : `${task.retry_count} retries`
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
