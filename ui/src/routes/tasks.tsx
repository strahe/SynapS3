import { useMutation, useQueryClient } from '@tanstack/react-query'
import { createFileRoute, useNavigate } from '@tanstack/react-router'
import { Check, CheckCheck, ListTodo, Loader2 } from 'lucide-react'
import { Fragment, useEffect, useRef, useState } from 'react'
import { toast } from 'sonner'

import { api, type TaskItem, type TaskScope } from '@/api/client'
import { CopyableValue } from '@/components/app/CopyableValue'
import { CursorPagination } from '@/components/app/CursorPagination'
import { DangerActionAlertDialog } from '@/components/app/DangerActionAlertDialog'
import {
  clickableRowProps,
  DataTableFrame,
  RowDetailsButton,
  TableSkeleton,
  tableHeaderRowClassName,
} from '@/components/app/DataTable'
import { PageHeader, RefreshButton } from '@/components/app/PageHeader'
import { EmptyState, PageError } from '@/components/app/PageState'
import { StatusBadge, taskStatusTone } from '@/components/app/StatusBadge'
import { RetryButton } from '@/components/tasks/RetryButton'
import { StorageConfirmationDetails } from '@/components/tasks/StorageConfirmationDetails'
import { TaskDetailsDialog } from '@/components/tasks/TaskDetailsDialog'
import { TaskSubject } from '@/components/tasks/TaskSubject'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { useRetryTask, useTasks } from '@/hooks/queries'
import { storageConfirmationAttentionView } from '@/lib/storage-confirmation-attention'
import { taskDetailsView, taskOperationLabel, taskRetryErrorMessage, taskStatusesForScope, taskTook } from '@/lib/tasks'
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
  { value: 'cache_capacity_reconcile', label: taskOperationLabel('cache_capacity_reconcile') },
  { value: 'observability_refresh', label: taskOperationLabel('observability_refresh') },
  { value: 'approved_provider_refresh', label: taskOperationLabel('approved_provider_refresh') },
  { value: 'endorsed_provider_refresh', label: taskOperationLabel('endorsed_provider_refresh') },
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
}

type TaskOperationFilter = (typeof taskOperations)[number]['value']
type TaskStatusFilter = (typeof taskStatuses)[number]['value']
type TasksSearch = {
  scope?: TaskScope
  type?: Exclude<TaskOperationFilter, 'all'>
  status?: Exclude<TaskStatusFilter, 'all'>
}

const taskOperationValues = new Set<string>(
  taskOperations.map((option) => option.value).filter((value) => value !== 'all')
)

export const Route = createFileRoute('/tasks')({
  validateSearch: (search: Record<string, unknown>): TasksSearch => {
    const scope = search.scope === 'history' ? 'history' : 'work'
    return {
      scope,
      type:
        typeof search.type === 'string' && taskOperationValues.has(search.type)
          ? (search.type as TasksSearch['type'])
          : undefined,
      status:
        typeof search.status === 'string' && taskStatusesForScope(scope).includes(search.status as TaskItem['status'])
          ? (search.status as TasksSearch['status'])
          : undefined,
    }
  },
  component: TasksPage,
})

function TasksPage() {
  const search = Route.useSearch()
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const [cursor, setCursor] = useState<number>()
  const [cursorHistory, setCursorHistory] = useState<Array<number | undefined>>([])
  const scope = search.scope ?? 'work'
  const taskType = search.type ?? ''
  const status = search.status ?? ''
  const filterKey = `${scope}:${taskType}:${status}`
  const previousFilterKey = useRef(filterKey)
  const tasks = useTasks(
    scope,
    taskType,
    status,
    PAGE_SIZE,
    previousFilterKey.current === filterKey ? cursor : undefined
  )
  // What the operator is asked to confirm: the server's own count, the moment
  // it counted, and the operation it counted for. Confirming sends all three
  // back, so nothing that failed while the dialog was open is swept up.
  const [acknowledgeAllScope, setAcknowledgeAllScope] = useState<AcknowledgeAllScope | null>(null)

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
  const retry = useRetryTask()
  const [detailTaskID, setDetailTaskID] = useState<number | null>(null)
  const acknowledge = useMutation({
    mutationFn: api.acknowledgeTask,
    onSuccess: (_, id) => {
      toast.success(`Task ${id} acknowledged`)
      refreshTasks()
    },
  })
  const previewAcknowledgeAll = useMutation({
    mutationFn: api.previewAcknowledgeTasks,
    onSuccess: (preview, variables) => setAcknowledgeAllScope({ ...preview, type: variables.type }),
  })
  const acknowledgeAll = useMutation({
    mutationFn: api.acknowledgeTasks,
    onSuccess: () => {
      const count = acknowledgeAllScope?.count ?? 0
      toast.success(count === 1 ? '1 failed task acknowledged' : `${count} failed tasks acknowledged`)
      setAcknowledgeAllScope(null)
      refreshTasks()
    },
  })
  const actionError = retry.error ?? acknowledge.error ?? previewAcknowledgeAll.error

  const setFilters = (nextType: TaskOperationFilter, nextStatus: TaskStatusFilter, nextScope: TaskScope = scope) => {
    retry.reset()
    acknowledge.reset()
    previewAcknowledgeAll.reset()
    acknowledgeAll.reset()
    navigate({
      to: '/tasks',
      search: {
        scope: nextScope,
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

  return (
    <div className="flex flex-col gap-6 p-6">
      <PageHeader
        title="Tasks"
        actions={
          <>
            <RefreshButton onClick={() => tasks.refetch()} refreshing={tasks.isFetching} />
            {scope === 'work' && status === 'failed' && (
              <Button
                variant="outline"
                size="sm"
                disabled={!tasks.data?.tasks.length || previewAcknowledgeAll.isPending}
                onClick={() => {
                  acknowledgeAll.reset()
                  previewAcknowledgeAll.mutate({ type: search.type })
                }}
              >
                <CheckCheck data-icon="inline-start" />
                Acknowledge all
              </Button>
            )}
          </>
        }
      />

      <Tabs
        value={scope}
        onValueChange={(value) => setFilters(search.type ?? 'all', 'all', value as TaskScope)}
        className="gap-6"
      >
        <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
          <TabsList aria-label="Tasks" className="max-w-full justify-start overflow-x-auto">
            <TabsTrigger value="work">Open</TabsTrigger>
            <TabsTrigger value="history">Closed</TabsTrigger>
          </TabsList>
          <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
            <div className="flex items-center gap-2">
              <Label htmlFor="task-operation-filter" className="text-sm text-muted-foreground">
                Operation
              </Label>
              <Select
                value={(search.type ?? 'all') as TaskOperationFilter}
                onValueChange={(value) =>
                  setFilters(value as TaskOperationFilter, (search.status ?? 'all') as TaskStatusFilter)
                }
              >
                <SelectTrigger id="task-operation-filter" className="w-52">
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
            <div className="flex items-center gap-2">
              <Label htmlFor="task-status-filter" className="text-sm text-muted-foreground">
                Status
              </Label>
              <Select
                value={(search.status ?? 'all') as TaskStatusFilter}
                onValueChange={(value) =>
                  setFilters((search.type ?? 'all') as TaskOperationFilter, value as TaskStatusFilter)
                }
              >
                <SelectTrigger id="task-status-filter" className="w-40">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectGroup>
                    {taskStatuses
                      .filter((option) => option.value === 'all' || taskStatusesForScope(scope).includes(option.value))
                      .map((option) => (
                        <SelectItem key={option.value} value={option.value}>
                          {option.label}
                        </SelectItem>
                      ))}
                  </SelectGroup>
                </SelectContent>
              </Select>
            </div>
          </div>
        </div>
        <TabsContent value={scope} className="flex flex-col gap-6">
          {actionError && (
            <Alert variant="destructive">
              <AlertTitle>Task action failed</AlertTitle>
              <AlertDescription>
                {retry.error ? taskRetryErrorMessage(retry.error) : errorMessage(actionError)}
              </AlertDescription>
            </Alert>
          )}

          {tasks.error ? (
            <PageError
              title="Failed to load tasks"
              description={errorMessage(tasks.error)}
              onRetry={() => tasks.refetch()}
              retrying={tasks.isFetching}
            />
          ) : tasks.isLoading ? (
            <TableSkeleton />
          ) : tasks.data?.tasks.length ? (
            <>
              <TaskTable
                tasks={tasks.data.tasks}
                retryingID={retry.isPending ? retry.variables : undefined}
                acknowledgingID={acknowledge.isPending ? acknowledge.variables : undefined}
                onOpen={setDetailTaskID}
                onRetry={(id) => {
                  acknowledge.reset()
                  retry.mutate(id, { onSuccess: () => toast.success(`Retry started for task ${id}`) })
                }}
                onAcknowledge={(id) => {
                  retry.reset()
                  acknowledge.mutate(id)
                }}
              />
              <CursorPagination
                hasPrevious={cursorHistory.length > 0}
                hasNext={Boolean(tasks.data.next_cursor)}
                onPrevious={previousPage}
                onNext={nextPage}
              />
            </>
          ) : (
            <EmptyState
              icon={<ListTodo />}
              title="No tasks found"
              description="Try another operation or status filter."
            />
          )}
        </TabsContent>
      </Tabs>

      <TaskDetailsDialog
        taskID={detailTaskID}
        onOpenChange={(open) => {
          if (!open) setDetailTaskID(null)
        }}
      />
      <DangerActionAlertDialog
        open={acknowledgeAllScope !== null}
        onOpenChange={(open) => {
          if (open) return
          acknowledgeAll.reset()
          setAcknowledgeAllScope(null)
        }}
        title="Acknowledge failed tasks"
        description={acknowledgeAllDescription(acknowledgeAllScope?.count ?? 0, acknowledgeAllScope?.type)}
        confirmLabel={
          acknowledgeAllScope?.count === 1
            ? 'Acknowledge 1 task'
            : `Acknowledge ${acknowledgeAllScope?.count ?? 0} tasks`
        }
        pending={acknowledgeAll.isPending}
        confirmDisabled={acknowledgeAllScope?.count === 0}
        error={acknowledgeAll.error ? errorMessage(acknowledgeAll.error) : null}
        onConfirm={() => {
          if (!acknowledgeAllScope) return
          acknowledgeAll.mutate({ type: acknowledgeAllScope.type, failed_before: acknowledgeAllScope.as_of })
        }}
      />
    </div>
  )
}

type AcknowledgeAllScope = { count: number; as_of: string; type?: string }

function acknowledgeAllDescription(count: number, operation?: string) {
  const label = operation ? (taskOperations.find((option) => option.value === operation)?.label ?? operation) : ''
  const scope = label ? ` for ${label}` : ''
  if (count === 0) return `No failed tasks${scope} are left to acknowledge.`
  const tasks = count === 1 ? '1 failed task' : `${count} failed tasks`
  return `${tasks}${scope} will move to Closed. Their results and Retry availability remain unchanged.`
}

function TaskTable({
  tasks,
  retryingID,
  acknowledgingID,
  onOpen,
  onRetry,
  onAcknowledge,
}: {
  tasks: TaskItem[]
  retryingID?: number
  acknowledgingID?: number
  onOpen: (id: number) => void
  onRetry: (id: number) => void
  onAcknowledge: (id: number) => void
}) {
  return (
    <DataTableFrame>
      <Table className="min-w-[1080px]">
        <TableHeader>
          <TableRow className={tableHeaderRowClassName}>
            <TableHead className="w-20 px-4">ID</TableHead>
            <TableHead className="px-4">Operation</TableHead>
            <TableHead className="px-4">Status</TableHead>
            <TableHead className="px-4">Subject</TableHead>
            <TableHead className="w-28 px-4">Created</TableHead>
            <TableHead className="w-20 px-4">
              <Tooltip>
                <TooltipTrigger asChild>
                  <button type="button" className="cursor-help underline decoration-dotted underline-offset-4">
                    Took
                  </button>
                </TooltipTrigger>
                <TooltipContent className="max-w-72">
                  Time from start to finish, including later waits, retries, and recovery.
                </TooltipContent>
              </Tooltip>
            </TableHead>
            <TableHead className="min-w-64 px-4">Details</TableHead>
            <TableHead className="px-4 text-right">
              <span className="sr-only">Actions</span>
            </TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {tasks.map((task) => (
            <TableRow key={task.id} {...clickableRowProps(() => onOpen(task.id))}>
              <TableCell className="px-4 font-mono text-xs tabular-nums">{task.id}</TableCell>
              <TableCell className="px-4 font-medium">{task.operation}</TableCell>
              <TableCell className="px-4">
                <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
                  <StatusBadge tone={taskStatusTone(task.presentation_status)}>
                    {presentationLabels[task.presentation_status]}
                  </StatusBadge>
                  {task.acknowledged_at && <span className="text-xs text-muted-foreground">Acknowledged</span>}
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
                <div className="flex items-center justify-end gap-2">
                  {task.retryable && (
                    <RetryButton
                      taskID={task.retry_task_id}
                      pending={retryingID === task.retry_task_id}
                      disabled={retryingID !== undefined || acknowledgingID !== undefined}
                      onRetry={onRetry}
                    />
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
                        <Check data-icon="inline-start" />
                      )}
                      Acknowledge
                    </Button>
                  )}
                  <RowDetailsButton label={`task ${task.id}`} onClick={() => onOpen(task.id)} />
                </div>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </DataTableFrame>
  )
}

function TaskDetails({ task }: { task: TaskItem }) {
  const details = taskDetailsView(task)
  const confirmation = task.storage_confirmation
  const attention = confirmation ? storageConfirmationAttentionView(confirmation.reason_code) : undefined
  return (
    <div className="flex flex-col gap-1">
      {details.value ? (
        <CopyableValue value={details.value} label={details.label} displayValue={details.value} maxLength={80} />
      ) : !confirmation ? (
        <span className="text-muted-foreground">—</span>
      ) : null}
      {confirmation && (
        <>
          {attention && attention.label !== details.value && (
            <span className="text-sm text-status-warning">{attention.label}</span>
          )}
          {confirmation.submit_error && (
            <CopyableValue
              value={confirmation.submit_error}
              label="Provider response"
              displayValue={confirmation.submit_error}
              maxLength={80}
            />
          )}
          <details className="text-sm">
            <summary className="cursor-pointer text-muted-foreground">Batch details</summary>
            <div className="mt-2">
              <StorageConfirmationDetails confirmation={confirmation} />
            </div>
          </details>
        </>
      )}
      {details.lastError && (
        <details className="text-sm">
          <summary className="cursor-pointer text-muted-foreground">Last error</summary>
          <div className="mt-2">
            <CopyableValue
              value={details.lastError}
              label="Last error"
              displayValue={details.lastError}
              maxLength={80}
            />
          </div>
        </details>
      )}
    </div>
  )
}

// Created, not last updated: a waiting task is rechecked every minute, so its
// update time says nothing about when the work was requested or how long it has waited.
function TaskCreatedTime({ task }: { task: TaskItem }) {
  const lifecycle: Array<[label: string, time: string | undefined]> = [
    ['Created', task.created_at],
    [task.type === 'storage_store' ? 'Upload started' : 'Started', task.started_at],
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
  if (task.max_attempts != null) return `${task.retry_count}/${task.max_attempts - 1} retries`
  return task.retry_count === 1 ? '1 retry' : `${task.retry_count} retries`
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : 'Please try again.'
}
