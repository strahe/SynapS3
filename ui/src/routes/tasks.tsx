import { useMutation, useQueryClient } from '@tanstack/react-query'
import { createFileRoute } from '@tanstack/react-router'
import { Loader2, RefreshCw, RotateCcw } from 'lucide-react'
import { useState } from 'react'
import { api, type TaskItem } from '@/api/client'
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
import { formatBytes, timeAgo } from '@/lib/utils'

export const Route = createFileRoute('/tasks')({
  component: TasksPage,
})

const taskTypeTabs = ['all', 'upload', 'evict_cache'] as const
const taskTypeLabels: Record<string, string> = {
  all: 'All',
  upload: 'Upload',
  evict_cache: 'Evict Cache',
}

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

const stageOptions = [
  'all',
  'prepare_upload',
  'ensure_dataset',
  'primary_store',
  'primary_commit',
  'secondary_pull',
  'secondary_commit',
  'legacy_upload',
] as const

const PAGE_SIZE = 20

const taskStageLabels: Record<string, string> = {
  prepare_upload: 'Prepare upload',
  ensure_dataset: 'Prepare storage',
  primary_store: 'Transfer primary copy',
  primary_commit: 'Confirm primary copy',
  secondary_pull: 'Transfer replica',
  secondary_commit: 'Confirm replica',
  legacy_upload: 'Upload',
}

function taskStageLabel(task: TaskItem) {
  const stage = task.stage ?? ''
  const base = taskStageLabels[stage] ?? stage
  if (typeof task.copy_index === 'number') return `${base} · copy ${task.copy_index}`
  return base
}

function isPrimaryTransferTask(task: TaskItem) {
  return task.stage === 'primary_store' || task.stage === 'legacy_upload'
}

function taskDetailText(task: TaskItem) {
  return task.status_message || task.last_error || ''
}

function taskDetailTitle(task: TaskItem) {
  return task.last_error ? 'Error Details' : 'Status Details'
}

function TaskRefCell({ task }: { task: TaskItem }) {
  const [detailEnabled, setDetailEnabled] = useState(false)
  const detail = useTaskRefDetail(task.id, detailEnabled)
  const refLabel = `${task.ref_type}:${task.ref_id}`

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
          aria-label={`${refLabel} reference`}
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
    return <span>Loading reference</span>
  }
  if (detail.error || !detail.data?.object) {
    return <span>Reference unavailable</span>
  }

  const object = detail.data.object
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

function taskObjectLocationLabel(location: { cache: boolean; filecoin: boolean }) {
  if (location.cache && location.filecoin) return 'Cache + Filecoin'
  if (location.cache) return 'Cache'
  if (location.filecoin) return 'Filecoin'
  return 'None'
}

function TasksPage() {
  const [status, setStatus] = useState('')
  const [taskType, setTaskType] = useState('')
  const [stage, setStage] = useState('')
  const [offset, setOffset] = useState(0)
  const { data, isLoading, error } = useTasks(taskType, stage, status, PAGE_SIZE, offset)
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
          value={taskType || 'all'}
          onValueChange={(value) => {
            setTaskType(value === 'all' ? '' : value)
            if (value !== 'upload') setStage('')
            setOffset(0)
          }}
          className="min-w-0"
        >
          <TabsList className="max-w-full justify-start overflow-x-auto">
            {taskTypeTabs.map((tab) => (
              <TabsTrigger key={tab} value={tab}>
                {taskTypeLabels[tab]}
              </TabsTrigger>
            ))}
          </TabsList>
        </Tabs>

        <div className="flex flex-wrap items-center gap-2">
          <Label htmlFor="task-stage-filter" className="text-sm text-muted-foreground">
            Stage:
          </Label>
          <Select
            value={stage || 'all'}
            disabled={taskType !== '' && taskType !== 'upload'}
            onValueChange={(value) => {
              if (value === 'all') {
                setStage('')
              } else {
                setTaskType('upload')
                setStage(value)
              }
              setOffset(0)
            }}
          >
            <SelectTrigger id="task-stage-filter" className="w-48">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectGroup>
                {stageOptions.map((option) => (
                  <SelectItem key={option} value={option}>
                    {option === 'all' ? 'All' : (taskStageLabels[option] ?? option)}
                  </SelectItem>
                ))}
              </SelectGroup>
            </SelectContent>
          </Select>
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
          <div className="overflow-hidden rounded-lg border border-border">
            <Table>
              <TableHeader>
                <TableRow className="bg-muted/50">
                  <TableHead className="whitespace-nowrap px-3 py-2">ID</TableHead>
                  <TableHead className="whitespace-nowrap px-3 py-2">Type</TableHead>
                  <TableHead className="whitespace-nowrap px-3 py-2">Stage</TableHead>
                  <TableHead className="whitespace-nowrap px-3 py-2">Progress</TableHead>
                  <TableHead className="whitespace-nowrap px-3 py-2">Ref</TableHead>
                  <TableHead className="whitespace-nowrap px-3 py-2">Status</TableHead>
                  <TableHead className="whitespace-nowrap px-3 py-2 text-right">Retries</TableHead>
                  <TableHead className="whitespace-nowrap px-3 py-2">Details</TableHead>
                  <TableHead className="whitespace-nowrap px-3 py-2">Scheduled</TableHead>
                  <TableHead className="whitespace-nowrap px-3 py-2">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {data && data.tasks.length > 0 ? (
                  data.tasks.map((t) => (
                    <TableRow key={t.id}>
                      <TableCell className="whitespace-nowrap px-3 py-2 font-mono text-xs">{t.id}</TableCell>
                      <TableCell className="whitespace-nowrap px-3 py-2">{t.type}</TableCell>
                      <TableCell className="whitespace-nowrap px-3 py-2">
                        {t.stage ? (
                          <span className="font-mono text-xs">{taskStageLabel(t)}</span>
                        ) : (
                          <span className="text-muted-foreground">—</span>
                        )}
                      </TableCell>
                      <TableCell className="whitespace-nowrap px-3 py-2">
                        {isPrimaryTransferTask(t) ? (
                          <UploadProgressBar progress={t.progress} />
                        ) : (
                          <span className="text-muted-foreground">—</span>
                        )}
                      </TableCell>
                      <TableCell className="whitespace-nowrap px-3 py-2 text-muted-foreground">
                        <TaskRefCell task={t} />
                      </TableCell>
                      <TableCell className="whitespace-nowrap px-3 py-2">
                        <StatusBadge tone={taskStatusTone(t.status)}>{t.status}</StatusBadge>
                      </TableCell>
                      <TableCell className="whitespace-nowrap px-3 py-2 text-right">
                        {t.retry_count}/{t.max_retries}
                      </TableCell>
                      <TableCell className="max-w-xs whitespace-nowrap px-3 py-2 text-xs text-muted-foreground">
                        {taskDetailText(t) ? (
                          <Button
                            type="button"
                            variant="link"
                            onClick={() => {
                              setDetailDialog({ title: taskDetailTitle(t), text: taskDetailText(t) })
                            }}
                            className="h-auto max-w-full justify-start p-0 text-left text-xs font-normal text-muted-foreground hover:text-foreground"
                          >
                            <span className="truncate">{taskDetailText(t)}</span>
                          </Button>
                        ) : (
                          '—'
                        )}
                      </TableCell>
                      <TableCell className="whitespace-nowrap px-3 py-2 text-muted-foreground">
                        {timeAgo(t.scheduled_at)}
                      </TableCell>
                      <TableCell className="whitespace-nowrap px-3 py-2">
                        {t.status === 'exhausted' && (
                          <Button
                            type="button"
                            variant="outline"
                            size="xs"
                            onClick={() => openRetryDialog(t)}
                            disabled={retryPending}
                          >
                            <RotateCcw data-icon="inline-start" /> Retry
                          </Button>
                        )}
                      </TableCell>
                    </TableRow>
                  ))
                ) : (
                  <TableRow>
                    <TableCell colSpan={10} className="h-24 text-center text-muted-foreground">
                      No tasks found
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </div>

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
              { id: 'type', label: 'Type', value: retryTarget.type },
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
    default:
      return 'This will requeue the exhausted task for background processing.'
  }
}
