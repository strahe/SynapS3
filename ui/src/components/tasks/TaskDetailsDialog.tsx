import { useQuery } from '@tanstack/react-query'
import { Circle, CircleCheck, CircleX, Clock3, History } from 'lucide-react'
import { useState } from 'react'
import { api, type TaskItem } from '@/api/client'
import { CopyableValue } from '@/components/app/CopyableValue'
import { StatusBadge, taskStatusTone } from '@/components/app/StatusBadge'
import { RetryButton } from '@/components/tasks/RetryButton'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog'
import { Empty, EmptyHeader, EmptyMedia, EmptyTitle } from '@/components/ui/empty'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Separator } from '@/components/ui/separator'
import { Skeleton } from '@/components/ui/skeleton'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { taskDetailsView, taskOperationLabel, taskSubjectLabel, taskTook } from '@/lib/tasks'
import { timeAgo, titleCaseEnum } from '@/lib/utils'

const eventLabels: Record<string, string> = {
  created: 'Created',
  retry: 'Retry scheduled',
  retry_scheduled: 'Retry scheduled',
  completed: 'Completed',
  failed: 'Failed',
  cancelled: 'Cancelled',
  superseded: 'New execution created',
  acknowledged: 'Acknowledged',
  effect_admitted: 'Operation started',
  legacy_handoff: 'Execution upgraded',
  pending: 'Retry scheduled',
  retired: 'Retired',
  policy_replaced: 'Execution upgraded',
  waiting: 'Waiting',
  wait: 'Waiting',
}

function TaskStatus({ task }: { task: TaskItem }) {
  const status = task.presentation_status ?? task.status
  return <StatusBadge tone={taskStatusTone(status)}>{titleCaseEnum(status)}</StatusBadge>
}

function TaskTime({ value }: { value?: string }) {
  return value ? (
    <time dateTime={value} title={new Date(value).toLocaleString()}>
      {timeAgo(value)}
    </time>
  ) : (
    <>—</>
  )
}

function LoadingDetails() {
  return (
    <div role="status" aria-label="Loading details" className="flex flex-col gap-4">
      {[1, 2, 3, 4].map((row) => (
        <div key={row} className="flex items-center justify-between gap-4">
          <Skeleton className="h-4 w-2/5" />
          <Skeleton className="h-4 w-1/4" />
        </div>
      ))}
    </div>
  )
}

function LoadError({ label, onRetry }: { label: string; onRetry: () => void }) {
  return (
    <Alert variant="destructive">
      <CircleX />
      <AlertTitle>Could not load {label}.</AlertTitle>
      <AlertDescription>
        <Button variant="outline" size="sm" onClick={onRetry}>
          Try again
        </Button>
      </AlertDescription>
    </Alert>
  )
}

export function TaskDetailsDialog({ taskID }: { taskID: number }) {
  const [open, setOpen] = useState(false)
  const [historyCursor, setHistoryCursor] = useState<number>()
  const [eventCursor, setEventCursor] = useState<number>()
  const detail = useQuery({ queryKey: ['task', taskID], queryFn: () => api.getTask(taskID), enabled: open })
  const history = useQuery({
    queryKey: ['taskHistory', taskID, historyCursor],
    queryFn: () => api.getTaskHistory(taskID, historyCursor),
    enabled: open,
  })
  const events = useQuery({
    queryKey: ['taskEvents', taskID, eventCursor],
    queryFn: () => api.getTaskEvents(taskID, eventCursor),
    enabled: open,
  })
  const task = detail.data?.task
  const policy = detail.data?.policy
  const message = task ? taskDetailsView(task) : undefined
  return (
    <Dialog
      open={open}
      onOpenChange={(next) => {
        setOpen(next)
        if (!next) {
          setHistoryCursor(undefined)
          setEventCursor(undefined)
        }
      }}
    >
      <DialogTrigger asChild>
        <Button variant="ghost" size="sm">
          View
        </Button>
      </DialogTrigger>
      <DialogContent className="flex h-[min(38rem,calc(100dvh-2rem))] flex-col gap-4 overflow-hidden sm:max-w-2xl">
        <DialogHeader className="shrink-0 pr-8">
          <DialogTitle>Task {taskID}</DialogTitle>
          <DialogDescription>{task ? taskOperationLabel(task.type) : 'Task details'}</DialogDescription>
        </DialogHeader>
        {detail.isPending && <LoadingDetails />}
        {detail.isError && <LoadError label="task details" onRetry={() => void detail.refetch()} />}
        {task && (
          <>
            <div className="flex shrink-0 flex-wrap items-center justify-between gap-3">
              <div className="flex flex-wrap items-center gap-2">
                <TaskStatus task={task} />
                {task.acknowledged_at && <Badge variant="secondary">Acknowledged</Badge>}
                <span className="text-xs text-muted-foreground">{taskSubjectLabel(task)}</span>
              </div>
              {task.retryable && <RetryButton taskID={task.retry_task_id} />}
            </div>
            {message?.value && (
              <Alert variant={task.status === 'failed' ? 'destructive' : 'default'} className="shrink-0">
                {task.status === 'failed' ? <CircleX /> : <Clock3 />}
                <AlertTitle>{task.status === 'failed' ? 'Failure reason' : 'Status'}</AlertTitle>
                <AlertDescription className="min-w-0">
                  <CopyableValue value={message.value} label={message.label} maxLength={150} className="w-full" />
                </AlertDescription>
              </Alert>
            )}
            <Tabs defaultValue="overview" className="min-h-0 flex-1 gap-4">
              <TabsList className="w-full shrink-0" aria-label="Task details">
                <TabsTrigger value="overview">Overview</TabsTrigger>
                <TabsTrigger value="history">History</TabsTrigger>
                <TabsTrigger value="events">Events</TabsTrigger>
              </TabsList>
              <TabsContent value="overview" className="min-h-0 overflow-hidden">
                <ScrollArea className="h-full">
                  <div className="flex flex-col gap-5 pr-3">
                    <dl className="grid grid-cols-2 gap-x-6 gap-y-4">
                      {[
                        ['Created', <TaskTime key="created" value={task.created_at} />],
                        ['Started', <TaskTime key="started" value={task.started_at} />],
                        ['Finished', <TaskTime key="finished" value={task.finished_at} />],
                        ['Took', taskTook(task)],
                      ].map(([label, value]) => (
                        <div key={String(label)} className="flex min-w-0 flex-col gap-1">
                          <dt className="text-xs text-muted-foreground">{label}</dt>
                          <dd>{value}</dd>
                        </div>
                      ))}
                    </dl>
                    <Separator />
                    <section className="flex flex-col gap-3">
                      <div className="flex items-center justify-between gap-2">
                        <h3 className="font-medium">Execution policy</h3>
                        {policy?.legacy && <Badge variant="outline">Legacy</Badge>}
                      </div>
                      {policy ? (
                        <dl className="grid gap-3 sm:grid-cols-2">
                          {[
                            ['Current attempt', `${task.retry_count + 1} / ${policy.max_attempts ?? 'Legacy limit'}`],
                            ['Automatic retries', policy.max_attempts === 1 ? 'Disabled' : 'Enabled'],
                            ...(policy.max_attempts === 1
                              ? []
                              : [
                                  [
                                    'Retry delay',
                                    `${policy.initial_delay} × ${policy.multiplier}, up to ${policy.maximum_delay}`,
                                  ],
                                  ['Delay variation', `±${Math.round(policy.jitter * 100)}%`],
                                ]),
                            [
                              'Execution timeout',
                              policy.invocation_timeout === '0s' ? 'Not set' : policy.invocation_timeout,
                            ],
                            [
                              'Observation window',
                              policy.observation_window === '0s' ? 'Not set' : policy.observation_window,
                            ],
                          ].map(([label, value]) => (
                            <div key={label} className="flex min-w-0 flex-col gap-1 rounded-lg bg-muted/40 px-3 py-2.5">
                              <dt className="text-xs text-muted-foreground">{label}</dt>
                              <dd className="break-words tabular-nums">{value}</dd>
                            </div>
                          ))}
                        </dl>
                      ) : (
                        <p className="text-muted-foreground">Policy is unavailable for this record.</p>
                      )}
                    </section>
                    {message?.lastError && (
                      <details>
                        <summary className="cursor-pointer text-muted-foreground">Last error</summary>
                        <CopyableValue value={message.lastError} label="Last error" maxLength={150} className="mt-2" />
                      </details>
                    )}
                  </div>
                </ScrollArea>
              </TabsContent>
              <TabsContent value="history" className="min-h-0 overflow-hidden">
                <ScrollArea className="h-full">
                  <div className="flex flex-col gap-4 pr-3">
                    {history.isPending && <LoadingDetails />}
                    {history.isError && <LoadError label="history" onRetry={() => void history.refetch()} />}
                    <ol>
                      {history.data?.tasks.map((round, index) => (
                        <li key={round.id}>
                          {index > 0 && <Separator />}
                          <div className="flex items-start justify-between gap-4 py-3">
                            <div className="flex min-w-0 flex-col gap-1.5">
                              <div className="flex flex-wrap items-center gap-2">
                                <span className="font-medium">Task {round.id}</span>
                                {round.id === taskID && <Badge variant="secondary">Current</Badge>}
                              </div>
                              <span className="text-muted-foreground">{taskOperationLabel(round.type)}</span>
                              <span className="text-xs text-muted-foreground">
                                <TaskTime value={round.created_at} />
                              </span>
                            </div>
                            <TaskStatus task={round} />
                          </div>
                        </li>
                      ))}
                    </ol>
                    <div className="flex items-center gap-2">
                      {historyCursor && (
                        <Button variant="ghost" size="sm" onClick={() => setHistoryCursor(undefined)}>
                          Latest executions
                        </Button>
                      )}
                      {history.data?.next_cursor && (
                        <Button variant="outline" size="sm" onClick={() => setHistoryCursor(history.data?.next_cursor)}>
                          Older executions
                        </Button>
                      )}
                    </div>
                  </div>
                </ScrollArea>
              </TabsContent>
              <TabsContent value="events" className="min-h-0 overflow-hidden">
                <ScrollArea className="h-full">
                  <div className="flex flex-col gap-4 pr-3">
                    {events.isPending && <LoadingDetails />}
                    {events.isError && <LoadError label="events" onRetry={() => void events.refetch()} />}
                    {events.data?.events.length === 0 && (
                      <Empty>
                        <EmptyHeader>
                          <EmptyMedia variant="icon">
                            <History />
                          </EmptyMedia>
                          <EmptyTitle>No events recorded.</EmptyTitle>
                        </EmptyHeader>
                      </Empty>
                    )}
                    <ol>
                      {events.data?.events.map((event, index) => {
                        const EventIcon =
                          event.type === 'failed' ? CircleX : event.type === 'completed' ? CircleCheck : Circle
                        return (
                          <li key={event.sequence} className="flex gap-3">
                            <div className="flex w-5 shrink-0 flex-col items-center gap-1 py-1">
                              <EventIcon aria-hidden="true" className="size-4 text-muted-foreground" />
                              {index < (events.data?.events.length ?? 0) - 1 && (
                                <Separator orientation="vertical" className="min-h-5 flex-1" />
                              )}
                            </div>
                            <div className="flex min-w-0 flex-1 flex-wrap items-start justify-between gap-x-4 gap-y-1 pb-5">
                              <div className="flex min-w-0 flex-col gap-1">
                                <span>{eventLabels[event.type] ?? 'Task updated'}</span>
                                {(event.next_attempt || event.attempt || event.retry_task_id) && (
                                  <span className="text-xs text-muted-foreground">
                                    {[
                                      event.next_attempt || event.attempt
                                        ? `Attempt ${event.next_attempt || event.attempt}`
                                        : '',
                                      event.retry_task_id ? `Task ${event.retry_task_id}` : '',
                                    ]
                                      .filter(Boolean)
                                      .join(' · ')}
                                  </span>
                                )}
                              </div>
                              <span className="text-xs text-muted-foreground">
                                <TaskTime value={event.created_at} />
                              </span>
                            </div>
                          </li>
                        )
                      })}
                    </ol>
                    <div className="flex items-center gap-2">
                      {eventCursor && (
                        <Button variant="ghost" size="sm" onClick={() => setEventCursor(undefined)}>
                          Latest events
                        </Button>
                      )}
                      {events.data?.next_cursor && (
                        <Button variant="outline" size="sm" onClick={() => setEventCursor(events.data?.next_cursor)}>
                          Older events
                        </Button>
                      )}
                    </div>
                  </div>
                </ScrollArea>
              </TabsContent>
            </Tabs>
          </>
        )}
      </DialogContent>
    </Dialog>
  )
}
