import { useMutation, useQueryClient } from '@tanstack/react-query'
import { createFileRoute } from '@tanstack/react-router'
import { Check, Copy, Loader2, RefreshCw, RotateCcw } from 'lucide-react'
import { useCallback, useEffect, useRef, useState } from 'react'
import { api } from '@/api/client'
import { PageHeader } from '@/components/app/PageHeader'
import { StatusBadge, taskStatusTone } from '@/components/app/StatusBadge'
import { Button } from '@/components/ui/button'
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Label } from '@/components/ui/label'
import { Pagination, PaginationContent, PaginationItem } from '@/components/ui/pagination'
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { useTasks } from '@/hooks/queries'
import { timeAgo } from '@/lib/utils'

export const Route = createFileRoute('/tasks')({
  component: TasksPage,
})

const taskTypeTabs = ['all', 'upload', 'evict_cache'] as const
const taskTypeLabels: Record<string, string> = {
  all: 'All',
  upload: 'Upload',
  evict_cache: 'Evict Cache',
}

const statusOptions = ['all', 'pending', 'running', 'completed', 'failed', 'cancelled', 'dead_letter'] as const
const statusLabels: Record<string, string> = {
  all: 'All',
  pending: 'Pending',
  running: 'Running',
  completed: 'Completed',
  failed: 'Failed',
  cancelled: 'Cancelled',
  dead_letter: 'Dead Letter',
}

const PAGE_SIZE = 50

function TasksPage() {
  const [status, setStatus] = useState('')
  const [taskType, setTaskType] = useState('')
  const [offset, setOffset] = useState(0)
  const { data, isLoading, error } = useTasks(taskType, status, PAGE_SIZE, offset)
  const qc = useQueryClient()

  const [retryingId, setRetryingId] = useState<number | null>(null)
  const [errorDialogText, setErrorDialogText] = useState<string | null>(null)
  const retryMutation = useMutation({
    mutationFn: (taskId: number) => {
      setRetryingId(taskId)
      return api.retryTask(taskId)
    },
    onSettled: () => {
      setRetryingId(null)
      qc.invalidateQueries({ queryKey: ['tasks'] })
    },
  })

  const totalPages = data ? Math.ceil(data.total / PAGE_SIZE) : 0
  const currentPage = Math.floor(offset / PAGE_SIZE) + 1

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

        <div className="flex items-center gap-2">
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
                  <TableHead className="px-4">ID</TableHead>
                  <TableHead className="px-4">Type</TableHead>
                  <TableHead className="px-4">Ref</TableHead>
                  <TableHead className="px-4">Status</TableHead>
                  <TableHead className="px-4 text-right">Retries</TableHead>
                  <TableHead className="px-4">Error</TableHead>
                  <TableHead className="px-4">Scheduled</TableHead>
                  <TableHead className="px-4">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {data && data.tasks.length > 0 ? (
                  data.tasks.map((t) => (
                    <TableRow key={t.id}>
                      <TableCell className="px-4 font-mono text-xs">{t.id}</TableCell>
                      <TableCell className="px-4">{t.type}</TableCell>
                      <TableCell className="px-4 text-muted-foreground">
                        {t.ref_type}:{t.ref_id}
                      </TableCell>
                      <TableCell className="px-4">
                        <StatusBadge tone={taskStatusTone(t.status)}>{t.status}</StatusBadge>
                      </TableCell>
                      <TableCell className="px-4 text-right">
                        {t.retry_count}/{t.max_retries}
                      </TableCell>
                      <TableCell className="max-w-xs px-4 text-xs text-muted-foreground">
                        {t.last_error ? (
                          <Button
                            type="button"
                            variant="link"
                            onClick={() => {
                              if (t.last_error) {
                                setErrorDialogText(t.last_error)
                              }
                            }}
                            className="h-auto max-w-full justify-start p-0 text-left text-xs font-normal text-muted-foreground hover:text-foreground"
                          >
                            <span className="truncate">{t.last_error}</span>
                          </Button>
                        ) : (
                          '—'
                        )}
                      </TableCell>
                      <TableCell className="px-4 text-muted-foreground">{timeAgo(t.scheduled_at)}</TableCell>
                      <TableCell className="px-4">
                        {t.status === 'dead_letter' && (
                          <Button
                            type="button"
                            variant="outline"
                            size="xs"
                            onClick={() => retryMutation.mutate(t.id)}
                            disabled={retryingId === t.id}
                          >
                            <RotateCcw data-icon="inline-start" /> Retry
                          </Button>
                        )}
                      </TableCell>
                    </TableRow>
                  ))
                ) : (
                  <TableRow>
                    <TableCell colSpan={8} className="h-24 text-center text-muted-foreground">
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

      <ErrorDetailDialog errorText={errorDialogText} onClose={() => setErrorDialogText(null)} />
    </div>
  )
}

function ErrorDetailDialog({ errorText, onClose }: { errorText: string | null; onClose: () => void }) {
  const [copied, setCopied] = useState(false)
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  useEffect(() => {
    if (errorText !== null) {
      setCopied(false)
    }
  }, [errorText])

  useEffect(() => {
    return () => {
      if (timerRef.current) clearTimeout(timerRef.current)
    }
  }, [])

  const handleCopy = useCallback(async () => {
    if (!errorText) return
    try {
      await navigator.clipboard.writeText(errorText)
      setCopied(true)
      if (timerRef.current) clearTimeout(timerRef.current)
      timerRef.current = setTimeout(() => setCopied(false), 2000)
    } catch {
      // Clipboard API may fail on non-HTTPS
    }
  }, [errorText])

  return (
    <Dialog
      open={errorText !== null}
      onOpenChange={(open) => {
        if (!open) onClose()
      }}
    >
      <DialogContent className="sm:max-w-2xl">
        <DialogHeader>
          <DialogTitle>Error Details</DialogTitle>
        </DialogHeader>
        <div className="max-h-80 overflow-auto rounded-md border border-border bg-muted/50 p-3">
          <pre className="whitespace-pre-wrap break-all font-mono text-xs">{errorText}</pre>
        </div>
        <DialogFooter>
          <Button type="button" variant="outline" onClick={handleCopy}>
            {copied ? <Check data-icon="inline-start" /> : <Copy data-icon="inline-start" />}
            {copied ? 'Copied' : 'Copy'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
