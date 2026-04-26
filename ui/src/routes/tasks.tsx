import { useMutation, useQueryClient } from '@tanstack/react-query'
import { createFileRoute } from '@tanstack/react-router'
import { Check, Copy, Loader2, RefreshCw, RotateCcw } from 'lucide-react'
import { useCallback, useEffect, useRef, useState } from 'react'
import { api } from '@/api/client'
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { useTasks } from '@/hooks/queries'
import { cn, timeAgo } from '@/lib/utils'

export const Route = createFileRoute('/tasks')({
  component: TasksPage,
})

const statusTabs = ['', 'pending', 'running', 'completed', 'failed', 'dead_letter'] as const
const statusLabels: Record<string, string> = {
  '': 'All',
  pending: 'Pending',
  running: 'Running',
  completed: 'Completed',
  failed: 'Failed',
  dead_letter: 'Dead Letter',
}

const statusColor: Record<string, string> = {
  pending: 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-300',
  running: 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300',
  completed: 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300',
  failed: 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300',
  dead_letter: 'bg-red-200 text-red-900 dark:bg-red-950 dark:text-red-300',
  cancelled: 'bg-gray-100 text-gray-800 dark:bg-gray-900 dark:text-gray-300',
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
    <div className="space-y-4 p-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">Tasks</h1>
        <button
          type="button"
          onClick={() => qc.invalidateQueries({ queryKey: ['tasks'] })}
          className="inline-flex items-center gap-2 rounded-md border border-border px-3 py-1.5 text-sm hover:bg-accent"
        >
          <RefreshCw className="h-4 w-4" /> Refresh
        </button>
      </div>

      {/* Status tabs */}
      <div className="flex gap-1 rounded-lg border border-border bg-muted/50 p-1">
        {statusTabs.map((tab) => (
          <button
            type="button"
            key={tab}
            onClick={() => {
              setStatus(tab)
              setOffset(0)
            }}
            className={cn(
              'rounded-md px-3 py-1.5 text-sm transition-colors',
              status === tab ? 'bg-background font-medium shadow-sm' : 'text-muted-foreground hover:text-foreground'
            )}
          >
            {statusLabels[tab]}
          </button>
        ))}
      </div>

      {/* Type filter */}
      <div className="flex items-center gap-2">
        <label htmlFor="task-type-filter" className="text-sm text-muted-foreground">
          Type:
        </label>
        <select
          id="task-type-filter"
          value={taskType}
          onChange={(e) => {
            setTaskType(e.target.value)
            setOffset(0)
          }}
          className="rounded-md border border-border bg-background px-2 py-1 text-sm"
        >
          <option value="">All</option>
          <option value="upload_to_sp">Upload to SP</option>
          <option value="create_proof_set">Create Proof Set</option>
          <option value="add_roots">Add Roots</option>
          <option value="evict_cache">Evict Cache</option>
          <option value="delete_proof_set">Delete Proof Set</option>
        </select>
      </div>

      {isLoading ? (
        <div className="flex h-60 items-center justify-center">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      ) : error ? (
        <div className="text-destructive">Failed to load tasks</div>
      ) : (
        <>
          <div className="overflow-x-auto rounded-lg border border-border">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border bg-muted/50">
                  <th className="px-4 py-3 text-left font-medium">ID</th>
                  <th className="px-4 py-3 text-left font-medium">Type</th>
                  <th className="px-4 py-3 text-left font-medium">Ref</th>
                  <th className="px-4 py-3 text-left font-medium">Status</th>
                  <th className="px-4 py-3 text-right font-medium">Retries</th>
                  <th className="px-4 py-3 text-left font-medium">Error</th>
                  <th className="px-4 py-3 text-left font-medium">Scheduled</th>
                  <th className="px-4 py-3 text-left font-medium">Actions</th>
                </tr>
              </thead>
              <tbody>
                {data && data.tasks.length > 0 ? (
                  data.tasks.map((t) => (
                    <tr key={t.id} className="border-b border-border hover:bg-muted/30">
                      <td className="px-4 py-3 font-mono text-xs">{t.id}</td>
                      <td className="px-4 py-3">{t.type}</td>
                      <td className="px-4 py-3 text-muted-foreground">
                        {t.ref_type}:{t.ref_id}
                      </td>
                      <td className="px-4 py-3">
                        <span
                          className={cn(
                            'inline-block rounded-full px-2 py-0.5 text-xs font-medium',
                            statusColor[t.status] ?? 'bg-gray-100 text-gray-800'
                          )}
                        >
                          {t.status}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-right">
                        {t.retry_count}/{t.max_retries}
                      </td>
                      <td className="max-w-xs px-4 py-3 text-xs text-muted-foreground">
                        {t.last_error ? (
                          <button
                            type="button"
                            onClick={() => {
                              if (t.last_error) {
                                setErrorDialogText(t.last_error)
                              }
                            }}
                            className="max-w-full cursor-pointer truncate text-left hover:text-foreground"
                          >
                            {t.last_error}
                          </button>
                        ) : (
                          '—'
                        )}
                      </td>
                      <td className="px-4 py-3 text-muted-foreground">{timeAgo(t.scheduled_at)}</td>
                      <td className="px-4 py-3">
                        {t.status === 'dead_letter' && (
                          <button
                            type="button"
                            onClick={() => retryMutation.mutate(t.id)}
                            disabled={retryingId === t.id}
                            className="inline-flex items-center gap-1 rounded-md border border-border px-2 py-1 text-xs hover:bg-accent disabled:opacity-50"
                          >
                            <RotateCcw className="h-3 w-3" /> Retry
                          </button>
                        )}
                      </td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={8} className="px-4 py-8 text-center text-muted-foreground">
                      No tasks found
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>

          {/* Pagination */}
          {totalPages > 1 && (
            <div className="flex items-center justify-between">
              <span className="text-sm text-muted-foreground">
                Page {currentPage} of {totalPages} ({data?.total} total)
              </span>
              <div className="flex gap-2">
                <button
                  type="button"
                  disabled={offset === 0}
                  onClick={() => setOffset(Math.max(0, offset - PAGE_SIZE))}
                  className="rounded-md border border-border px-3 py-1.5 text-sm hover:bg-accent disabled:opacity-50"
                >
                  ← Prev
                </button>
                <button
                  type="button"
                  disabled={currentPage >= totalPages}
                  onClick={() => setOffset(offset + PAGE_SIZE)}
                  className="rounded-md border border-border px-3 py-1.5 text-sm hover:bg-accent disabled:opacity-50"
                >
                  Next →
                </button>
              </div>
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
          <button
            type="button"
            onClick={handleCopy}
            className="inline-flex items-center gap-2 rounded-md border border-border px-3 py-1.5 text-sm hover:bg-accent"
          >
            {copied ? <Check className="h-4 w-4 text-green-500" /> : <Copy className="h-4 w-4" />}
            {copied ? 'Copied' : 'Copy'}
          </button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
