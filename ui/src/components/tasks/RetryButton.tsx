import { Loader2, RotateCcw } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { useRetryTask } from '@/hooks/queries'

export function RetryButton({
  taskID,
  pending = false,
  disabled = false,
  onRetry,
}: {
  taskID: number | null | undefined
  pending?: boolean
  disabled?: boolean
  onRetry?: (id: number) => void
}) {
  const retry = useRetryTask()
  if (taskID == null) return null
  const busy = pending || retry.isPending
  return (
    <span className="inline-flex flex-col gap-1">
      <Button
        type="button"
        variant="outline"
        size="sm"
        disabled={disabled || busy}
        onClick={() => (onRetry ? onRetry(taskID) : retry.mutate(taskID))}
      >
        {busy ? <Loader2 data-icon="inline-start" className="animate-spin" /> : <RotateCcw data-icon="inline-start" />}
        Retry
      </Button>
      {retry.error && (
        <span role="alert" className="text-xs text-destructive">
          Could not retry. Refresh and try again.
        </span>
      )}
    </span>
  )
}
