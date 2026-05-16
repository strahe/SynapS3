import type { FilecoinReadinessCheck, FilecoinReadinessData } from '@/api/client'
import { StatusBadge } from '@/components/app/StatusBadge'
import { Button } from '@/components/ui/button'
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import {
  filecoinReadinessCheckTitle,
  filecoinReadinessStatusDescription,
  filecoinReadinessStatusLabel,
  filecoinReadinessStatusTone,
  importantFilecoinReadinessChecks,
  isDismissibleFilecoinReadinessCheck,
} from '@/lib/filecoin-readiness'

export function FilecoinReadinessDialog({
  title,
  data,
  open,
  onOpenChange,
  dismissedCheckIds,
  onDismissCheck,
}: {
  title: string
  data: FilecoinReadinessData | null | undefined
  open: boolean
  onOpenChange: (open: boolean) => void
  dismissedCheckIds?: ReadonlySet<string>
  onDismissCheck?: (id: string) => void
}) {
  const attentionChecks = data ? importantFilecoinReadinessChecks(data.checks, dismissedCheckIds) : []

  return (
    <Dialog open={open && Boolean(data)} onOpenChange={onOpenChange}>
      <DialogContent className="w-[calc(100vw-2rem)] max-w-[calc(100vw-2rem)] sm:max-w-xl">
        {data && (
          <>
            <DialogHeader>
              <DialogTitle className="flex flex-wrap items-center gap-2">
                {title}
                <StatusBadge tone={filecoinReadinessStatusTone(data.status)}>
                  {filecoinReadinessStatusLabel(data.status)}
                </StatusBadge>
              </DialogTitle>
              <DialogDescription className="sr-only">
                {filecoinReadinessStatusDescription(data.status)}
              </DialogDescription>
            </DialogHeader>

            <div className="grid gap-2">
              {attentionChecks.length === 0 ? (
                <div className="rounded-md border border-[color:var(--status-success-border)] bg-[var(--status-success-bg)] px-3 py-2 text-sm text-[color:var(--status-success)]">
                  No action needed.
                </div>
              ) : (
                <div className="grid max-h-[min(58vh,28rem)] gap-2 overflow-y-auto pr-1">
                  {attentionChecks.map((check) => (
                    <ReadinessIssueRow key={check.id} check={check} onDismissCheck={onDismissCheck} />
                  ))}
                </div>
              )}
            </div>
          </>
        )}
      </DialogContent>
    </Dialog>
  )
}

function ReadinessIssueRow({
  check,
  onDismissCheck,
}: {
  check: FilecoinReadinessCheck
  onDismissCheck?: (id: string) => void
}) {
  const dismissible = Boolean(onDismissCheck && isDismissibleFilecoinReadinessCheck(check.id))

  return (
    <div className="rounded-md border border-border bg-card px-3 py-2.5">
      <div className="flex min-w-0 flex-wrap items-start justify-between gap-2">
        <div className="min-w-0">
          <div className="font-medium">{filecoinReadinessCheckTitle(check.id)}</div>
          <div className="mt-1 break-words text-sm text-muted-foreground">{check.message}</div>
        </div>
        <StatusBadge tone={filecoinReadinessStatusTone(check.status)}>
          {filecoinReadinessStatusLabel(check.status)}
        </StatusBadge>
      </div>
      {(check.action || dismissible) && (
        <div className="mt-2 flex flex-wrap items-center justify-between gap-2 text-xs">
          {check.action ? (
            <div>
              <span className="font-medium">Next step: </span>
              <span className="text-muted-foreground">{check.action}</span>
            </div>
          ) : (
            <span />
          )}
          {dismissible && (
            <Button type="button" variant="outline" size="sm" onClick={() => onDismissCheck?.(check.id)}>
              Dismiss
            </Button>
          )}
        </div>
      )}
    </div>
  )
}
