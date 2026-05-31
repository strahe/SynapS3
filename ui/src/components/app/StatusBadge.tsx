import type { ComponentPropsWithoutRef } from 'react'

import { Badge } from '@/components/ui/badge'
import { cn } from '@/lib/utils'

export type StatusTone = 'success' | 'warning' | 'danger' | 'info' | 'neutral'

const toneClasses: Record<StatusTone, string> = {
  success:
    'border-[color:var(--status-success-border)] bg-[var(--status-success-bg)] text-[color:var(--status-success)]',
  warning:
    'border-[color:var(--status-warning-border)] bg-[var(--status-warning-bg)] text-[color:var(--status-warning)]',
  danger: 'border-[color:var(--status-danger-border)] bg-[var(--status-danger-bg)] text-[color:var(--status-danger)]',
  info: 'border-[color:var(--status-info-border)] bg-[var(--status-info-bg)] text-[color:var(--status-info)]',
  neutral: 'border-border bg-background text-muted-foreground',
}

export function StatusBadge({
  children,
  tone = 'neutral',
  className,
  ...props
}: ComponentPropsWithoutRef<'span'> & {
  tone?: StatusTone
}) {
  return (
    <Badge variant="outline" className={cn(toneClasses[tone], className)} {...props}>
      {children}
    </Badge>
  )
}

export function bucketStatusTone(status: string): StatusTone {
  switch (status) {
    case 'active':
      return 'success'
    case 'creating':
    case 'deleting':
      return 'warning'
    case 'create_failed':
    case 'delete_failed':
      return 'danger'
    default:
      return 'neutral'
  }
}

export function taskStatusTone(status: string): StatusTone {
  switch (status) {
    case 'completed':
      return 'success'
    case 'queued':
    case 'scheduled':
      return 'warning'
    case 'running':
    case 'waiting':
      return 'info'
    case 'failed':
    case 'exhausted':
      return 'danger'
    default:
      return 'neutral'
  }
}

export function objectStateTone(state: string): StatusTone {
  switch (state) {
    case 'stored':
    case 'onchained':
      return 'success'
    case 'uploading':
    case 'committing':
    case 'replicating':
    case 'onchaining':
      return 'warning'
    case 'failed':
      return 'danger'
    case 'cached':
    case 'uploaded':
      return 'info'
    case 'cache_evicted':
      return 'neutral'
    default:
      return 'neutral'
  }
}
