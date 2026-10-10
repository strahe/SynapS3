import { AlertTriangle, RefreshCw } from 'lucide-react'
import type { ReactNode } from 'react'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Empty, EmptyContent, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from '@/components/ui/empty'
import { Skeleton } from '@/components/ui/skeleton'
import { cn } from '@/lib/utils'

/** Placeholder for a page body that is still loading; the page header stays visible above it. */
export function PageLoading({ className }: { className?: string }) {
  return (
    <div role="status" aria-label="Loading" className={cn('flex flex-col gap-4', className)}>
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <Skeleton className="h-24" />
        <Skeleton className="h-24" />
        <Skeleton className="h-24" />
        <Skeleton className="h-24" />
      </div>
      <Skeleton className="h-64" />
    </div>
  )
}

/** A failed load, shown below the page header, with a way to try again. */
export function PageError({
  title,
  description,
  onRetry,
  retrying = false,
  className,
}: {
  title: ReactNode
  description?: ReactNode
  onRetry?: () => void
  retrying?: boolean
  className?: string
}) {
  return (
    <Alert variant="destructive" role="alert" className={className}>
      <AlertTriangle />
      <AlertTitle>{title}</AlertTitle>
      {(description || onRetry) && (
        <AlertDescription className="flex flex-col items-start gap-3">
          {description && <span className="break-words">{description}</span>}
          {onRetry && (
            <Button type="button" variant="outline" size="sm" onClick={onRetry} disabled={retrying}>
              <RefreshCw data-icon="inline-start" className={retrying ? 'animate-spin' : undefined} />
              Retry
            </Button>
          )}
        </AlertDescription>
      )}
    </Alert>
  )
}

/** An empty list or page, optionally with the action that fills it. */
export function EmptyState({
  icon,
  title,
  description,
  action,
  bordered = true,
  className,
}: {
  icon: ReactNode
  title: ReactNode
  description?: ReactNode
  action?: ReactNode
  bordered?: boolean
  className?: string
}) {
  return (
    <Empty
      className={cn('min-h-56', bordered ? 'rounded-lg border border-solid border-border' : 'border-0', className)}
    >
      <EmptyHeader>
        <EmptyMedia variant="icon">{icon}</EmptyMedia>
        <EmptyTitle>{title}</EmptyTitle>
        {description && <EmptyDescription>{description}</EmptyDescription>}
      </EmptyHeader>
      {action && <EmptyContent>{action}</EmptyContent>}
    </Empty>
  )
}
