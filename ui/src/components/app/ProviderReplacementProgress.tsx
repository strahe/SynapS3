import type { ProviderReplacementProgress as ProviderReplacementProgressData } from '@/api/client'
import { Progress } from '@/components/ui/progress'
import { providerReplacementProgressView } from '@/lib/provider-replacement-progress'
import { cn } from '@/lib/utils'

export function ProviderReplacementProgress({
  progress,
  statusMessage,
  compact = false,
}: {
  progress: ProviderReplacementProgressData
  statusMessage?: string
  compact?: boolean
}) {
  const view = providerReplacementProgressView(progress, statusMessage)

  return (
    <div className={cn('flex min-w-0 flex-col', compact ? 'w-56 max-w-full gap-1' : 'gap-2')}>
      <Progress
        value={view.value}
        aria-label={view.indeterminate ? view.summary : 'Provider replacement progress'}
        aria-valuemin={view.value === null ? undefined : 0}
        aria-valuemax={view.value === null ? undefined : 100}
        aria-valuenow={view.value ?? undefined}
        className={cn(
          compact ? 'h-1' : 'h-1.5',
          view.indeterminate &&
            '[&_[data-slot=progress-indicator]]:!w-1/3 [&_[data-slot=progress-indicator]]:!flex-none [&_[data-slot=progress-indicator]]:!translate-x-0 [&_[data-slot=progress-indicator]]:animate-pulse motion-reduce:[&_[data-slot=progress-indicator]]:animate-none'
        )}
      />
      <span className={cn('text-muted-foreground', compact ? 'truncate text-[11px]' : 'text-sm')}>{view.summary}</span>
      {view.activity && (
        <span className={cn('text-muted-foreground', compact ? 'truncate text-[11px]' : 'text-xs')}>
          {view.activity}
        </span>
      )}
    </div>
  )
}
