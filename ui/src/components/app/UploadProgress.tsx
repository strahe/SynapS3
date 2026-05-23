import type { ReactNode } from 'react'
import type { UploadTransferProgress } from '@/api/client'
import { Progress } from '@/components/ui/progress'

export function uploadProgressPercent(progress?: UploadTransferProgress, options: { includeDone?: boolean } = {}) {
  if (!progress || (!options.includeDone && progress.done) || typeof progress.percent !== 'number') return null
  return Math.max(0, Math.min(100, progress.percent))
}

export function UploadProgressRing({
  percent,
  compact = false,
  children,
}: {
  percent: number
  compact?: boolean
  children: ReactNode
}) {
  const value = Math.max(0, Math.min(100, percent))
  return (
    <span
      className={`relative inline-flex shrink-0 items-center justify-center rounded-full ${
        compact ? 'size-[18px]' : 'size-6'
      }`}
      style={{
        background: `conic-gradient(var(--status-info) ${value * 3.6}deg, var(--muted) 0deg)`,
      }}
      role="progressbar"
      aria-valuemin={0}
      aria-valuemax={100}
      aria-valuenow={value}
    >
      <span className={`absolute rounded-full bg-background ${compact ? 'inset-[3px]' : 'inset-[4px]'}`} />
      <span className="relative inline-flex items-center justify-center">{children}</span>
    </span>
  )
}

export function UploadProgressBar({ progress }: { progress?: UploadTransferProgress }) {
  const percent = uploadProgressPercent(progress, { includeDone: true })
  if (percent === null) return null

  return (
    <div className="inline-flex w-32 shrink-0 items-center gap-2" title={`${percent}% uploaded`}>
      <Progress value={percent} className="min-w-0 flex-1 [&_[data-slot=progress-indicator]]:bg-status-info" />
      <span className="w-8 shrink-0 text-right font-mono text-[10px] text-muted-foreground">{percent}%</span>
    </div>
  )
}
