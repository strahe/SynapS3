import type { AlertTriangle } from 'lucide-react'
import type { ReactNode } from 'react'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { cn } from '@/lib/utils'

export type BannerTone = 'info' | 'warning' | 'danger' | 'success'

const bannerToneClasses: Record<BannerTone, { alert?: string; description?: string }> = {
  info: {
    alert: 'border-[color:var(--status-info-border)] bg-[var(--status-info-bg)] text-[color:var(--status-info)]',
    description: 'text-[color:var(--status-info)]',
  },
  warning: {
    alert:
      'border-[color:var(--status-warning-border)] bg-[var(--status-warning-bg)] text-[color:var(--status-warning)]',
    description: 'text-[color:var(--status-warning)]',
  },
  danger: {},
  success: {
    alert:
      'border-[color:var(--status-success-border)] bg-[var(--status-success-bg)] text-[color:var(--status-success)]',
    description: 'text-[color:var(--status-success)]',
  },
}

/** A page-level notice whose color says how serious it is. */
export function Banner({
  tone,
  icon: Icon,
  title,
  className,
  children,
}: {
  tone: BannerTone
  icon: typeof AlertTriangle
  title?: ReactNode
  className?: string
  children: ReactNode
}) {
  const toneClass = bannerToneClasses[tone]

  return (
    <Alert variant={tone === 'danger' ? 'destructive' : 'default'} className={cn(toneClass.alert, className)}>
      <Icon />
      {title && <AlertTitle>{title}</AlertTitle>}
      <AlertDescription className={toneClass.description}>{children}</AlertDescription>
    </Alert>
  )
}
