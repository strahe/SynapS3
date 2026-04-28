import type { ReactNode } from 'react'
import { cn } from '@/lib/utils'

export function BreadcrumbCurrentPage({ children, className }: { children: ReactNode; className?: string }) {
  return (
    <span aria-current="page" className={cn('font-normal text-foreground', className)}>
      {children}
    </span>
  )
}
