import { ChevronRight } from 'lucide-react'
import type { ReactNode, RefObject } from 'react'
import { Button } from '@/components/ui/button'
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible'
import { ScrollArea } from '@/components/ui/scroll-area'
import { SheetDescription, SheetHeader, SheetTitle } from '@/components/ui/sheet'
import { cn } from '@/lib/utils'

/** The header every detail sheet uses: what kind of thing it is, its name and state, and its main actions. */
export function DetailHeader({
  titleRef,
  kind,
  title,
  badge,
  subtitle,
  actions,
}: {
  titleRef?: RefObject<HTMLHeadingElement | null>
  kind: string
  title: ReactNode
  badge?: ReactNode
  subtitle?: ReactNode
  actions?: ReactNode
}) {
  return (
    <SheetHeader className="gap-1 border-b pr-12">
      <p className="text-xs font-medium text-muted-foreground">{kind}</p>
      <div className="flex min-w-0 items-center gap-2">
        <SheetTitle ref={titleRef} tabIndex={-1} className="min-w-0 truncate outline-none">
          {title}
        </SheetTitle>
        {badge}
      </div>
      {subtitle ? (
        <SheetDescription className="break-words">{subtitle}</SheetDescription>
      ) : (
        <SheetDescription className="sr-only">{kind} details</SheetDescription>
      )}
      {actions && <div className="mt-3 flex flex-wrap gap-2">{actions}</div>}
    </SheetHeader>
  )
}

export function DetailBody({ children }: { children: ReactNode }) {
  return (
    <ScrollArea className="min-h-0 flex-1">
      <div className="@container flex flex-col gap-6 p-4">{children}</div>
    </ScrollArea>
  )
}

export function DetailSection({ title, action, children }: { title: string; action?: ReactNode; children: ReactNode }) {
  return (
    <section className="flex min-w-0 flex-col gap-3">
      <div className="flex min-w-0 items-center justify-between gap-2">
        <h3 className="text-sm font-semibold">{title}</h3>
        {action}
      </div>
      {children}
    </section>
  )
}

export function DisclosureSection({ title, children }: { title: string; children: ReactNode }) {
  return (
    <Collapsible className="flex min-w-0 flex-col gap-3">
      <CollapsibleTrigger asChild>
        <Button variant="ghost" size="sm" className="-ml-2 w-fit [&[data-state=open]>svg]:rotate-90">
          <ChevronRight data-icon="inline-start" className="transition-transform" />
          {title}
        </Button>
      </CollapsibleTrigger>
      <CollapsibleContent>{children}</CollapsibleContent>
    </Collapsible>
  )
}

/** Label/value pairs that reflow with the width they are given. */
export function DetailGrid({ className, children }: { className?: string; children: ReactNode }) {
  return (
    <div className="@container min-w-0">
      <dl className={cn('grid grid-cols-1 gap-x-6 gap-y-4 text-sm @sm:grid-cols-2 @xl:grid-cols-3', className)}>
        {children}
      </dl>
    </div>
  )
}

export function DetailField({
  label,
  action,
  wide,
  className,
  children,
}: {
  label: string
  action?: ReactNode
  wide?: boolean
  className?: string
  children: ReactNode
}) {
  return (
    <div className={cn('min-w-0', wide && 'col-span-full', className)}>
      <dt className="flex h-5 items-center gap-1 text-xs text-muted-foreground">
        {label}
        {action}
      </dt>
      <dd className="mt-1 min-w-0 break-words">{children}</dd>
    </div>
  )
}

export function DetailNote({ className, children }: { className?: string; children: ReactNode }) {
  return <span className={cn('mt-0.5 block text-xs text-muted-foreground', className)}>{children}</span>
}
