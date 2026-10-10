import { ChevronRight } from 'lucide-react'
import type { MouseEvent, ReactNode } from 'react'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { cn } from '@/lib/utils'

/** Header row background shared by every list table. */
export const tableHeaderRowClassName = 'bg-muted/50 hover:bg-muted/50'

/** The bordered box every list table sits in. */
export function DataTableFrame({ className, children }: { className?: string; children: ReactNode }) {
  return <div className={cn('min-w-0 overflow-hidden rounded-lg border border-border', className)}>{children}</div>
}

/**
 * Props that make a whole table row open its target, unless the click belongs to
 * a control inside the row or ends a text selection. Keyboard users reach the
 * same target through the row's link or RowDetailsButton.
 */
export function clickableRowProps(open: () => void) {
  return {
    className: 'cursor-pointer',
    onClick: (event: MouseEvent<HTMLTableRowElement>) => {
      // React bubbles clicks from portaled menus and dialogs through the row; only clicks on the row itself count.
      if (!(event.target instanceof Element) || !event.currentTarget.contains(event.target)) return
      if (
        event.target.closest(
          'a, button, input, select, textarea, label, details, [role="button"], [role="menuitem"], [role="checkbox"]'
        )
      )
        return
      if (window.getSelection()?.toString()) return
      open()
    },
  }
}

export function RowDetailsButton({ label, onClick }: { label: string; onClick: () => void }) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <Button variant="ghost" size="icon-xs" aria-label={`Details for ${label}`} onClick={onClick}>
          <ChevronRight />
        </Button>
      </TooltipTrigger>
      <TooltipContent>Details</TooltipContent>
    </Tooltip>
  )
}

const skeletonRows = ['row-1', 'row-2', 'row-3', 'row-4', 'row-5', 'row-6']

export function TableSkeleton({ rows = skeletonRows.length }: { rows?: number }) {
  return (
    <DataTableFrame className="p-4">
      <div role="status" aria-label="Loading" className="flex flex-col gap-3">
        {skeletonRows.slice(0, rows).map((row) => (
          <Skeleton key={row} className="h-8 w-full" />
        ))}
      </div>
    </DataTableFrame>
  )
}
