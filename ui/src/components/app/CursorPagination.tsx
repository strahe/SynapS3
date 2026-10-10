import { ChevronLeft, ChevronRight } from 'lucide-react'
import type { ReactNode } from 'react'
import { Button } from '@/components/ui/button'
import { Pagination, PaginationContent, PaginationItem } from '@/components/ui/pagination'
import { cn } from '@/lib/utils'

/** Previous/next paging shared by every list; renders nothing when there is only one page. */
export function CursorPagination({
  hasPrevious,
  hasNext,
  onPrevious,
  onNext,
  summary,
  disabled = false,
  className,
}: {
  hasPrevious: boolean
  hasNext: boolean
  onPrevious: () => void
  onNext: () => void
  summary?: ReactNode
  disabled?: boolean
  className?: string
}) {
  if (!hasPrevious && !hasNext) return null

  return (
    <div className={cn('flex flex-wrap items-center justify-between gap-2', className)}>
      <span className="text-sm text-muted-foreground">{summary}</span>
      <Pagination className="mx-0 w-auto justify-end">
        <PaginationContent className="gap-2">
          <PaginationItem>
            <Button type="button" variant="outline" size="sm" disabled={disabled || !hasPrevious} onClick={onPrevious}>
              <ChevronLeft data-icon="inline-start" />
              Previous
            </Button>
          </PaginationItem>
          <PaginationItem>
            <Button type="button" variant="outline" size="sm" disabled={disabled || !hasNext} onClick={onNext}>
              Next
              <ChevronRight data-icon="inline-end" />
            </Button>
          </PaginationItem>
        </PaginationContent>
      </Pagination>
    </div>
  )
}
