import { MoreHorizontal } from 'lucide-react'
import type { ComponentProps, ReactNode } from 'react'
import { Button } from '@/components/ui/button'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuGroup,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'

/**
 * The `⋯` menu at the end of a table row. Put destructive items last, after a
 * DropdownMenuSeparator.
 */
export function RowActionsMenu({ label, children }: { label: string; children: ReactNode }) {
  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="ghost" size="icon-sm" aria-label={`Actions for ${label}`} title="Actions">
          <MoreHorizontal />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="min-w-44">
        <DropdownMenuGroup>{children}</DropdownMenuGroup>
      </DropdownMenuContent>
    </DropdownMenu>
  )
}

/** A menu item that explains why it is unavailable instead of relying on a hover title. */
export function RowActionItem({
  disabledReason,
  children,
  ...props
}: ComponentProps<typeof DropdownMenuItem> & { disabledReason?: string }) {
  return (
    <DropdownMenuItem {...props} disabled={props.disabled || Boolean(disabledReason)}>
      {disabledReason ? (
        <span className="flex min-w-0 flex-col gap-0.5">
          <span className="flex items-center gap-2">{children}</span>
          <span className="max-w-56 text-xs font-normal text-muted-foreground">{disabledReason}</span>
        </span>
      ) : (
        children
      )}
    </DropdownMenuItem>
  )
}
