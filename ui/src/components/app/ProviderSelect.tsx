import { Check, ChevronsUpDown, Search } from 'lucide-react'
import { useMemo, useState } from 'react'

import type { ReplacementProviderCandidate } from '@/api/client'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import {
  providerCandidateDisabledReason,
  providerCandidateLabel,
  providerCandidateMatches,
  providerCandidateNote,
  providerCandidateRegistryLine,
} from '@/lib/provider-replacement'
import { cn } from '@/lib/utils'

interface ProviderSelectProps {
  id?: string
  candidates: ReplacementProviderCandidate[]
  value: string
  onInspect: (providerID: string) => void
  disabled?: boolean
}

/**
 * A provider chooser that searches by name or registry ID. Providers that
 * cannot take the replica remain inspectable with their reason shown.
 */
export function ProviderSelect({ id, candidates, value, onInspect, disabled }: ProviderSelectProps) {
  const [open, setOpen] = useState(false)
  const [query, setQuery] = useState('')

  const matches = useMemo(
    () => candidates.filter((candidate) => providerCandidateMatches(candidate, query)),
    [candidates, query]
  )
  const selected = candidates.find((candidate) => candidate.provider_id === value)

  return (
    <Popover
      // The chooser lives inside a modal alert dialog, which switches off
      // pointer events everywhere outside its own subtree. This content is
      // portalled to the body, so without its own modal layer it never gets
      // them back and the list cannot be scrolled.
      modal
      open={open}
      onOpenChange={(next) => {
        setOpen(next)
        if (!next) setQuery('')
      }}
    >
      <PopoverTrigger asChild>
        <Button
          id={id}
          type="button"
          variant="outline"
          role="combobox"
          aria-expanded={open}
          disabled={disabled}
          className="w-full justify-between font-normal"
        >
          <span className={cn('truncate', !selected && 'text-muted-foreground')}>
            {selected ? providerCandidateLabel(selected) : 'Review a provider'}
          </span>
          <ChevronsUpDown className="size-4 shrink-0 opacity-50" />
        </Button>
      </PopoverTrigger>
      <PopoverContent align="start" className="w-(--radix-popover-trigger-width) p-0">
        <div className="flex items-center gap-2 border-b px-3 py-2">
          <Search className="size-4 shrink-0 text-muted-foreground" />
          <Input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Search by name or ID"
            autoComplete="off"
            className="h-8 border-0 px-0 shadow-none focus-visible:ring-0"
          />
        </div>
        <div className="max-h-64 overflow-y-auto overscroll-contain p-1">
          {matches.length === 0 && (
            <p className="px-2 py-6 text-center text-sm text-muted-foreground">No provider matches that search.</p>
          )}
          {matches.map((candidate) => {
            const disabledReason = providerCandidateDisabledReason(candidate)
            const note = providerCandidateNote(candidate)
            const registryLine = providerCandidateRegistryLine(candidate)
            return (
              <button
                key={candidate.provider_id}
                type="button"
                onClick={() => {
                  onInspect(candidate.provider_id)
                  setOpen(false)
                  setQuery('')
                }}
                className={cn(
                  'flex w-full items-start gap-2 rounded-sm px-2 py-2 text-left text-sm',
                  disabledReason ? 'opacity-60 hover:bg-accent' : 'hover:bg-accent hover:text-accent-foreground'
                )}
              >
                <Check
                  className={cn(
                    'mt-0.5 size-4 shrink-0',
                    candidate.provider_id === value ? 'opacity-100' : 'opacity-0'
                  )}
                />
                <span className="min-w-0 flex-1">
                  <span className="block truncate">{providerCandidateLabel(candidate)}</span>
                  {registryLine && <span className="block truncate text-xs text-muted-foreground">{registryLine}</span>}
                  {!candidate.provider_profile && (
                    <span className="block truncate text-xs text-muted-foreground">
                      Registry details not collected yet
                    </span>
                  )}
                  {(disabledReason || note) && (
                    <span className="block truncate text-xs text-muted-foreground">{disabledReason ?? note}</span>
                  )}
                </span>
              </button>
            )
          })}
        </div>
      </PopoverContent>
    </Popover>
  )
}
