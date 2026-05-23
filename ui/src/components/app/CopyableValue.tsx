import { Check, Copy } from 'lucide-react'
import { useEffect, useRef, useState } from 'react'

import { Button } from '@/components/ui/button'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { buildCopyableValueModel } from '@/lib/copyable-value'
import { cn } from '@/lib/utils'

type CopyState = 'idle' | 'copied' | 'failed'

export function CopyableValue({
  value,
  displayValue,
  label,
  monospace = false,
  maxLength,
  className,
}: {
  value: string
  displayValue?: string
  label: string
  monospace?: boolean
  maxLength?: number
  className?: string
}) {
  const [copyState, setCopyState] = useState<CopyState>('idle')
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const model = buildCopyableValueModel({ value, displayValue, maxLength })
  const copyLabel = copyState === 'copied' ? 'Copied' : copyState === 'failed' ? 'Copy failed' : 'Copy'

  useEffect(() => {
    return () => {
      if (timerRef.current) clearTimeout(timerRef.current)
    }
  }, [])

  async function handleCopy() {
    try {
      if (!navigator.clipboard?.writeText) throw new Error('Clipboard unavailable')
      await navigator.clipboard.writeText(model.copyValue)
      setCopyState('copied')
    } catch {
      setCopyState('failed')
    }

    if (timerRef.current) clearTimeout(timerRef.current)
    timerRef.current = setTimeout(() => setCopyState('idle'), 2000)
  }

  return (
    <span className={cn('inline-flex max-w-full min-w-0 items-center gap-1.5', className)}>
      <Tooltip delayDuration={200}>
        <TooltipTrigger asChild>
          <span className={cn('min-w-0 truncate', monospace && 'font-mono text-xs')}>{model.displayText}</span>
        </TooltipTrigger>
        <TooltipContent
          side="top"
          className="max-h-72 max-w-[min(32rem,calc(100vw-4rem))] overflow-auto whitespace-pre-wrap break-all text-left"
        >
          <span className={cn(monospace && 'font-mono text-xs')}>{model.tooltipValue}</span>
        </TooltipContent>
      </Tooltip>
      <Tooltip delayDuration={200}>
        <TooltipTrigger asChild>
          <Button
            type="button"
            variant="ghost"
            size="icon-xs"
            onClick={handleCopy}
            aria-label={`${copyLabel} ${label}`}
          >
            {copyState === 'copied' ? <Check data-icon="inline-start" /> : <Copy data-icon="inline-start" />}
          </Button>
        </TooltipTrigger>
        <TooltipContent>{copyLabel}</TooltipContent>
      </Tooltip>
    </span>
  )
}
