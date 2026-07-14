import { Check, Copy } from 'lucide-react'
import { type ReactElement, useEffect, useRef } from 'react'

import { Button } from '@/components/ui/button'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { useClipboardCopy } from '@/hooks/use-clipboard-copy'
import { buildCopyableValueModel } from '@/lib/copyable-value'
import { cn } from '@/lib/utils'

export function CopyableValue({
  value,
  displayValue,
  label,
  monospace = false,
  maxLength,
  linkHref,
  external = false,
  children,
  className,
}: {
  value: string
  displayValue?: string
  label: string
  monospace?: boolean
  maxLength?: number
  linkHref?: string
  external?: boolean
  children?: ReactElement
  className?: string
}) {
  const { copyState, copy, reset } = useClipboardCopy()
  const previousValueRef = useRef(value)

  useEffect(() => {
    if (previousValueRef.current === value) return
    previousValueRef.current = value
    reset()
  }, [value, reset])

  const model = buildCopyableValueModel({ value, displayValue, maxLength, linkHref, external })
  const copyLabel = copyState === 'copied' ? 'Copied' : copyState === 'failed' ? 'Copy failed' : 'Copy'

  const valueClassName = cn('min-w-0 truncate', monospace && 'font-mono text-xs')
  const valueNode = children ? (
    children
  ) : model.linkHref ? (
    <a
      href={model.linkHref}
      target={model.external ? '_blank' : undefined}
      rel={model.external ? 'noreferrer' : undefined}
      className={cn(valueClassName, 'hover:text-foreground hover:underline')}
    >
      {model.displayText}
    </a>
  ) : (
    <span role="note" aria-label={`${label}: ${model.tooltipValue}`} className={valueClassName}>
      {model.displayText}
    </span>
  )

  return (
    <span className={cn('inline-flex max-w-full min-w-0 items-center gap-1.5', className)}>
      <Tooltip delayDuration={200}>
        <TooltipTrigger asChild>{valueNode}</TooltipTrigger>
        <TooltipContent
          side="top"
          className="max-h-72 max-w-[min(32rem,calc(100vw-4rem))] overflow-auto whitespace-pre-wrap break-all text-left"
        >
          <span className={cn(monospace && 'font-mono text-xs')}>{model.tooltipValue}</span>
        </TooltipContent>
      </Tooltip>
      <Button
        type="button"
        variant="ghost"
        size="icon-xs"
        onClick={() => copy(model.copyValue)}
        aria-label={`${copyLabel} ${label}`}
        title={copyLabel}
      >
        {copyState === 'copied' ? <Check data-icon="inline-start" /> : <Copy data-icon="inline-start" />}
      </Button>
    </span>
  )
}
