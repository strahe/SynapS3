import { Check, Copy } from 'lucide-react'
import { useEffect, useRef, useState } from 'react'

import { Button } from '@/components/ui/button'

export function CopyButton({
  value,
  label,
  size = 'icon-sm',
}: {
  value: string
  label: string
  size?: 'icon-sm' | 'icon-xs'
}) {
  const [copied, setCopied] = useState(false)
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  useEffect(() => {
    return () => {
      if (timerRef.current) clearTimeout(timerRef.current)
    }
  }, [])

  async function handleCopy() {
    try {
      await navigator.clipboard.writeText(value)
      setCopied(true)
      if (timerRef.current) clearTimeout(timerRef.current)
      timerRef.current = setTimeout(() => setCopied(false), 2000)
    } catch {
      // Clipboard can be unavailable in non-secure browser contexts.
    }
  }

  return (
    <Button
      type="button"
      variant="ghost"
      size={size}
      onClick={handleCopy}
      aria-label={`${copied ? 'Copied' : 'Copy'} ${label}`}
    >
      {copied ? <Check data-icon="inline-start" /> : <Copy data-icon="inline-start" />}
    </Button>
  )
}
