import { Check, Copy } from 'lucide-react'
import { useCallback, useEffect, useRef, useState } from 'react'

import { Button } from '@/components/ui/button'
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'

export function DetailTextDialog({
  title,
  text,
  onClose,
}: {
  title: string
  text: string | null
  onClose: () => void
}) {
  const [copied, setCopied] = useState(false)
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  useEffect(() => {
    if (text !== null) {
      setCopied(false)
    }
  }, [text])

  useEffect(() => {
    return () => {
      if (timerRef.current) clearTimeout(timerRef.current)
    }
  }, [])

  const handleCopy = useCallback(async () => {
    if (!text) return
    try {
      await navigator.clipboard.writeText(text)
      setCopied(true)
      if (timerRef.current) clearTimeout(timerRef.current)
      timerRef.current = setTimeout(() => setCopied(false), 2000)
    } catch {
      // Clipboard API may fail outside secure contexts.
    }
  }, [text])

  return (
    <Dialog
      open={text !== null}
      onOpenChange={(open) => {
        if (!open) onClose()
      }}
    >
      <DialogContent className="sm:max-w-2xl">
        <DialogHeader>
          <DialogTitle>{title}</DialogTitle>
        </DialogHeader>
        <div className="max-h-80 overflow-auto rounded-md border border-border bg-muted/50 p-3">
          <pre className="whitespace-pre-wrap break-all font-mono text-xs">{text}</pre>
        </div>
        <DialogFooter>
          <Button type="button" variant="outline" onClick={handleCopy}>
            {copied ? <Check data-icon="inline-start" /> : <Copy data-icon="inline-start" />}
            {copied ? 'Copied' : 'Copy'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
