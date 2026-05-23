import { Check, Copy } from 'lucide-react'
import { useEffect } from 'react'

import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { useClipboardCopy } from '@/hooks/use-clipboard-copy'

export function DetailTextDialog({
  title,
  text,
  onClose,
}: {
  title: string
  text: string | null
  onClose: () => void
}) {
  const { copyState, copy, reset } = useClipboardCopy()
  const copyLabel = copyState === 'copied' ? 'Copied' : copyState === 'failed' ? 'Copy failed' : 'Copy'

  useEffect(() => {
    if (text !== null) reset()
  }, [reset, text])

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
          <DialogDescription className="sr-only">Detailed text for {title}</DialogDescription>
        </DialogHeader>
        <div className="max-h-80 overflow-auto rounded-md border border-border bg-muted/50 p-3">
          <pre className="whitespace-pre-wrap break-all font-mono text-xs">{text}</pre>
        </div>
        <DialogFooter>
          <Button type="button" variant="outline" onClick={() => text && copy(text)}>
            {copyState === 'copied' ? <Check data-icon="inline-start" /> : <Copy data-icon="inline-start" />}
            {copyLabel}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
