import { Loader2 } from 'lucide-react'
import { type ReactNode, useEffect, useId, useRef, useState } from 'react'
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { confirmationMatches } from '@/lib/risk-confirmation'

export interface DangerActionAlertDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  title: ReactNode
  description: ReactNode
  confirmLabel: string
  onConfirm: () => void
  pending?: boolean
  error?: string | null
  typedTarget?: string
  typedTargetLabel?: string
  contentClassName?: string
  children?: ReactNode
}

export function DangerActionAlertDialog({
  open,
  onOpenChange,
  title,
  description,
  confirmLabel,
  onConfirm,
  pending = false,
  error,
  typedTarget,
  typedTargetLabel = 'Type to confirm',
  contentClassName,
  children,
}: DangerActionAlertDialogProps) {
  const inputId = useId()
  const inputRef = useRef<HTMLInputElement>(null)
  const [confirmInput, setConfirmInput] = useState('')
  const needsTypedConfirmation = typedTarget !== undefined
  const typedConfirmationValid =
    !needsTypedConfirmation || (typedTarget.length > 0 && confirmationMatches(confirmInput, typedTarget))
  const canConfirm = !pending && typedConfirmationValid

  useEffect(() => {
    if (!open) setConfirmInput('')
  }, [open])

  useEffect(() => {
    if (open && needsTypedConfirmation && !pending) inputRef.current?.focus()
  }, [open, needsTypedConfirmation, pending])

  return (
    <AlertDialog open={open} onOpenChange={onOpenChange}>
      <AlertDialogContent className={contentClassName}>
        <AlertDialogHeader>
          <AlertDialogTitle>{title}</AlertDialogTitle>
          <AlertDialogDescription>{description}</AlertDialogDescription>
        </AlertDialogHeader>

        {children}

        {needsTypedConfirmation && (
          <div className="flex flex-col gap-2">
            <Label htmlFor={inputId}>
              {typedTargetLabel} <span className="font-mono font-semibold">{typedTarget}</span>
            </Label>
            <Input
              id={inputId}
              ref={inputRef}
              value={confirmInput}
              disabled={pending}
              autoFocus
              aria-invalid={Boolean(confirmInput && !typedConfirmationValid)}
              onChange={(event) => setConfirmInput(event.target.value)}
            />
          </div>
        )}

        {error && <p className="text-sm text-destructive">{error}</p>}

        <AlertDialogFooter>
          <AlertDialogCancel type="button">Cancel</AlertDialogCancel>
          <AlertDialogAction
            type="button"
            variant="destructive"
            disabled={!canConfirm}
            onClick={(event) => {
              event.preventDefault()
              if (canConfirm) onConfirm()
            }}
          >
            {pending && <Loader2 data-icon="inline-start" className="animate-spin" />}
            {confirmLabel}
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  )
}
