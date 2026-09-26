import { Loader2 } from 'lucide-react'
import { type ReactNode, useEffect, useId, useRef, useState } from 'react'
import { Alert, AlertDescription } from '@/components/ui/alert'
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
import { Field, FieldGroup, FieldLabel } from '@/components/ui/field'
import { Input } from '@/components/ui/input'
import { confirmationMatches } from '@/lib/risk-confirmation'

export interface DangerActionAlertDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  title: ReactNode
  description: ReactNode
  confirmLabel: string
  onConfirm: () => void
  pending?: boolean
  confirmDisabled?: boolean
  error?: string | null
  typedTarget?: string
  confirmationResetKey?: string
  typedTargetLabel?: string
  contentClassName?: string
  bodyClassName?: string
  confirmationNotice?: ReactNode
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
  confirmDisabled = false,
  error,
  typedTarget,
  confirmationResetKey,
  typedTargetLabel = 'Type to confirm',
  contentClassName,
  bodyClassName,
  confirmationNotice,
  children,
}: DangerActionAlertDialogProps) {
  const inputId = useId()
  const inputRef = useRef<HTMLInputElement>(null)
  const [confirmInput, setConfirmInput] = useState('')
  const previousResetKey = useRef(confirmationResetKey)
  const needsTypedConfirmation = typedTarget !== undefined
  const typedConfirmationValid =
    !needsTypedConfirmation ||
    (previousResetKey.current === confirmationResetKey &&
      typedTarget.length > 0 &&
      confirmationMatches(confirmInput, typedTarget))
  const canConfirm = !pending && !confirmDisabled && typedConfirmationValid

  useEffect(() => {
    if (!open) setConfirmInput('')
  }, [open])

  useEffect(() => {
    if (previousResetKey.current !== confirmationResetKey) {
      previousResetKey.current = confirmationResetKey
      setConfirmInput('')
    }
  }, [confirmationResetKey])

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

        {bodyClassName ? <div className={bodyClassName}>{children}</div> : children}

        {confirmationNotice}

        {needsTypedConfirmation && (
          <FieldGroup>
            <Field data-invalid={Boolean(confirmInput && !typedConfirmationValid)}>
              <FieldLabel htmlFor={inputId}>
                {typedTargetLabel} <span className="font-mono font-semibold">{typedTarget}</span>
              </FieldLabel>
              <Input
                id={inputId}
                ref={inputRef}
                value={confirmInput}
                disabled={pending}
                autoFocus
                aria-invalid={Boolean(confirmInput && !typedConfirmationValid)}
                onChange={(event) => setConfirmInput(event.target.value)}
              />
            </Field>
          </FieldGroup>
        )}

        {error && (
          <Alert variant="destructive">
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}

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
