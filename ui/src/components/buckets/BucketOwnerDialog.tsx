import { Loader2 } from 'lucide-react'
import { useEffect, useState } from 'react'
import { toast } from 'sonner'
import { BucketOwnerSelect } from '@/components/app/BucketOwnerSelect'
import { ReviewDetails } from '@/components/app/ReviewDetails'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Field, FieldDescription, FieldError, FieldGroup, FieldLabel } from '@/components/ui/field'
import { useS3Users, useUpdateBucketOwner } from '@/hooks/queries'
import { ownerLabel } from '@/lib/s3-owner'

/** Assigns or transfers a bucket's owner after the operator reviews the change. */
export function BucketOwnerDialog({
  bucketName,
  ownerAccessKey,
  open,
  onOpenChange,
}: {
  bucketName: string
  ownerAccessKey: string | null
  open: boolean
  onOpenChange: (open: boolean) => void
}) {
  const [selectedOwner, setSelectedOwner] = useState(ownerAccessKey ?? '')
  const [reviewing, setReviewing] = useState(false)
  const { data: users = [], isLoading: usersLoading, error: usersError } = useS3Users()
  const updateOwner = useUpdateBucketOwner()

  useEffect(() => {
    if (!open) {
      setSelectedOwner(ownerAccessKey ?? '')
      setReviewing(false)
    }
  }, [ownerAccessKey, open])

  const handleOpenChange = (next: boolean) => {
    if (!next) {
      setSelectedOwner(ownerAccessKey ?? '')
      setReviewing(false)
      updateOwner.reset()
    }
    onOpenChange(next)
  }

  const handleUpdate = () => {
    if (!selectedOwner || selectedOwner === ownerAccessKey) return
    if (!reviewing) {
      setReviewing(true)
      return
    }
    updateOwner.mutate(
      { name: bucketName, ownerAccessKey: selectedOwner },
      {
        onSuccess: () => {
          toast.success(`Owner of ${bucketName} changed to ${ownerLabel(selectedOwner, users)}`)
          handleOpenChange(false)
        },
      }
    )
  }

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>
            {reviewing ? 'Review bucket owner' : ownerAccessKey ? 'Change bucket owner' : 'Assign bucket owner'}
          </DialogTitle>
          <DialogDescription>
            {reviewing
              ? 'Confirm the owner that will receive full control of this bucket.'
              : `Transfer full control of "${bucketName}" to an existing S3 user.`}
          </DialogDescription>
        </DialogHeader>
        {reviewing ? (
          <ReviewDetails
            rows={[
              { id: 'bucket', label: 'Bucket', value: bucketName, copyable: true },
              {
                id: 'current-owner',
                label: 'Current owner',
                value: ownerAccessKey ?? ownerLabel(ownerAccessKey),
                displayValue: ownerLabel(ownerAccessKey, users),
                maxLength: ownerLabel(ownerAccessKey, users).length,
                copyable: Boolean(ownerAccessKey),
              },
              {
                id: 'new-owner',
                label: 'New owner',
                value: selectedOwner || ownerLabel(null),
                displayValue: ownerLabel(selectedOwner, users),
                maxLength: ownerLabel(selectedOwner, users).length,
                copyable: Boolean(selectedOwner),
              },
            ]}
          />
        ) : (
          <FieldGroup>
            <Field data-invalid={Boolean(usersError)}>
              <FieldLabel htmlFor="bucket-owner-select">Owner</FieldLabel>
              <BucketOwnerSelect
                id="bucket-owner-select"
                value={selectedOwner}
                onChange={setSelectedOwner}
                disabled={updateOwner.isPending || usersLoading}
                invalid={Boolean(usersError)}
                users={users}
              />
              {users.length === 0 && !usersLoading && (
                <FieldDescription>No S3 users yet. Internal root can be used as fallback owner.</FieldDescription>
              )}
              {usersError && <FieldError>Failed to load S3 users.</FieldError>}
            </Field>
          </FieldGroup>
        )}
        {updateOwner.error && (
          <Alert variant="destructive">
            <AlertDescription>{updateOwner.error.message}</AlertDescription>
          </Alert>
        )}
        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            onClick={() => (reviewing ? setReviewing(false) : handleOpenChange(false))}
            disabled={updateOwner.isPending}
          >
            {reviewing ? 'Back' : 'Cancel'}
          </Button>
          <Button
            type="button"
            onClick={handleUpdate}
            disabled={!selectedOwner || selectedOwner === ownerAccessKey || updateOwner.isPending}
          >
            {updateOwner.isPending && <Loader2 data-icon="inline-start" className="animate-spin" />}
            {reviewing ? 'Confirm owner' : 'Review'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
