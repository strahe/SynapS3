import type { BucketItem } from '@/api/client'

type BucketCopyPolicy = Pick<BucketItem, 'default_copies' | 'minimum_durable_copies'>

export const copyPolicyOptions = Array.from({ length: 8 }, (_, index) => index + 1)

export function bucketCopyPolicyValue(bucket: Pick<BucketItem, 'default_copies'>) {
  return bucket.default_copies.toString()
}

export function bucketCopyPolicyLabel(bucket: Pick<BucketItem, 'default_copies'>) {
  return copyCountLabel(bucket.default_copies)
}

export function replicaTargetChoiceNote() {
  return 'Applies to new uploads. Existing objects keep the replica target they started with.'
}

// Lowering the target would leave the replicas above it stored and billed with
// nothing to retire them, so the server refuses it. Offer those counts as
// visibly unavailable rather than hiding them, so the choice reads as
// temporarily closed instead of missing.
export function replicaTargetLocked(copies: number, currentCopies: number) {
  return copies < currentCopies
}

export function replicaTargetLockNote() {
  return 'Lowering the replica target is not supported yet.'
}

export function bucketCopyPolicySavedMessage() {
  return 'Saved. New uploads use this replica target. Cache can be released after the selected count.'
}

export function minimumDurableCopiesChoiceNote() {
  return 'Keeps cache until every replica of that upload is stored.'
}

export function minimumDurableCopiesFixedCountNote() {
  return 'Keeps this count if Replicas later increases.'
}

export function minimumDurableCopiesValue(bucket: BucketCopyPolicy) {
  return Math.min(bucket.minimum_durable_copies, bucket.default_copies).toString()
}

export function minimumDurableCopiesLabel(bucket: BucketCopyPolicy) {
  const target = bucket.default_copies
  return `${Math.min(bucket.minimum_durable_copies, target)} of ${target} ${target === 1 ? 'replica' : 'replicas'}`
}

export function minimumDurableCopiesOptionLabel(copies: number, targetCopies?: number | null) {
  const count = `${copies} ${copies === 1 ? 'replica' : 'replicas'}`
  if (targetCopies != null && copies === targetCopies) {
    return `${count} (all replicas)`
  }
  return count
}

export function selectedTargetCopies(copyPolicy: string) {
  const copies = Number(copyPolicy)
  return Number.isInteger(copies) && copies >= 1 && copies <= 8 ? copies : null
}

export function minimumDurableCopiesOptions(targetCopies: number | null) {
  if (targetCopies == null) return []
  return copyPolicyOptions.filter((copies) => copies <= targetCopies)
}

export function clampMinimumDurableCopiesValue(value: string, targetCopies: number | null) {
  if (targetCopies == null) return value
  const copies = Number(value)
  if (!Number.isInteger(copies) || copies < 1) return targetCopies.toString()
  if (copies > targetCopies) return targetCopies.toString()
  return value
}

export function persistMinimumDurableCopies(
  selected: string,
  storedMinimum: number,
  targetCopies: number | null
): number | undefined {
  const selectedNumber = Number(selected)
  if (!Number.isInteger(selectedNumber) || selectedNumber < 1 || selectedNumber > 8) {
    return undefined
  }
  const next = targetCopies != null && selectedNumber > targetCopies ? targetCopies : selectedNumber
  if (storedMinimum === next) return undefined
  return next
}

export function minimumDurableCopiesWarning() {
  return 'Cache may be removed before every target replica is ready. Raising this later cannot restore deleted cache.'
}

export function showsMinimumDurableCopiesWarning(value: string, targetCopies: number | null) {
  const copies = Number(value)
  return Number.isInteger(copies) && targetCopies != null && copies < targetCopies
}

function copyCountLabel(copies: number) {
  return `${copies} ${copies === 1 ? 'copy' : 'copies'}`
}
