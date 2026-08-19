import type { BucketItem } from '@/api/client'

type BucketCopyPolicy = Pick<
  BucketItem,
  'default_copies' | 'effective_copies' | 'minimum_durable_copies' | 'effective_minimum_durable_copies'
>

export const inheritedCopyPolicyValue = 'inherit'
export const strictMinimumDurableCopiesValue = 'strict'
export const copyPolicyOptions = Array.from({ length: 8 }, (_, index) => index + 1)

export function bucketCopyPolicyValue(bucket: Pick<BucketItem, 'default_copies'>) {
  return bucket.default_copies == null ? inheritedCopyPolicyValue : bucket.default_copies.toString()
}

export function bucketCopyPolicyLabel(bucket: BucketCopyPolicy) {
  const copies = copyCountLabel(bucket.effective_copies)
  return bucket.default_copies == null ? `Inherits global default (${copies})` : `Override (${copies})`
}

export function bucketCopyPolicyInheritOptionLabel(bucket: BucketCopyPolicy, runtimeDefaultCopies?: number) {
  const copies = bucket.default_copies == null ? bucket.effective_copies : runtimeDefaultCopies
  if (copies == null) return 'Inherit current runtime default'
  return `Inherit current runtime default (${copyCountLabel(copies)})`
}

export function bucketCopyPolicySavedMessage() {
  return 'Replica policy saved.'
}

export function bucketCopyPolicyEffectNote() {
  return 'New uploads use the Replicas target. Cache can be removed after the Release cache after count is ready, if eviction is enabled. Remaining target replicas keep syncing.'
}

export function minimumDurableCopiesChoiceNote() {
  return 'All replicas waits for every target replica of that upload. A number stays fixed if the target changes later.'
}

export function minimumDurableCopiesValue(bucket: Pick<BucketItem, 'minimum_durable_copies' | 'effective_copies'>) {
  if (bucket.minimum_durable_copies == null) return strictMinimumDurableCopiesValue
  if (bucket.minimum_durable_copies > bucket.effective_copies) return bucket.effective_copies.toString()
  return bucket.minimum_durable_copies.toString()
}

export function minimumDurableCopiesLabel(bucket: BucketCopyPolicy) {
  if (bucket.minimum_durable_copies == null) return 'All replicas (strict)'
  return `${bucket.effective_minimum_durable_copies} of ${bucket.effective_copies} ${bucket.effective_copies === 1 ? 'replica' : 'replicas'}`
}

export function minimumDurableCopiesOptionLabel(copies: number) {
  return `${copies} ${copies === 1 ? 'replica' : 'replicas'}`
}

export function selectedTargetCopies(copyPolicy: string, runtimeDefaultCopies?: number) {
  if (copyPolicy === inheritedCopyPolicyValue) return runtimeDefaultCopies ?? null
  const copies = Number(copyPolicy)
  return Number.isInteger(copies) && copies >= 1 && copies <= 8 ? copies : null
}

export function minimumDurableCopiesOptions(targetCopies: number | null) {
  if (targetCopies == null) return []
  return copyPolicyOptions.filter((copies) => copies <= targetCopies)
}

export function clampMinimumDurableCopiesValue(value: string, targetCopies: number | null) {
  if (value === strictMinimumDurableCopiesValue) return value
  if (targetCopies == null) return strictMinimumDurableCopiesValue
  const copies = Number(value)
  if (!Number.isInteger(copies) || copies < 1) return strictMinimumDurableCopiesValue
  if (copies > targetCopies) return targetCopies.toString()
  return value
}

export function persistMinimumDurableCopies(
  selected: string,
  storedMinimum: number | null,
  targetCopies: number | null
): number | null | undefined {
  if (selected === strictMinimumDurableCopiesValue) {
    return storedMinimum == null ? undefined : null
  }
  const selectedNumber = Number(selected)
  if (!Number.isInteger(selectedNumber) || selectedNumber < 1 || selectedNumber > 8) {
    return undefined
  }
  const next = targetCopies != null && selectedNumber > targetCopies ? targetCopies : selectedNumber
  if (storedMinimum === next) return undefined
  return next
}

export function minimumDurableCopiesWarning() {
  return 'Cache may be removed before every target replica is ready, depending on cache eviction settings. Raising this later cannot restore cache that has already been deleted.'
}

export function showsMinimumDurableCopiesWarning(value: string, targetCopies: number | null) {
  if (value === strictMinimumDurableCopiesValue) return false
  const copies = Number(value)
  return Number.isInteger(copies) && targetCopies != null && copies < targetCopies
}

function copyCountLabel(copies: number) {
  return `${copies} ${copies === 1 ? 'copy' : 'copies'}`
}
