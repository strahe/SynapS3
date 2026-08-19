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
  return 'Target replicas apply to new uploads. After the selected replica count is ready, the object is treated as stored and remaining replicas keep syncing. Cache can then be removed only if eviction is enabled.'
}

export function minimumDurableCopiesChoiceNote() {
  return 'Choose All replicas to follow later target changes. A number stays fixed.'
}

export function minimumDurableCopiesValue(bucket: Pick<BucketItem, 'minimum_durable_copies' | 'effective_copies'>) {
  if (bucket.minimum_durable_copies == null || bucket.minimum_durable_copies > bucket.effective_copies) {
    return strictMinimumDurableCopiesValue
  }
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
  if (targetCopies == null || Number(value) > targetCopies) return strictMinimumDurableCopiesValue
  return value
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
