import type { BucketItem } from '@/api/client'

type BucketCopyPolicy = Pick<BucketItem, 'default_copies' | 'effective_copies'>

export const inheritedCopyPolicyValue = 'inherit'
export const copyPolicyOptions = Array.from({ length: 8 }, (_, index) => index + 1)

export function bucketCopyPolicyValue(bucket: Pick<BucketItem, 'default_copies'>) {
  return bucket.default_copies == null ? inheritedCopyPolicyValue : bucket.default_copies.toString()
}

export function bucketCopyPolicyLabel(bucket: BucketCopyPolicy) {
  const copies = copyCountLabel(bucket.effective_copies)
  return bucket.default_copies == null ? `Inherits global default (${copies})` : `Override (${copies})`
}

export function bucketCopyPolicyInheritOptionLabel(bucket: BucketCopyPolicy) {
  if (bucket.default_copies != null) return 'Inherit global default'
  return `Inherit global default (${copyCountLabel(bucket.effective_copies)})`
}

export function bucketCopyPolicySavedMessage() {
  return 'Replica policy saved.'
}

export function bucketCopyPolicyEffectNote() {
  return 'Applies only to uploads started after this change. Existing objects and in-flight uploads keep their current replica plan.'
}

function copyCountLabel(copies: number) {
  return `${copies} ${copies === 1 ? 'copy' : 'copies'}`
}
