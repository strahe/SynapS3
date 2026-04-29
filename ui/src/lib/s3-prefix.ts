export interface BucketPrefixCrumb {
  label: string
  prefix: string
}

export function bucketPrefixCrumbs(prefix: string): BucketPrefixCrumb[] {
  const crumbs: BucketPrefixCrumb[] = []
  let segmentStart = 0

  for (let index = 0; index < prefix.length; index += 1) {
    if (prefix[index] !== '/') continue

    const segment = prefix.slice(segmentStart, index)
    crumbs.push({
      label: segment || '/',
      prefix: prefix.slice(0, index + 1),
    })
    segmentStart = index + 1
  }

  if (segmentStart < prefix.length) {
    crumbs.push({
      label: prefix.slice(segmentStart),
      prefix,
    })
  }

  return crumbs
}
