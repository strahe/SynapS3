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

export function objectUploadKey(prefix: string, fileName: string) {
  if (prefix === '' || prefix.endsWith('/')) return `${prefix}${fileName}`
  return `${prefix}/${fileName}`
}

export function duplicateObjectUploadKeys(keys: string[]) {
  const seen = new Set<string>()
  const duplicates = new Set<string>()
  const result: string[] = []

  for (const key of keys) {
    if (seen.has(key) && !duplicates.has(key)) {
      duplicates.add(key)
      result.push(key)
    }
    seen.add(key)
  }

  return result
}
