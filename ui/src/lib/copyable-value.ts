const defaultMaxLength = 24
const defaultPrefixLength = 10
const defaultSuffixLength = 6
const ellipsis = '...'

export interface CopyableValueModelInput {
  value: string
  displayValue?: string
  maxLength?: number
  linkHref?: string
  external?: boolean
}

export interface CopyableValueModel {
  displayText: string
  tooltipValue: string
  copyValue: string
  linkHref?: string
  external: boolean
}

export function buildCopyableValueModel({
  value,
  displayValue,
  maxLength,
  linkHref,
  external = false,
}: CopyableValueModelInput): CopyableValueModel {
  const visibleValue = displayValue ?? value
  const safeLinkHref = safeAbsoluteHttpURL(linkHref)
  return {
    displayText: middleTruncate(visibleValue, maxLength),
    tooltipValue: value,
    copyValue: value,
    linkHref: safeLinkHref,
    external: Boolean(safeLinkHref && external),
  }
}

function safeAbsoluteHttpURL(value: string | undefined) {
  if (!value) return undefined

  try {
    const url = new URL(value)
    return url.protocol === 'http:' || url.protocol === 'https:' ? url.href : undefined
  } catch {
    return undefined
  }
}

export function middleTruncate(value: string, maxLength?: number) {
  const limit = maxLength ?? defaultMaxLength
  if (value.length <= limit) return value

  if (maxLength !== undefined) return dynamicMiddleTruncate(value, limit)

  return `${value.slice(0, defaultPrefixLength)}${ellipsis}${value.slice(-defaultSuffixLength)}`
}

function dynamicMiddleTruncate(value: string, maxLength: number) {
  if (maxLength <= ellipsis.length) return ellipsis.slice(0, Math.max(0, maxLength))

  const fixedLength = defaultPrefixLength + defaultSuffixLength + ellipsis.length
  if (fixedLength <= maxLength) {
    const availableLength = maxLength - ellipsis.length
    const prefixLength = Math.ceil(availableLength / 2)
    const suffixLength = Math.floor(availableLength / 2)
    return truncateWithParts(value, prefixLength, suffixLength)
  }

  const availableLength = maxLength - ellipsis.length
  const prefixLength = Math.ceil(availableLength / 2)
  const suffixLength = Math.floor(availableLength / 2)
  return truncateWithParts(value, prefixLength, suffixLength)
}

function truncateWithParts(value: string, prefixLength: number, suffixLength: number) {
  const suffix = suffixLength > 0 ? value.slice(-suffixLength) : ''
  return `${value.slice(0, prefixLength)}${ellipsis}${suffix}`
}
