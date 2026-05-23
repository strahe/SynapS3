const defaultMaxLength = 24
const defaultPrefixLength = 10
const defaultSuffixLength = 6
const ellipsis = '...'

export interface CopyableValueModelInput {
  value: string
  displayValue?: string
  maxLength?: number
}

export interface CopyableValueModel {
  displayText: string
  tooltipValue: string
  copyValue: string
}

export function buildCopyableValueModel({
  value,
  displayValue,
  maxLength,
}: CopyableValueModelInput): CopyableValueModel {
  const visibleValue = displayValue ?? value
  return {
    displayText: middleTruncate(visibleValue, maxLength),
    tooltipValue: value,
    copyValue: value,
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
