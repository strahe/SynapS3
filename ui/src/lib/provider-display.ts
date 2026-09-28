/** How the dashboard writes a provider's registry ID. A bare number reads as an index or a rank. */
export function providerRegistryLabel(providerID: string) {
  return `Registry ${providerID}`
}

/** A provider reads as its name; the registry ID stands in when it has none. */
export function providerDisplayName(providerID: string, name?: string | null) {
  return name?.trim() || providerRegistryLabel(providerID)
}

/**
 * Registry locations are free text, usually written like a certificate subject
 * (`C=US;ST=Texas;L=Austin`). Structured values read as "Austin, Texas, US", or
 * "Austin, US" when compact; anything else is shown as declared.
 */
export function providerLocationLabel(location?: string | null, options: { compact?: boolean } = {}) {
  const declared = location?.trim()
  if (!declared) return undefined

  const fields = new Map<string, string>()
  for (const segment of declared.split(/[;,]/)) {
    const separator = segment.indexOf('=')
    if (separator <= 0) {
      if (segment.trim()) return declared
      continue
    }
    const value = segment.slice(separator + 1).trim()
    if (value) fields.set(segment.slice(0, separator).trim().toUpperCase(), value)
  }

  const locality = fields.get('L')
  const state = fields.get('ST')
  const country = fields.get('C')
  const parts = options.compact ? [locality ?? state, country] : [locality, state, country]
  const label: string[] = []
  for (const part of parts) {
    if (!part) continue
    const previous = label[label.length - 1]
    if (previous && sameLocationPart(previous, part)) continue
    label.push(part)
  }
  return label.length > 0 ? label.join(', ') : declared
}

function sameLocationPart(left: string, right: string) {
  return left.replace(/\s+/g, '').toLowerCase() === right.replace(/\s+/g, '').toLowerCase()
}
