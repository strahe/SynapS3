import { useCallback, useEffect, useRef, useState } from 'react'

export type ClipboardCopyState = 'idle' | 'copied' | 'failed'

export function useClipboardCopy(resetDelayMs = 2000) {
  const [copyState, setCopyState] = useState<ClipboardCopyState>('idle')
  const mountedRef = useRef(true)
  const copyRequestRef = useRef(0)
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  useEffect(() => {
    mountedRef.current = true
    return () => {
      mountedRef.current = false
      copyRequestRef.current += 1
      if (timerRef.current) clearTimeout(timerRef.current)
    }
  }, [])

  const reset = useCallback(() => {
    copyRequestRef.current += 1
    if (timerRef.current) clearTimeout(timerRef.current)
    if (!mountedRef.current) return
    setCopyState('idle')
  }, [])

  const resetLater = useCallback(() => {
    if (timerRef.current) clearTimeout(timerRef.current)
    if (!mountedRef.current) return
    timerRef.current = setTimeout(() => {
      if (mountedRef.current) setCopyState('idle')
    }, resetDelayMs)
  }, [resetDelayMs])

  const copy = useCallback(
    async (value: string) => {
      const copyRequest = ++copyRequestRef.current
      try {
        if (!navigator.clipboard?.writeText) throw new Error('Clipboard unavailable')
        await navigator.clipboard.writeText(value)
        if (!mountedRef.current || copyRequest !== copyRequestRef.current) return
        setCopyState('copied')
      } catch {
        if (!mountedRef.current || copyRequest !== copyRequestRef.current) return
        setCopyState('failed')
      }

      resetLater()
    },
    [resetLater]
  )

  return { copyState, copy, reset }
}
