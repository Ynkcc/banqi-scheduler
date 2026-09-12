import { useCallback, useEffect, useRef, useState } from 'react'

export function usePoll<T>(load: () => Promise<T>, intervalMs = 5000) {
  const [data, setData] = useState<T>()
  const [error, setError] = useState<string>()
  const loadRef = useRef(load)
  loadRef.current = load

  const reload = useCallback(async () => {
    try {
      setData(await loadRef.current())
      setError(undefined)
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    }
  }, [])

  useEffect(() => {
    void reload()
    const timer = setInterval(() => void reload(), intervalMs)
    return () => clearInterval(timer)
  }, [reload, intervalMs])

  return { data, error, reload }
}
