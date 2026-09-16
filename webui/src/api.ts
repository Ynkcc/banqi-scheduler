import type { DataKind, EpisodePage, Match, Network, RunningTask, Status, WorkerInfo } from './types'

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, init)
  if (!res.ok) {
    const body = await res.text()
    throw new Error(readError(body) ?? `${res.status} ${res.statusText}`)
  }
  return (await res.json()) as T
}

function readError(body: string): string | undefined {
  try {
    const parsed = JSON.parse(body) as { error?: string }
    return parsed.error
  } catch {
    return body || undefined
  }
}

export const api = {
  status: () => request<Status>('/api/status'),
  networks: () => request<Network[]>('/api/networks'),
  promote: (sha: string) => request<{ ok: boolean }>(`/api/networks/${sha}/promote`, { method: 'POST' }),
  matches: () => request<Match[]>('/api/matches'),
  workers: () => request<WorkerInfo[]>('/api/workers'),
  tasks: () => request<RunningTask[]>('/api/tasks'),
  episodes: (before: number, limit = 100) =>
    request<EpisodePage>(`/api/episodes?before=${before}&limit=${limit}`),
  control: (body: { paused?: boolean; initialRevealed?: number; dataKind?: DataKind }) =>
    request<{ ok: boolean }>('/api/control', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    }),
}
