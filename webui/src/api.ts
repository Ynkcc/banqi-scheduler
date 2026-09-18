import type {
  DataKind,
  EpisodePage,
  EvalView,
  Match,
  Network,
  RunningTask,
  Status,
  TrainConfigView,
  WorkerInfo,
} from './types'

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
  eval: () => request<EvalView>('/api/eval'),
  workers: () => request<WorkerInfo[]>('/api/workers'),
  tasks: () => request<RunningTask[]>('/api/tasks'),
  episodes: (before: number, limit = 100) =>
    request<EpisodePage>(`/api/episodes?before=${before}&limit=${limit}`),
  control: (body: {
    paused?: boolean
    initialRevealed?: number
    dataKind?: DataKind
    clearStop?: boolean
  }) =>
    request<{ ok: boolean }>('/api/control', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    }),
  trainConfig: () => request<TrainConfigView>('/api/train-config'),
  setTrainConfig: (overrides: Record<string, string>) =>
    request<{ ok: boolean }>('/api/train-config', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ overrides }),
    }),
}
