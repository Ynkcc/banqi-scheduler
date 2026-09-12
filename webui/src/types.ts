export interface Counts {
  networks: number
  candidates: number
  matchesRunning: number
  episodes: number
  episodeGames: number
  workers: number
}

export interface Network {
  sha: string
  parentSha: string
  createdAt: number
  isBest: boolean
  status: string
  notes: string
}

export interface Sprt {
  elo0: number
  elo1: number
  alpha: number
  beta: number
}

export interface Status {
  variant: string
  paused: boolean
  initialRevealed: number
  minClientVersion: string
  sprt: Sprt
  best: Network | null
  counts: Counts
}

export interface Match {
  id: number
  candidate: string
  opponent: string
  status: string
  pairs: number[]
  numGames: number
  targetGames: number
  score: number | null
  llr: number
  lower: number
  upper: number
  verdict: string
}

export interface WorkerInfo {
  id: string
  lastSeen: number
  online: boolean
  threads: number
  completedGames: number
  clientVersion: string
  memoryMb: number
}

export interface Episode {
  id: number
  workerId: string
  taskId: string
  networkSha: string
  gameCount: number
  totalSteps: number
  winner: number
  objectKey: string
  createdAt: number
}

export interface EpisodePage {
  items: Episode[]
  nextBefore: number
  hasMore: boolean
}

export interface RunningTask {
  taskId: string
  workerId: string
  kind: string
  matchId: number
  networkSha: string
  opponentSha: string
  games: number
  createdAt: number
}
