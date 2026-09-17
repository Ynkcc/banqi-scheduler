export interface Counts {
  networks: number
  candidates: number
  matchesRunning: number
  episodes: number
  episodeGames: number
  workers: number
  evalResults: number
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

/** 自对弈产出的数据类别（与 proto DataKind 对应） */
export type DataKind = 'resnet' | 'nnue'

export interface Status {
  variant: string
  paused: boolean
  initialRevealed: number
  dataKind: DataKind
  minClientVersion: string
  sprt: Sprt
  best: Network | null
  counts: Counts
  eval: EvalConfig
  /** 绝对强度判据置位的停机信号（trainer 轮询 GetInfo 后优雅停止） */
  shouldStop: boolean
  stopReason: string
}

/** 绝对强度评估配置快照 */
export interface EvalConfig {
  enabled: boolean
  opponents: string[]
  games: number
  mctsSims: number
  mode: string
  everyNPromotions: number
  noProgressN: number
  noProgressEps: number
  /** 待下发评估任务数 */
  pending: number
}

/** 单次评估结果（被测网络 × 对手标识） */
export interface EvalResult {
  networkSha: string
  opponentSpec: string
  wins: number
  draws: number
  losses: number
  numGames: number
  winRate: number
  avgMoves: number
  createdAt: number
}

/** 单对手的版本序列（升序）与「连续无提升」计数 */
export interface EvalTrend {
  opponent: string
  versions: number
  noProgress: number
  latestWinRate: number
  points: EvalResult[]
}

export interface EvalView {
  config: EvalConfig
  shouldStop: boolean
  stopReason: string
  latest: EvalResult[]
  trends: EvalTrend[]
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
  dataKind: DataKind
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
  /** eval 任务的对手标识（rule:capture_first 等）；其余任务为空 */
  opponentSpec: string
  games: number
  createdAt: number
}
