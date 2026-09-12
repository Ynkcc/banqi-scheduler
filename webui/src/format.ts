import dayjs from 'dayjs'
import 'dayjs/locale/zh-cn'
import relativeTime from 'dayjs/plugin/relativeTime'

dayjs.extend(relativeTime)
dayjs.locale('zh-cn')

export const shortSha = (sha: string, n = 12) => (sha ? sha.slice(0, n) : '-')

export const fmtTime = (unix: number) => (unix ? dayjs.unix(unix).format('YYYY-MM-DD HH:mm:ss') : '-')

export const fmtAgo = (unix: number) => (unix ? dayjs.unix(unix).fromNow() : '-')

export const fmtMemory = (mb: number) => (mb > 0 ? `${(mb / 1024).toFixed(1)} GiB` : '-')

export const winnerText = (winner: number) => (winner > 0 ? '红胜' : winner < 0 ? '黑胜' : '和棋')
