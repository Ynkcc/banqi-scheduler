import { Alert, Card, Progress, Table, Tag, Typography } from 'antd'
import type { TableProps } from 'antd'
import { api } from '../api'
import { shortSha } from '../format'
import type { Match } from '../types'
import { usePoll } from '../usePoll'

const verdictColor: Record<string, string> = {
  accept: 'green',
  reject: 'red',
  continue: 'blue',
}

const columns: TableProps<Match>['columns'] = [
  { title: 'ID', dataIndex: 'id', width: 70 },
  {
    title: '候选',
    dataIndex: 'candidate',
    render: (v: string) => <Typography.Text code>{shortSha(v)}</Typography.Text>,
  },
  {
    title: '对手 best',
    dataIndex: 'opponent',
    render: (v: string) => <Typography.Text code>{shortSha(v)}</Typography.Text>,
  },
  {
    title: '状态',
    dataIndex: 'status',
    render: (v: string) => <Tag color={v === 'running' ? 'processing' : 'default'}>{v}</Tag>,
  },
  {
    title: '进度（局）',
    width: 180,
    render: (_, m) => (
      <Progress
        percent={m.targetGames > 0 ? Math.min(100, Math.round((m.numGames / (m.targetGames * 2)) * 100)) : 0}
        size="small"
        format={() => `${m.numGames}/${m.targetGames * 2}`}
      />
    ),
  },
  {
    title: 'LL / LD / DD / DW / WW',
    render: (_, m) => m.pairs.join(' / '),
  },
  {
    title: '得分率',
    render: (_, m) => (m.score === null ? '-' : `${(m.score * 100).toFixed(1)}%`),
  },
  {
    title: 'LLR',
    width: 220,
    render: (_, m) => (
      <Typography.Text>
        {m.llr.toFixed(3)} <Typography.Text type="secondary">({m.lower.toFixed(2)} ~ {m.upper.toFixed(2)})</Typography.Text>
      </Typography.Text>
    ),
  },
  {
    title: 'SPRT 判定',
    dataIndex: 'verdict',
    render: (v: string) => <Tag color={verdictColor[v] ?? 'default'}>{v}</Tag>,
  },
]

export default function Matches() {
  const { data, error } = usePoll(api.matches, 5000)

  if (error) {
    return <Alert type="error" showIcon message="加载 /api/matches 失败" description={error} />
  }

  return (
    <Card size="small" title={`Gatekeeper 对战（${data?.length ?? 0}）`}>
      <Table<Match> rowKey="id" size="small" columns={columns} dataSource={data ?? []} pagination={false} />
    </Card>
  )
}
