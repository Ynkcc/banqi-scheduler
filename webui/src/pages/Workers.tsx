import { Alert, Card, Table, Tag, Typography } from 'antd'
import type { TableProps } from 'antd'
import { api } from '../api'
import { fmtAgo, fmtMemory } from '../format'
import type { WorkerInfo } from '../types'
import { usePoll } from '../usePoll'

const columns: TableProps<WorkerInfo>['columns'] = [
  {
    title: '角色',
    dataIndex: 'id',
    width: 100,
    render: (_: string, w: WorkerInfo) =>
      w.id.startsWith('trainer-') ? <Tag color="purple">trainer</Tag> : <Tag color="blue">collector</Tag>,
  },
  { title: 'worker', dataIndex: 'id' },
  {
    title: '在线',
    dataIndex: 'online',
    width: 100,
    render: (v: boolean) => <Tag color={v ? 'green' : 'default'}>{v ? '在线' : '离线'}</Tag>,
  },
  { title: '最后心跳', dataIndex: 'lastSeen', render: (v: number) => fmtAgo(v) },
  { title: '线程', dataIndex: 'threads', width: 90 },
  { title: '可用内存', dataIndex: 'memoryMb', width: 120, render: (v: number) => fmtMemory(v) },
  { title: '已完成局数', dataIndex: 'completedGames', width: 120 },
  {
    title: '版本',
    dataIndex: 'clientVersion',
    render: (v: string) => v || <Typography.Text type="secondary">-</Typography.Text>,
  },
]

export default function Workers() {
  const { data, error } = usePoll(api.workers, 5000)

  if (error) {
    return <Alert type="error" showIcon message="加载 /api/workers 失败" description={error} />
  }

  return (
    <Card size="small" title={`Worker（${data?.filter((w) => w.online).length ?? 0} 在线 / ${data?.length ?? 0} 总计）`}>
      <Table<WorkerInfo> rowKey="id" size="small" columns={columns} dataSource={data ?? []} pagination={false} />
    </Card>
  )
}
