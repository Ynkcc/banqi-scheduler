import { Alert, Button, Card, Space, Table, Tag, Typography } from 'antd'
import type { TableProps } from 'antd'
import { useState } from 'react'
import { api } from '../api'
import { fmtTime, shortSha, winnerText } from '../format'
import type { Episode } from '../types'
import { usePoll } from '../usePoll'

const limit = 100

const columns: TableProps<Episode>['columns'] = [
  { title: 'ID', dataIndex: 'id', width: 90 },
  { title: 'Worker', dataIndex: 'workerId' },
  {
    title: '网络',
    dataIndex: 'networkSha',
    render: (v: string) => <Typography.Text code>{shortSha(v)}</Typography.Text>,
  },
  { title: '局数', dataIndex: 'gameCount', width: 90 },
  { title: '步数', dataIndex: 'totalSteps', width: 120 },
  {
    title: '结果',
    dataIndex: 'winner',
    width: 90,
    render: (v: number) => <Tag color={v > 0 ? 'red' : v < 0 ? 'black' : 'default'}>{winnerText(v)}</Tag>,
  },
  { title: '登记时间', dataIndex: 'createdAt', render: (v: number) => fmtTime(v) },
  {
    title: 'R2 对象键',
    dataIndex: 'objectKey',
    ellipsis: true,
    render: (v: string) => <Typography.Text type="secondary">{v}</Typography.Text>,
  },
]

export default function Episodes() {
  const [page, setPage] = useState(1)
  const [cursors, setCursors] = useState<number[]>([0])
  const before = cursors[page - 1] ?? 0
  const { data, error } = usePoll(() => api.episodes(before, limit), 10000)

  const goNext = () => {
    const last = data?.items[data.items.length - 1]
    if (!last || !data?.hasMore) return
    setCursors([...cursors, last.id])
    setPage(page + 1)
  }

  if (error) {
    return <Alert type="error" showIcon message="加载 /api/episodes 失败" description={error} />
  }

  return (
    <Card
      size="small"
      title={`Episode（按 ID 倒序，第 ${page} 页，本页 ${data?.items.length ?? 0} 条）`}
      extra={
        <Space>
          <Button size="small" disabled={page === 1} onClick={() => setPage(page - 1)}>
            上一页
          </Button>
          <Button size="small" disabled={!data?.hasMore} onClick={goNext}>
            下一页
          </Button>
        </Space>
      }
    >
      <Table<Episode>
        rowKey="id"
        size="small"
        columns={columns}
        dataSource={data?.items ?? []}
        pagination={false}
      />
    </Card>
  )
}
