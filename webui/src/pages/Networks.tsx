import { Alert, App, Button, Card, Popconfirm, Table, Tag, Typography } from 'antd'
import type { TableProps } from 'antd'
import { useState } from 'react'
import { api } from '../api'
import { fmtTime, shortSha } from '../format'
import type { Network } from '../types'
import { usePoll } from '../usePoll'

const statusColor: Record<string, string> = {
  best: 'green',
  candidate: 'blue',
  archived: 'default',
  rejected: 'red',
}

export default function Networks() {
  const { message } = App.useApp()
  const { data, error, reload } = usePoll(api.networks, 10000)
  const [busySha, setBusySha] = useState<string>()

  const promote = async (sha: string) => {
    setBusySha(sha)
    try {
      await api.promote(sha)
      message.success(`已晋级 ${shortSha(sha)}`)
      await reload()
    } catch (e) {
      message.error(`晋级失败：${e instanceof Error ? e.message : String(e)}`)
    } finally {
      setBusySha(undefined)
    }
  }

  const columns: TableProps<Network>['columns'] = [
    {
      title: 'sha',
      dataIndex: 'sha',
      render: (v: string, row) => (
        <Typography.Text code copyable>
          {row.isBest ? v : shortSha(v)}
        </Typography.Text>
      ),
    },
    { title: '父网络', dataIndex: 'parentSha', render: (v: string) => shortSha(v) },
    {
      title: '状态',
      dataIndex: 'status',
      render: (v: string) => <Tag color={statusColor[v] ?? 'default'}>{v}</Tag>,
    },
    { title: '登记时间', dataIndex: 'createdAt', render: (v: number) => fmtTime(v) },
    { title: '备注', dataIndex: 'notes', render: (v: string) => v || '-' },
    {
      title: '操作',
      width: 120,
      render: (_, row) =>
        row.isBest ? (
          <Tag color="green">best</Tag>
        ) : (
          <Popconfirm
            title={`确认将 ${shortSha(row.sha)} 设为 best？`}
            description="会立即切换 best 指针，selfplay 将改用该网络。"
            okText="确认"
            cancelText="取消"
            onConfirm={() => promote(row.sha)}
          >
            <Button size="small" loading={busySha === row.sha}>
              手动晋级
            </Button>
          </Popconfirm>
        ),
    },
  ]

  if (error) {
    return <Alert type="error" showIcon message="加载 /api/networks 失败" description={error} />
  }

  return (
    <Card size="small" title={`网络（${data?.length ?? 0}）`}>
      <Table<Network> rowKey="sha" size="small" columns={columns} dataSource={data ?? []} pagination={false} />
    </Card>
  )
}
