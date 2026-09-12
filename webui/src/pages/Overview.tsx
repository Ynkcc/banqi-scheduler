import {
  Alert,
  App,
  Button,
  Card,
  Col,
  Descriptions,
  InputNumber,
  Row,
  Space,
  Statistic,
  Switch,
  Table,
  Tag,
  Typography,
} from 'antd'
import type { TableProps } from 'antd'
import { useEffect, useState } from 'react'
import { api } from '../api'
import { fmtAgo, shortSha } from '../format'
import type { RunningTask } from '../types'
import { usePoll } from '../usePoll'

const taskColumns: TableProps<RunningTask>['columns'] = [
  { title: '任务', dataIndex: 'taskId', render: (v: string) => <Typography.Text code>{v.slice(0, 12)}</Typography.Text> },
  {
    title: '类型',
    dataIndex: 'kind',
    render: (v: string) => <Tag color={v === 'TASK_RATING' ? 'gold' : 'blue'}>{v.replace('TASK_', '')}</Tag>,
  },
  { title: 'Worker', dataIndex: 'workerId' },
  { title: '网络', dataIndex: 'networkSha', render: (v: string) => <Typography.Text code>{shortSha(v)}</Typography.Text> },
  { title: '对手', dataIndex: 'opponentSha', render: (v: string) => (v ? <Typography.Text code>{shortSha(v)}</Typography.Text> : '-') },
  { title: '局数', dataIndex: 'games', width: 80 },
  { title: 'Match', dataIndex: 'matchId', width: 90, render: (v: number) => (v > 0 ? v : '-') },
  { title: '下发时间', dataIndex: 'createdAt', render: (v: number) => fmtAgo(v) },
]

export default function Overview() {
  const { message } = App.useApp()
  const { data, error } = usePoll(api.status, 5000)
  const tasks = usePoll(api.tasks, 5000)
  const [revealed, setRevealed] = useState<number | null>(null)
  const [busy, setBusy] = useState(false)

  useEffect(() => {
    if (data && revealed === null) {
      setRevealed(data.initialRevealed)
    }
  }, [data, revealed])

  const report = (e: unknown) => message.error(`操作失败：${e instanceof Error ? e.message : String(e)}`)

  const setPaused = async (paused: boolean) => {
    setBusy(true)
    try {
      await api.control({ paused })
      message.success(paused ? '已暂停 selfplay（rating 继续）' : '已恢复 selfplay')
    } catch (e) {
      report(e)
    } finally {
      setBusy(false)
    }
  }

  const applyRevealed = async () => {
    if (revealed === null) return
    setBusy(true)
    try {
      await api.control({ initialRevealed: revealed })
      message.success(`课程阶段已切换：initial_revealed_pieces=${revealed}`)
    } catch (e) {
      report(e)
    } finally {
      setBusy(false)
    }
  }

  if (error) {
    return <Alert type="error" showIcon message="加载 /api/status 失败" description={error} />
  }

  const counts = data?.counts
  const best = data?.best

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      <Row gutter={16}>
        <Col span={4}>
          <Card size="small">
            <Statistic title="网络" value={counts?.networks ?? '-'} suffix={counts ? `(${counts.candidates} 候选)` : ''} />
          </Card>
        </Col>
        <Col span={4}>
          <Card size="small">
            <Statistic title="进行中对战" value={counts?.matchesRunning ?? '-'} />
          </Card>
        </Col>
        <Col span={4}>
          <Card size="small">
            <Statistic title="Episode" value={counts?.episodes ?? '-'} />
          </Card>
        </Col>
        <Col span={4}>
          <Card size="small">
            <Statistic title="累计自对弈局数" value={counts?.episodeGames ?? '-'} />
          </Card>
        </Col>
        <Col span={4}>
          <Card size="small">
            <Statistic title="Worker" value={counts?.workers ?? '-'} />
          </Card>
        </Col>
        <Col span={4}>
          <Card size="small">
            <Statistic title="变体" value={data?.variant ?? '-'} />
          </Card>
        </Col>
      </Row>

      <Row gutter={16}>
        <Col span={10}>
          <Card size="small" title="运行控制">
            <Space direction="vertical" size="middle" style={{ width: '100%' }}>
              <Space>
                <Switch checked={data?.paused} disabled={!data || busy} onChange={setPaused} />
                <span>暂停 selfplay</span>
                <Typography.Text type="secondary">暂停后仅下发 rating 任务</Typography.Text>
              </Space>
              <Space>
                <span>初始翻子数</span>
                <InputNumber
                  min={0}
                  max={32}
                  value={revealed}
                  disabled={busy}
                  onChange={(v) => setRevealed(typeof v === 'number' ? v : null)}
                />
                <Button
                  type="primary"
                  loading={busy}
                  disabled={revealed === null || revealed === data?.initialRevealed}
                  onClick={applyRevealed}
                >
                  应用
                </Button>
                <Typography.Text type="secondary">0 = 使用变体默认值</Typography.Text>
              </Space>
            </Space>
          </Card>
        </Col>
        <Col span={14}>
          <Card size="small" title="当前 best 网络">
            {best ? (
              <Descriptions column={1} size="small">
                <Descriptions.Item label="sha">
                  <Typography.Text code copyable>
                    {best.sha}
                  </Typography.Text>
                </Descriptions.Item>
                <Descriptions.Item label="父网络">{best.parentSha || '-'}</Descriptions.Item>
                <Descriptions.Item label="登记时间">{fmtAgo(best.createdAt)}</Descriptions.Item>
                <Descriptions.Item label="状态">{best.status}</Descriptions.Item>
              </Descriptions>
            ) : (
              <Typography.Text type="secondary">尚未登记任何网络</Typography.Text>
            )}
          </Card>
        </Col>
      </Row>

      <Card size="small" title={`进行中任务（进程内，${tasks.data?.length ?? 0}）`}>
        <Table<RunningTask>
          rowKey="taskId"
          size="small"
          columns={taskColumns}
          dataSource={tasks.data ?? []}
          pagination={false}
          locale={{ emptyText: '无进行中任务' }}
        />
      </Card>
    </Space>
  )
}
