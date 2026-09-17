import { Alert, App, Button, Card, Descriptions, Space, Statistic, Table, Tag, Typography } from 'antd'
import type { TableProps } from 'antd'
import { api } from '../api'
import { fmtAgo, shortSha } from '../format'
import type { EvalConfig, EvalResult } from '../types'
import { usePoll } from '../usePoll'

/** 对手标识 → 中文名（与 collector 的 rule_opponents.rs / 调度器 eval.go 同一套标识） */
function opponentLabel(spec: string): string {
  switch (spec) {
    case 'random':
      return '随机'
    case 'rule:capture_first':
      return '优先吃子'
    case 'rule:reveal_first':
      return '优先翻棋'
    default:
      return spec || '-'
  }
}

const pct = (v: number) => `${(v * 100).toFixed(1)}%`

/**
 * 无依赖的胜率迷你趋势图（不引入图表库）。
 * 纵向跨度固定下限 5pt：全平/微动序列也能看出是「一条平线」而不是被拉伸成锯齿。
 */
function Sparkline({ rates, eps }: { rates: number[]; eps: number }) {
  const w = 260
  const h = 52
  if (rates.length < 2) {
    return <Typography.Text type="secondary">仅 {rates.length} 个版本，暂不成曲线</Typography.Text>
  }
  const min = Math.min(...rates)
  const span = Math.max(Math.max(...rates) - min, 0.05)
  const xy = rates.map((r, i) => {
    const x = (i / (rates.length - 1)) * (w - 8) + 4
    const y = h - 4 - ((r - min) / span) * (h - 8)
    return `${x.toFixed(1)},${y.toFixed(1)}`
  })
  const improved = rates[rates.length - 1] - rates[0] >= eps
  return (
    <svg width={w} height={h} role="img" aria-label="胜率趋势">
      <polyline points={xy.join(' ')} fill="none" stroke={improved ? '#52c41a' : '#faad14'} strokeWidth={2} />
    </svg>
  )
}

function noProgressTag(np: number, cfg: EvalConfig) {
  if (cfg.noProgressN <= 0) {
    return <Tag>仅观测（未启用判停）</Tag>
  }
  if (np >= cfg.noProgressN) {
    return <Tag color="red">连续无提升 {np}/{cfg.noProgressN}（已达停机条件）</Tag>
  }
  if (np > 0) {
    return <Tag color="orange">连续无提升 {np}/{cfg.noProgressN}</Tag>
  }
  return <Tag color="green">有提升</Tag>
}

const pointColumns: TableProps<EvalResult>['columns'] = [
  { title: '版本', dataIndex: 'networkSha', render: (v: string) => <Typography.Text code>{shortSha(v)}</Typography.Text> },
  { title: '胜', dataIndex: 'wins', width: 70 },
  { title: '和', dataIndex: 'draws', width: 60 },
  { title: '负', dataIndex: 'losses', width: 70 },
  { title: '胜率', dataIndex: 'winRate', width: 90, render: (v: number) => <b>{pct(v)}</b> },
  { title: '步均', dataIndex: 'avgMoves', width: 80, render: (v: number) => v.toFixed(1) },
  { title: '局数', dataIndex: 'numGames', width: 80 },
  { title: '时间', dataIndex: 'createdAt', render: (v: number) => fmtAgo(v) },
]

export default function Eval() {
  const { message } = App.useApp()
  const { data, error, reload } = usePoll(api.eval, 5000)

  const clearStop = async () => {
    try {
      await api.control({ clearStop: true })
      message.success('已清除停机标记（训练可继续）')
      await reload()
    } catch (e) {
      message.error(`清除失败：${e instanceof Error ? e.message : String(e)}`)
    }
  }

  if (error) {
    return <Alert type="error" showIcon message="加载 /api/eval 失败" description={error} />
  }

  const cfg = data?.config
  const trends = data?.trends ?? []

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      {data?.shouldStop && (
        <Alert
          type="error"
          showIcon
          message="绝对强度判据已置位停机信号：trainer 将优雅停止"
          description={data.stopReason || '（未提供原因）'}
          action={
            <Button danger onClick={clearStop}>
              清除停机标记（确认续训）
            </Button>
          }
        />
      )}

      {cfg && !cfg.enabled && (
        <Alert
          type="warning"
          showIcon
          message="绝对强度评估未启用"
          description="设置 SCHEDULER_EVAL_ENABLED=true 并确保 collector 已升级（认识 TASK_EVAL）后重启调度器。"
        />
      )}

      {cfg && (
        <Card size="small" title="评估配置">
          <Descriptions column={3} size="small">
            <Descriptions.Item label="状态">{cfg.enabled ? '启用' : '未启用'}</Descriptions.Item>
            <Descriptions.Item label="模式">{cfg.mode}</Descriptions.Item>
            <Descriptions.Item label="每档局数">{cfg.games}</Descriptions.Item>
            <Descriptions.Item label="对手阶梯">
              {cfg.opponents.map((o) => (
                <Tag key={o}>{opponentLabel(o)}</Tag>
              ))}
            </Descriptions.Item>
            <Descriptions.Item label="节流">每 {cfg.everyNPromotions} 次晋级评测一次</Descriptions.Item>
            <Descriptions.Item label="停机判据">
              {cfg.noProgressN > 0
                ? `连续 ${cfg.noProgressN} 次提升 < ${(cfg.noProgressEps * 100).toFixed(1)}pt`
                : '关闭（仅观测）'}
            </Descriptions.Item>
            <Descriptions.Item label="待下发任务">{cfg.pending}</Descriptions.Item>
            <Descriptions.Item label="累计评估结果">{data?.latest.length ?? 0}</Descriptions.Item>
          </Descriptions>
        </Card>
      )}

      {trends.map((t) => {
        const rates = t.points.map((p) => p.winRate)
        return (
          <Card
            key={t.opponent}
            size="small"
            title={`vs ${opponentLabel(t.opponent)}`}
            extra={cfg ? noProgressTag(t.noProgress, cfg) : null}
          >
            <Space size="large" align="start" wrap>
              <Statistic title="最新胜率" value={pct(t.latestWinRate)} />
              <Statistic title="已评测版本" value={t.versions} />
              <Statistic title="最新网络" value={shortSha(t.points[t.points.length - 1]?.networkSha ?? '')} />
              {cfg && <Sparkline rates={rates} eps={cfg.noProgressEps} />}
            </Space>
            <Table<EvalResult>
              rowKey={(r) => `${r.networkSha}-${r.opponentSpec}`}
              size="small"
              style={{ marginTop: 12 }}
              columns={pointColumns}
              dataSource={t.points}
              pagination={false}
            />
          </Card>
        )
      })}

      <Card size="small" title="最近评估明细">
        <Table<EvalResult>
          rowKey={(r) => `${r.networkSha}-${r.opponentSpec}`}
          size="small"
          columns={[
            { title: '网络', dataIndex: 'networkSha', render: (v: string) => <Typography.Text code>{shortSha(v)}</Typography.Text> },
            { title: '对手', dataIndex: 'opponentSpec', render: (v: string) => opponentLabel(v) },
            { title: '胜率', dataIndex: 'winRate', render: (v: number) => <b>{pct(v)}</b> },
            { title: '胜/和/负', render: (_, r) => `${r.wins}/${r.draws}/${r.losses}` },
            { title: '步均', dataIndex: 'avgMoves', render: (v: number) => v.toFixed(1) },
            { title: '时间', dataIndex: 'createdAt', render: (v: number) => fmtAgo(v) },
          ]}
          dataSource={data?.latest ?? []}
          pagination={{ pageSize: 20, hideOnSinglePage: true }}
          locale={{ emptyText: cfg?.enabled ? '暂无评估结果（等待 best 晋级触发）' : '评估未启用' }}
        />
      </Card>
    </Space>
  )
}
