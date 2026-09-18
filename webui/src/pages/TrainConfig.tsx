import { Alert, App, Button, Card, InputNumber, Select, Space, Statistic, Table, Tag, Typography } from 'antd'
import type { TableProps } from 'antd'
import { useEffect, useState } from 'react'
import { api } from '../api'
import type { TrainConfigField } from '../types'
import { usePoll } from '../usePoll'

const KIND_LABEL: Record<string, string> = { float: '浮点', int: '整数', bool: '布尔', enum: '枚举' }

const BOOL_OPTIONS = [
  { value: 'true', label: 'true' },
  { value: 'false', label: 'false' },
]

/** 取值约束的可读提示：服务端仍是唯一权威，这里只把白名单 spec 摊开展示 */
function constraintText(f: TrainConfigField): string {
  if (f.kind === 'enum') return `可选 ${f.enum?.join(' / ') ?? ''}`
  if (f.kind === 'bool') return 'true / false'
  const bounds = [f.gtZero ? `> ${f.min}` : `>= ${f.min}`]
  if (f.hasMax) bounds.push(`<= ${f.max}`)
  return bounds.join(' 且 ')
}

const same = (a: Record<string, string>, b: Record<string, string>) =>
  Object.keys(a).length === Object.keys(b).length && Object.keys(a).every((k) => a[k] === b[k])

/** 训练超参面板：白名单字段清单 + 覆盖值编辑（全量替换语义，留空即回落本地 YAML）。 */
export default function TrainConfig() {
  const { message } = App.useApp()
  const { data, error, reload } = usePoll(api.trainConfig, 15000)
  const [draft, setDraft] = useState<Record<string, string> | null>(null)
  const [busy, setBusy] = useState(false)

  // 只在首次拿到数据时灌入草稿：轮询刷新不能覆盖用户正在编辑的内容。
  useEffect(() => {
    if (data && draft === null) {
      setDraft({ ...data.overrides })
    }
  }, [data, draft])

  const setField = (name: string, value: string | undefined) => {
    setDraft((prev) => {
      const next = { ...(prev ?? {}) }
      if (value === undefined || value === '') delete next[name]
      else next[name] = value
      return next
    })
  }

  const report = (e: unknown) => message.error(`操作失败：${e instanceof Error ? e.message : String(e)}`)

  const reloadFromServer = async () => {
    setBusy(true)
    try {
      const fresh = await api.trainConfig()
      setDraft({ ...fresh.overrides })
      await reload()
      message.success('已从调度器重新载入')
    } catch (e) {
      report(e)
    } finally {
      setBusy(false)
    }
  }

  const submit = async () => {
    if (draft === null) return
    setBusy(true)
    try {
      await api.setTrainConfig(draft)
      const fresh = await api.trainConfig()
      setDraft({ ...fresh.overrides })
      await reload()
      message.success(`已提交 ${Object.keys(fresh.overrides).length} 项覆盖，trainer 在下一个轮边界生效`)
    } catch (e) {
      report(e)
    } finally {
      setBusy(false)
    }
  }

  if (error) {
    return <Alert type="error" showIcon message="加载 /api/train-config 失败" description={error} />
  }

  const fields = data?.fields ?? []
  const serverOverrides = data?.overrides ?? {}
  const overridden = new Set(Object.keys(draft ?? {}))
  const dirty = draft !== null && !same(draft, serverOverrides)

  const columns: TableProps<TrainConfigField>['columns'] = [
    {
      title: '字段',
      dataIndex: 'name',
      width: 300,
      render: (v: string) => (
        <Typography.Text code copyable>
          {v}
        </Typography.Text>
      ),
    },
    { title: '类型', dataIndex: 'kind', width: 80, render: (v: string) => <Tag>{KIND_LABEL[v] ?? v}</Tag> },
    { title: '取值约束', width: 160, render: (_, f) => <Typography.Text type="secondary">{constraintText(f)}</Typography.Text> },
    {
      title: '覆盖值',
      width: 240,
      render: (_, f) => {
        const value = draft?.[f.name]
        const clearable = { allowClear: true, placeholder: '未覆盖', disabled: busy, style: { width: '100%' } }
        if (f.kind === 'bool') {
          return <Select<string> {...clearable} value={value} options={BOOL_OPTIONS} onChange={(v) => setField(f.name, v)} />
        }
        if (f.kind === 'enum') {
          return (
            <Select<string>
              {...clearable}
              value={value}
              options={(f.enum ?? []).map((v) => ({ value: v, label: v }))}
              onChange={(v) => setField(f.name, v)}
            />
          )
        }
        return (
          <InputNumber
            style={{ width: '100%' }}
            value={value === undefined ? null : Number(value)}
            min={f.min}
            max={f.hasMax ? f.max : undefined}
            step={f.kind === 'int' ? 1 : 0.0001}
            precision={f.kind === 'int' ? 0 : undefined}
            placeholder="未覆盖"
            disabled={busy}
            onChange={(v) => setField(f.name, typeof v === 'number' ? String(v) : undefined)}
          />
        )
      },
    },
    {
      title: '来源',
      width: 120,
      render: (_, f) => (overridden.has(f.name) ? <Tag color="blue">调度器覆盖</Tag> : <Tag>本地 YAML</Tag>),
    },
  ]

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      <Alert
        type="info"
        showIcon
        message="训练超参可远程调整（无需重启 trainer）"
        description="白名单字段与取值校验由调度器裁定；提交为全量替换——留空的字段即删除覆盖、回落 trainer 本地 YAML。trainer 在下一个训练轮边界应用，改动学习率计划时会重建调度器并保留训练进度。"
      />

      <Card size="small" title="覆盖项">
        <Space size="large" align="center" wrap>
          <Statistic title="已覆盖" value={overridden.size} suffix={`/ ${fields.length}`} />
          <Statistic title="服务端当前覆盖" value={Object.keys(serverOverrides).length} />
          <Space>
            <Button type="primary" loading={busy} disabled={!dirty} onClick={submit}>
              提交（全量替换）
            </Button>
            <Button disabled={busy || overridden.size === 0} onClick={() => setDraft({})}>
              清空表单
            </Button>
            <Button disabled={busy} onClick={reloadFromServer}>
              重新载入
            </Button>
          </Space>
          {dirty && <Typography.Text type="warning">有未提交的改动</Typography.Text>}
        </Space>
      </Card>

      <Card size="small" title={`可调字段（${fields.length}）`}>
        <Table<TrainConfigField>
          rowKey="name"
          size="small"
          columns={columns}
          dataSource={fields}
          pagination={false}
          locale={{ emptyText: '调度器未提供可调字段清单' }}
        />
      </Card>
    </Space>
  )
}
