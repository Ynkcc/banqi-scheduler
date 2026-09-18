# ARCHITECTURE — banqi-scheduler（Go 中心调度器）

> **仓库状态**：已从主仓库 `rust_4x8`（`server/` 子树，含完整提交历史）拆分为独立私有仓库 `Ynkcc/banqi-scheduler`。
> 主仓库侧对应文档为 `docs/ARCHITECTURE.md` §6.4（拆分后的引用摘要）。

## 1. 定位

分布式自对弈训练的中心调度器：任务分发（selfplay/rating/eval/reanalysis）、网络登记与晋级、episode 元数据登记 + R2 预签名直传、五项 GSPRT 判停、**绝对强度评估与停机判据**。

> **两类门禁的分工**：gatekeeper（GSPRT）是**相对**门禁，只回答「这代比上代强吗」，结构上测不出「所有版本都打不过一个 3 行的优先吃子启发式」；`TASK_EVAL` 是**绝对**强度观测（best vs 规则/内建对手），用于发现后一类问题（见 §3.2）。

调研结论（主仓库 `docs/distributed_training_reference_survey.md`）落地：lczero 拉取式调度 + KataGo URL 下发/预签名直传 + fishtest/pentanomial 五项 GSPRT 判停。
技术栈：Go + grpc-go + SQLite（modernc 纯 Go 驱动，WAL）+ aws-sdk-go-v2 S3 预签名（R2 兼容，凭据走标准 `AWS_*` 环境变量）。

## 2. 结构

| 条目 | 说明 |
|---|---|
| `go.mod` | module `banqi/server` |
| `cmd/scheduler/main.go` | 入口：`run()` 编排生命周期（`signal.NotifyContext` → `GracefulStop` + WebUI `Shutdown`），配置全走 `SCHEDULER_*` 环境变量（`-h` 列出）；示例配置见 `config.example.env`（含 R2 凭据与 GSPRT 参数说明）。另读标准 `AWS_*` 凭据/endpoint，`AWS_S3_PATH_STYLE=1` 时改用 path-style 寻址（本地 RustFS / MinIO 必需，R2 默认不设） |
| `internal/store` | SQLite 元数据，全部方法以 `context` 为首参；文件按表拆分：`store.go`（连接/迁移/统计/设置）、`networks.go`、`matches.go`、`eval.go`（绝对强度评估结果与趋势，**无外键**——规则对手没有 networks 行）、`episodes.go`、`workers.go`；迁移用 `pragma_table_info` 探测列存在性，best 指针事务切换 |
| `internal/r2` | 预签名 PUT/GET，键布局 `episodes/<sha>/*.epb.gz`（EpisodeBatch 二进制记录）、`networks/<sha>.<format>`（扩展名 = 权重格式 onnx/pt/nnue，worker 据此分派加载器） |
| `internal/sprt` | 五项 GSPRT（正态近似 LLR，elo0/elo1/alpha/beta 可配，含单测） |
| `internal/scheduler` | gRPC 服务实现 + 内存任务表（task↔worker 归属校验、rating 在飞判定、超期回收）；文件按职责拆分：`server.go`（运行状态与全局 RPC）、`tasks.go`（GetTask/ratingTask，下发 `network_key`/`opponent_key`）、`networks.go`（登记/晋级/gatekeeper 判停 + 权重格式白名单校验）、`episodes.go`（episode 登记与 trainer 数据面）、`reanalysis.go`（局面重搜任务队列：SubmitReanalysis 入队 + 按间隔节流下发）、`eval.go`（绝对强度评估编排：晋级触发/TASK_EVAL 下发/结果落库/「连续 N 次无提升」判据与停机置位）、`trainconfig.go`（可远程调整训练超参的白名单 + 归一化/范围校验，见 §5.1）、`control.go`（运行时控制 + 停机信号 + 训练超参覆盖，落库 settings） |
| `internal/api` | WebUI 的 JSON API + 控制端点 + embed 前端产物（`HTTPServer(addr)` 交由调用方启停，同进程 http.Server） |
| `webui/` | React 18 + Vite + TS + AntD 前端源码，构建产物输出到 `internal/api/dist` |
| `pb/` | protoc 生成代码（不提交） |
| `proto/scheduler.proto` | 契约源文件（已随拆分迁入；主仓库 `build.rs` 仍编译自己的 `proto/scheduler.proto` 副本，proto 变更需双侧同步） |

## 3. gRPC 契约（scheduler.proto，11 RPC）

- `GetTask`：worker 按机器规格拉任务（优先 gatekeeper rating，其次**绝对强度评估 TASK_EVAL**（见 §3.2），再次**局面重搜 reanalysis**（按间隔节流，见 §3.1），最后 best 网络 selfplay），按 worker 线程数缩放下发局数（`SCHEDULER_THREADS_BASELINE`）；恒下发网络对象键 `network_key`（rating 任务另含 `opponent_key`），下载 URL 仅在 worker 需要拉取时签发；selfplay 与 rating 均在 `SelfPlayParams.extra_config` 下发课程参数 `{"initial_revealed_pieces":N}`（<=0 不下发，worker 用变体默认值）——课程阶段切换可改 `SCHEDULER_INITIAL_REVEALED` 重启调度器，也可经 WebUI 在线切换（见 §5），worker 无需重编译；selfplay 另经 `SelfPlayParams.data_kind` 下发**数据类别**（`SCHEDULER_DATA_KIND` / WebUI，见 `DataKind`），worker 据此产出对应类别的记录；`TASK_REANALYSIS` 任务经 `reanalysis_payload` 下发历史局面载荷（恒用 `DATA_RESNET`，与 selfplay 的类别切换无关），其 `games` 字段表示位置条数；
- `ReportEpisode`：只收元数据（含数据类别 `kind`，落库 `episodes.kind`），签发 R2 预签名 PUT（对象键 `episodes/<sha>/<id>.epb.gz`），数据直传 R2（校验 task↔worker 归属）；
- `GetNetwork`：sha 或 best → 对象键 + 预签名 GET；
- `RegisterNetwork`：trainer 登记新网络（含权重格式 `format`，落库 `networks.format`）→ 自动创建 gatekeeper 对打；首个网络直接晋级；格式不在白名单（onnx/pt/nnue）时拒绝登记；
- `ReportMatchResult`：rating 分支累计五项成对计数 → GSPRT 判停 → 晋级/拒绝 best 指针；`TASK_EVAL` 分支只落 `eval_results` 并更新停机判据（**不参与 GSPRT 与晋级**，见 §3.2）；
- `Heartbeat`：worker 状态（client_version/memory_mb/running_task_id）+ best sha 下发；
- `SignNetworkUpload`：trainer 请求网络直传预签名 PUT（须声明权重格式 `format`，对象键 `networks/<sha>.<format>`）；
- `ListEpisodes`：trainer 游标分页拉 episode 预签名 GET 列表（游标为上次返回的对象键，服务端据此解析 `episodes.id` 并按登记顺序推进，不依赖对象键字典序）；可带 `kind` 只取某一数据类别（缺省不过滤），消费方据此避免下载无法消费的对象；
- `GetInfo`：返回 `variant`（变体类型由服务端下发，`SCHEDULER_VARIANT` 配置）与 **`should_stop` / `stop_reason`**（绝对强度判据置位的停机信号，trainer 按 `SHOULD_STOP_POLL_SECONDS` 轮询后优雅停止，见 §3.2）；
- `SubmitReanalysis`：trainer 提交一批**待重搜局面**（异步：入队后由 `GetTask` 分发给任意 worker）。拒绝情形：未启用（`SCHEDULER_REANALYSIS_INTERVAL_TASKS=0`）、变体与服务端不一致（防串变体）、载荷为空、队列满（不丢已有条目）；
- `GetTrainConfig`：trainer 启动引导与轮询拉取**可远程调整的训练超参**（见 §5.1）。变体不符时 `accepted=false` 并附说明（不返回覆盖值），否则 `accepted=true` + `overrides`（仅覆盖项，全量替换语义）。

**安全约定**：R2 凭据只在调度器持有，worker/trainer 零存储配置，全部经预签名 URL 上下行。

### 3.1 局面重搜（reanalysis）任务队列

trainer 从历史 episode 里攒下带胜负的**局面快照**（`EpisodeRecord.positions`），经 `SubmitReanalysis` 入队；`GetTask` 在 rating 之后、selfplay 之前按**间隔节流**取一条下发：每 `SCHEDULER_REANALYSIS_INTERVAL_TASKS` 个 selfplay 任务最多下发 1 个重搜任务（重搜与自对弈争抢同一份算力，间隔是这两类工作的配比旋钮；0 = 关闭）。队列上限 `SCHEDULER_REANALYSIS_MAX_QUEUE`（按载荷条数）满了拒绝新提交 —— 不按 FIFO 丢弃，因为最旧的位置恰恰是重搜收益最大的。

组装（拉 best / 签发 URL）失败时**不消费队首**，等下次 `GetTask` 重试，避免丢掉 trainer 提交的数据。重搜产出的数据走常规 `ReportEpisode` 通道（`DATA_RESNET`），训练侧零改动即可消费。

### 3.2 绝对强度评估（eval）任务与停机判据

**动机**：gatekeeper 是相对门禁（candidate vs 当前 best + GSPRT），只比较相邻两代。历史事故：380 个版本一路晋级，而同一模型的纯策略对「优先吃子」这个 3 行启发式只有 57.6% —— 相对门禁结构上不可能发现这类问题。

**执行位置**：调度器（Go）没有棋引擎，只负责编排；实际对局由采集端（Rust）执行，因此评估被建模为一种新任务 `TASK_EVAL`，复用 «下发任务 → worker 执行 → 上报结果 → 服务端聚合» 主链路。

**编排**（`internal/scheduler/eval.go`）：

| 环节 | 行为 |
|---|---|
| 触发 | best 指针变更时（`RegisterNetwork` 首个网络晋级 / `ReportMatchResult` 晋级）按 `SCHEDULER_EVAL_EVERY_N_PROMOTIONS` 节流入队（每对手一条）；启动时补齐当前 best 缺失的对手（新增配置 / 上次中断） |
| 下发 | `TaskResponse.opponent_spec` 携带对手标识（`random` / `rule:capture_first` / `rule:reveal_first`）；规则对手无网络文件，故不签发 `opponent_key`/`opponent_url`。**一次任务即含全部局数**（`SCHEDULER_EVAL_GAMES`，不做累加），因此重下发只是重做同一件事，结果按 `(network_sha, opponent_spec)` 覆盖，天然幂等 |
| 模式 | `SelfPlayParams.mcts_sims`：0 = 纯策略 argmax（门禁主口径），>0 = MCTS 模拟数 |
| 在飞保护 | `(network, spec)` 同刻只允许一个在飞任务；超过 `evalTaskStaleAfter`(15m) 未上报视为失效可重下发；队首连续 `evalMaxAttempts`(3) 次下发未上报即**丢弃并告警**（旧版 collector 不认 `TASK_EVAL` 时会安全降级为「无任务」，若不丢弃会把自对弈饿死） |
| 落库 | `eval_results` 表（**无外键**：规则对手不存在 networks 行，与 `matches` 表刻意分离）；`UNIQUE(network_sha, opponent_spec)` 保证同版本复测覆盖而不产生重复点；上报局数少于已有行时忽略（部分结果不劣化完整结果） |
| 判据 | 同一对手的胜率序列上，连续 `SCHEDULER_EVAL_NO_PROGRESS_N` 次「提升 < `SCHEDULER_EVAL_NO_PROGRESS_EPS`」→ `Control.SetStop(reason)`（落库 settings，跨重启保留）。**不设绝对阈值**（用户口径：只看趋势）。`N=0` = 只观测不判停 |
| 停机 | `GetInfoReply.should_stop/stop_reason` 下发；trainer 按 `SHOULD_STOP_POLL_SECONDS` 轮询，命中后走**既有**优雅停止路径（先训完当前轮并落 checkpoint）。清除需显式操作（`POST /api/control {"clearStop":true}`），避免「重启即绕过判据」 |

**算力与数据边界**：评估任务排在 rating 之后、reanalysis/selfplay 之前（单批完成、纯策略 1000 局秒级），受在飞保护与次数上限约束；`record_episodes=false`，**不产 episode、不上传对象**，不会污染训练数据。

**对手标识**（与采集端 `rule_opponents.rs` 同一套）：`random` / `rule:capture_first` / `rule:reveal_first`。启动时校验，写错即失败（`ValidateEvalOpponents`）。若将来引入 expectimax/NNUE 对手，只需扩展采集端解析与 `ValidateEvalOpponents`。

## 4. proto 生成

```
protoc --proto_path=proto --go_out=. --go_opt=module=banqi/server \
  --go-grpc_out=. --go-grpc_opt=module=banqi/server proto/scheduler.proto
```

## 5. WebUI（同进程 HTTP，默认仅本机）

`cmd/scheduler` 在同一进程内起 `http.Server`（`SCHEDULER_HTTP_ADDR`，默认 `127.0.0.1:9536`），复用同一 `store` 与 `scheduler.Server` 状态，不经过 gRPC，无鉴权。WebUI 监听失败只记日志，不影响 gRPC 调度。

前端产物 embed 进二进制，构建顺序与「先 protoc 生成 `pb/`」一致：

```
cd webui && npm install && npm run build   # 输出到 internal/api/dist
go build ./cmd/scheduler
```

只读接口：

| 路由 | 说明 |
|---|---|
| `GET /api/status` | 变体、课程阶段、暂停状态、SPRT 参数、best、计数、评估配置、**`shouldStop`/`stopReason`** |
| `GET /api/networks` | 网络列表（best 优先） |
| `GET /api/matches` | 对战列表，附 LLR / 得分率 / 判停结果 |
| `GET /api/eval` | 绝对强度趋势：评估配置 + 按对手分组的版本序列（升序）+ 最新明细 + 「连续无提升」计数 + 停机信号 |
| `GET /api/workers` | Worker 列表与在线判定（`SCHEDULER_WORKER_ONLINE_SECONDS`，默认 60s） |
| `GET /api/episodes?before=&limit=` | episode 元数据按 id 倒序分页 |
| `GET /api/tasks` | 进程内进行中任务快照 |
| `GET /api/train-config` | 可远程调整的训练超参：字段清单（名称/类型/取值约束）+ 当前覆盖值。清单即白名单投影，前端据此渲染表单，无需硬编码字段名与范围（见 §5.1） |

控制接口（写入 `settings` 表并跨重启保留；环境变量仅在库中无记录时作为初始值）：

| 路由 | 说明 |
|---|---|
| `POST /api/networks/{sha}/promote` | 手动晋级 best |
| `POST /api/control` | `{"paused":bool}` 暂停 selfplay（rating 继续：`GetTask` 只回 rating、`Heartbeat` 回 `pause_self_play`）；`{"initialRevealed":int}` 在线切换课程阶段，经 `extra_config` 下发；`{"dataKind":"resnet"\|"nnue"}` 在线切换自对弈产出的数据类别，经 `SelfPlayParams.data_kind` 下发；`{"clearStop":true}` 清除绝对强度判据置位的停机信号（确认续训） |
| `PUT /api/train-config` | `{"overrides":{...}}` 全量替换训练超参覆盖（空对象 = 清除全部覆盖，回落本地值）。校验失败返回 400，不落库（见 §5.1） |

前端页面：总览 / 网络 / 对战·SPRT / **绝对强度** / **训练超参** / Worker / Episode。「绝对强度」页展示停机横幅（含一键清除）、评估配置、按对手的版本趋势（含零依赖 SVG 迷你趋势图）与评估明细。「训练超参」页（`pages/TrainConfig.tsx`）按 `fields` 清单动态渲染编辑控件（bool/enum 用 Select、int/float 用 InputNumber 并按 `min`/`max` 约束），留空即删除覆盖，提交为全量替换；轮询只刷新服务端快照，不覆盖正在编辑的草稿（见 §5.1）。

内存任务表保留策略（见 `internal/scheduler/server.go`）：tasks 仅用于 task↔worker 归属校验与 rating 在飞判定，进度以 DB 为准；已上报结束（`Done`）的记录保留 `doneTaskRetention`(30m)、未上报记录最多保留 `taskMaxRetention`(24h)，由 `pruneTasks` 在 `GetTask` 路径上按 `pruneInterval`(1m) 节流回收；`/api/tasks` 只展示未结束任务。

### 5.1 可远程调整的训练超参

**动机**：训练超参（学习率、目标函数权重、训练量、EMA、重搜节流等）原先固化在 trainer 本地 YAML，改动需登录训练机改文件并重启进程。改为调度器持有**覆盖项**作为唯一权威：WebUI / gRPC 写入，trainer 启动引导 + 轮询热更，无需重编译或重启训练。

| 环节 | 行为 |
|---|---|
| 白名单 | `internal/scheduler/trainconfig.go` 的 `trainConfigSpecs`（约 30 字段，类型 float/int/bool/enum + 范围）。字段名与 `banqi_training.config.Config` 一一对应，**必须与 `banqi_training/config.py` 的 `TRAIN_CONFIG_OVERRIDABLE` 同步增删** |
| 可调判据 | 不改模型结构、不依赖本地路径/设备、能在训练循环中热更。**排除**：结构开关（`HEALTH_VALUE_HEAD_ENABLED` / `VALUE_DIST_*` / `POLICY_TRUNK_INDEPENDENT`，与旧 checkpoint 不兼容）、路径与设备、构造期一次性资源（`MAX_SAMPLE_BUFFER_SIZE` / `REANALYSIS_POOL_SIZE`） |
| 校验 | 非白名单字段直接报错（不静默丢弃）；数值域按 spec 校验；bool 只接受 `true/false`、`1/0`、`yes/no`、`on/off`（**刻意不复用 config.py `_cast_bool` 的「未知即真」容错**，远程写错必须报错）；enum 取值域与 `config.py` / `buffer.py` 的构造期校验对齐 |
| 语义 | **全量替换**：`PUT /api/train-config` 的 `overrides` 即当前全部覆盖，未出现 = 删除该覆盖、回落 trainer 本地 YAML 值（空对象 = 清空全部覆盖） |
| 落库/下发 | 落库 `settings.train_config`（JSON 对象，字段名 → 值字符串），跨重启保留；`GetTrainConfig` 只下发覆盖项，变体不符时 `accepted=false` 且不返回覆盖值 |
| 引导 | trainer 构造 `TrainWorker` 前先 `snapshot_overridable(config)` 存本地基线，再拉覆盖并 `apply_overrides`（含基线即可在后续删除覆盖时回落）；主 CLI 经 `cfg_baseline` 把基线交给 worker |
| 热更 | RPC/轮询线程只把覆盖登记到 `_pending_overrides`（锁保护），训练线程在**轮边界**（拉不到 episode 时）统一应用，避免与调度器/优化器争用；`lr_decay_batches` / `anneal_rounds` / `ema_decay` / 节流阈值等构造期快照一并重算，LR 调度器重建并 `scheduler.step(进度)` 保留训练进度 |
| 容错 | 拉取失败只记日志并沿用上次覆盖，绝不中断训练；`SHOULD_STOP_POLL_SECONDS=0` 会同时关闭停机轮询与配置轮询（文档与日志均告警） |

## 6. 变更记录

- 2026-09-11：从主仓库 `docs/ARCHITECTURE.md` §6.4 拆出，作为未来独立仓库的架构文档。
- 2026-09-11：经 `git subtree split` 自主仓库 `server/` 拆出为独立仓库（保留完整提交历史）；`proto/scheduler.proto` 迁入本仓库。
- 2026-09-11：新增 `SCHEDULER_INITIAL_REVEALED`（课程学习初始翻子数），经 `SelfPlayParams.extra_config` 下发（selfplay + rating 均生效），proto 无变更。
- 2026-09-12：新增 WebUI：`internal/api`（同进程 HTTP JSON API + 控制端点 + embed 前端产物，`SCHEDULER_HTTP_ADDR`/`SCHEDULER_WORKER_ONLINE_SECONDS`）、`webui/`（React 18 + Vite + TS + AntD SPA）；`store` 新增 `settings` 表与只读列表/统计查询；`scheduler` 新增运行时 `Control`（暂停 selfplay、课程阶段切换，落库并优先于环境变量，`New` 改为返回 error），proto 无变更。
- 2026-09-14：修正 `ListEpisodes` 游标语义——原按 `object_key` 字典序推进，而对象键含随机段（`episodes/<sha>/<random>.jsonl.gz`），字典序与登记顺序无关，会导致已登记但键更小的 episode 永久不出现在列表里（trainer 静默丢数据）。改为以 `afterKey` 反查 `episodes.id` 后按 id 递增返回，并补 `idx_episodes_object_key`；proto 与客户端无需变更。
- 2026-09-14：`internal/r2` 新增 `AWS_S3_PATH_STYLE`（默认关闭）以支持 RustFS / MinIO 等无法 virtual-host 寻址的本地 S3 实现；启动时打印 `path_style` 便于排查。R2 侧行为不变。
- 2026-09-15：运行时健壮性改造：①`cmd/scheduler/main.go` 改为 `run()` + `signal.NotifyContext`，收到 SIGINT/SIGTERM 后 `grpcServer.GracefulStop()` 并 `Shutdown` WebUI，启动失败路径不再被 `log.Fatalf` 跳过 defer；②内存任务表新增超期回收（`pruneTasks`，`Done` 保留 30m / 未上报最多 24h，`pruneInterval` 节流），`/api/tasks` 不再展示已结束任务；③`internal/store` 全部方法改为 ctx 首参（`QueryContext`/`ExecContext`/`BeginTx`），调用方传请求 ctx；④建表迁移改用 `pragma_table_info` 探测列存在性（删除依赖驱动错误文案的 `duplicate column name` 匹配与 `q[:40]` 切片），`RowsAffected` 错误不再被忽略；⑤`newID` 由 panic 改为返回错误；⑥gRPC 层错误补上下文（`fmt.Errorf("...: %w")`）；⑦`internal/api` 的 `ListenAndServe` 改为 `HTTPServer(addr)`，生命周期交由 `cmd/scheduler` 编排。新增 `internal/store/store_test.go`、`internal/scheduler/server_test.go`。
- 2026-09-15：文件拆分（降低单文件认知负荷）：`internal/store/store.go`(455 行) 拆为 `store.go` / `networks.go` / `matches.go` / `episodes.go` / `workers.go`（并抽出 `matchColumns` 统一列序）；`internal/scheduler/server.go`(495→544 行) 拆为 `server.go` / `tasks.go` / `networks.go` / `episodes.go`（新增 `lookupTask` / `markTaskDone` / `registerTask` / `judgeMatch` 辅助），单文件均 < 250 行。新增 `internal/api/handlers_test.go`（status/tasks/promote/control 与错误码）。`internal/sprt/sprt.go` 补 gofmt。
- 2026-09-16：**对象键自描述与训练数据记录 schema 化**（一侧改动，两侧契约同步——proto 变更已同步到 banqi-collector / banqi-training 副本）：
  - **episode 键**：`episodes/<sha>/<id>.jsonl.gz` → `episodes/<sha>/<id>.epb.gz`（载荷为 `EpisodeBatch` 二进制，schema 定义在 proto）。
  - **权重键**：`networks/<sha>.bin` → `networks/<sha>.<format>`；`networks` 表新增 `format` 列（默认 `onnx`，迁移按 `pragma_table_info` 探测补列），权重格式由上传方在 `SignNetworkUpload` 与 `RegisterNetwork` 两处声明并由调度器按白名单（onnx/pt/nnue）校验，任一为空或非法即拒绝，避免生成无法下载的键。
  - **下发给 worker 的对象键**：`TaskResponse.network_key` / `opponent_key` 恒下发（本地缓存命名与权重格式判定的依据），`NetworkInfo.key` 供心跳预取使用；下载 URL 仍在需要拉取时才签发。旧缓存与旧对象作废（用户确认不做兼容）。
  - **新增训练数据记录消息**：`EpisodeBatch` / `EpisodeRecord` / `NnueEpisodeRecord` / `NnueFeatures` / `NnueMeta`（字段号 + `schema_version`）。调度器只登记元数据、不解析对象内容，本组消息由采集端与训练端共享。
- 2026-09-16：**数据类别（ResNet / NNUE）贯通为可在线切换的服务端配置**（本轮不含采集端产出 NNUE 的能力）：
  - **proto**：新增 `enum DataKind`（`DATA_RESNET` / `DATA_NNUE`），落到 `SelfPlayParams.data_kind`（任务要产哪类）、`EpisodeBatch.kind`（对象自描述）、`EpisodeMeta.kind`（落库）、`ListEpisodesRequest.kind`（消费端按类别过滤，缺省不过滤）。
  - **类别来源**：`SCHEDULER_DATA_KIND`（默认 `resnet`）作为库中无记录时的初值，运行时以 `Control.dataKind` 为准，可经 WebUI `POST /api/control` 在线切换并落库 `settings.data_kind`。
  - **落库与过滤**：`episodes` 表新增 `kind` 列（迁移按 `pragma_table_info` 补列，缺省 0 = ResNet），`ListEpisodeKeys` 支持按类别过滤（游标子查询独立于过滤条件，跨类别切换游标仍能正确推进）。**`idx_episodes_kind` 必须建在补列之后**——建表语句块先于补列执行，把依赖新列的索引写在那里会让老库启动即 `no such column: kind`；已补 `TestMigrateFromLegacySchema` 覆盖该升级路径。
  - **`EpisodeRecord.nnue` 字段废弃**（`reserved 21`）：MCTS 自对弈不再顺带收集 NNUE 稀疏特征，两类数据彻底分家——NNUE 特征只由 `NnueEpisodeRecord` 承载；`NnueEpisodeRecord` 改为内嵌 `NnueFeatures`，稀疏特征的布局定义只此一处。
  - **语义边界**：调度器只负责下发类别，不校验 worker 是否具备该采集能力；当前 `banqi-collector` 仅支持 `DATA_RESNET`，收到 `DATA_NNUE` 任务会明确报错退出（绝不静默产出别类数据）。
- 2026-09-16：**新增局面重搜（reanalysis）任务类型**（跨进程：trainer 提交历史局面 → worker 用当前 best 网络重跑 MCTS 刷新训练目标）：
  - **proto**：`TaskKind` 新增 `TASK_REANALYSIS`；`TaskResponse` 新增 `reanalysis_payload`（版本化紧凑条目，格式由 banqi-core 的局面快照 + collector 的 encode/decode_payload 定义，调度器视为不透明字节）；新增 `SubmitReanalysis` RPC（trainer 提交）与 `SubmitReanalysisRequest/Reply`（**10 RPC**）。三份 proto 副本已同步。
  - **队列与节流**（`internal/scheduler/reanalysis.go`）：`SubmitReanalysis` 异步入队（拒绝：未启用 / 变体不符 / 空载荷 / 队列满），`GetTask` 在 rating 之后、selfplay 之前按 `SCHEDULER_REANALYSIS_INTERVAL_TASKS`（每 N 个 selfplay 任务最多 1 个重搜）取一条下发；组装失败不消费队首（下次重试），队列上限 `SCHEDULER_REANALYSIS_MAX_QUEUE` 满了拒绝而不丢最旧。
  - **下发内容**：`NetworkSha` = 当前 best（重搜的意义就是更强的网络重搜旧局面）、`NetworkKey/Url` 与 selfplay 同规则、`games` = 位置条数、`Params.data_kind` 恒为 `DATA_RESNET`（重搜基于 MCTS，与 selfplay 的类别切换无关）、`Params.mcts_sims` 取提交方指定值（0 = worker 默认）。
  - **回收链路复用**：重搜产物由 worker 经常规 `ReportEpisode` 上报（一局面一条 1 样本 episode），训练侧零改动即可消费。
  - **测试**：新增 `internal/scheduler/reanalysis_test.go`（提交入口三条拒绝路径 + 队列节流 / peek 不消费 / commit 重置计数 / 上限拒绝）。
- 2026-09-17：**新增绝对强度评估（TASK_EVAL）与停机判据**（`SCHEDULER_EVAL_*`）：
  - **proto**：`TaskKind` 新增 `TASK_EVAL`；`TaskResponse` 新增 `opponent_spec`（14）、`MatchResult` 新增 `opponent_spec`（15）与 `avg_moves`（16）、`GetInfoReply` 新增 `should_stop`（2）/`stop_reason`（3）。RPC 数量不变（10）；三份 proto 副本已同步。
  - **落库**：新增 `eval_results` 表（**无外键**，`UNIQUE(network_sha, opponent_spec)`），新增 `Counts.evalResults`；迁移沿用 `CREATE TABLE IF NOT EXISTS`，老库自动补表。
  - **编排**（`internal/scheduler/eval.go`）：best 晋级触发（按 `SCHEDULER_EVAL_EVERY_N_PROMOTIONS` 节流）+ 启动补齐；一次任务含全部局数；`(network,spec)` 在飞保护与 15m 失效判定；队首连续 3 次下发未上报即丢弃（防旧版 worker 饿死自对弈）；`reportEvalResult` 只落库不参与晋级，且部分结果不劣化完整结果；`judgeEvalProgress` 按「连续 N 次提升 < EPS」置位 `Control.SetStop`（落库 settings，跨重启保留，需显式 `clearStop`）。
  - **API/UI**：新增 `GET /api/eval`；`/api/status` 增加 `eval` 配置与 `shouldStop`/`stopReason`；`/api/control` 支持 `clearStop`；WebUI 新增「绝对强度」页。
  - **测试**：新增 `internal/store/eval_test.go`（去重覆盖 / 趋势序列 / 老库补表）、`internal/scheduler/eval_test.go`（节流去重 / 在飞 / 丢弃 / 判据 / 部分结果保护 / 进程内全链路）、`internal/api` 增补 `/api/eval` 与停机信号用例。
- 2026-09-18：**新增可远程调整的训练超参**（见 §5.1）：
  - **proto**：新增 `GetTrainConfig` RPC（trainer 引导 + 轮询拉取覆盖项）与 `GetTrainConfigRequest/Reply`（**11 RPC**）。三份 proto 副本已同步。
  - **白名单与校验**（`internal/scheduler/trainconfig.go`）：`trainConfigSpecs`（约 30 字段，float/int/bool/enum + 范围），归一化即校验，非白名单字段/越界/非法枚举直接报错；bool 用严格写法（不复用 config.py 的「未知即真」容错）。落库 `settings.train_config`。
  - **运行时**（`internal/scheduler/control.go`）：`Control` 持有 `trainConfig`，`loadControl` 重读时重新校验，`SetTrainConfig` 归一后落库。
  - **API/UI**：新增 `GET /api/train-config`（字段清单 + 当前覆盖，前端无需硬编码字段名与范围）与 `PUT /api/train-config`（全量替换，非法 400 不落库）；`TrainConfigField` 随 spec 一并暴露 `min`/`max`/`hasMax`/`gtZero`/`enum` 供表单约束（权威校验仍在服务端）。WebUI 新增「训练超参」页。
  - **测试**：新增 `internal/scheduler/trainconfig_test.go`（归一化 / 白名单拒绝 / 整批拒绝 / 字段清单 / 落库重载往返）、`internal/api/handlers_test.go::TestTrainConfigEndpoint`（JSON 契约与全量替换语义）；`go test ./...` 全绿。
- 2026-09-18：WebUI 默认监听端口由 `127.0.0.1:8080` 改为 `127.0.0.1:9536`：`8080` 在部署机上已被既有服务占用，导致 WebUI 绑定失败（日志 `address already in use`，gRPC 调度不受影响）。`SCHEDULER_HTTP_ADDR` 仍可覆盖；同步 `config.example.env`、`webui/vite.config.ts` dev 代理与部署 README。
